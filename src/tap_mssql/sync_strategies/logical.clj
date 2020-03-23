(ns tap-mssql.sync-strategies.logical
  (:refer-clojure :exclude [sync])
  (:require [tap-mssql.config :as config]
            [tap-mssql.singer.fields :as singer-fields]
            [tap-mssql.singer.bookmarks :as singer-bookmarks]
            [tap-mssql.singer.messages :as singer-messages]
            [tap-mssql.sync-strategies.full :as full]
            [tap-mssql.sync-strategies.common :as common]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure.java.jdbc :as jdbc]))

(defn get-change-tracking-tables*
  "Structure: {\"schema_name\" [\"table1\" \"table2\" ...] ...}"
  [config dbname]
  (reduce (fn [acc val] (assoc acc
                               (:schema_name val)
                               (-> (get acc (:schema_name val))
                                   (concat [(:table_name val)])
                                   set)))
          {}
          (jdbc/query (assoc (config/->conn-map config)
                             :dbname dbname)
                      [(str "SELECT OBJECT_SCHEMA_NAME(object_id) AS schema_name, "
                            "       OBJECT_NAME(object_id) AS table_name "
                            "FROM sys.change_tracking_tables")])))

(def get-change-tracking-tables (memoize get-change-tracking-tables*))

(defn get-change-tracking-databases* [conf]
  (set (map #(:db_name %)
            (jdbc/query (config/->conn-map conf)
                        [(str "SELECT DB_NAME(database_id) AS db_name "
                              "FROM sys.change_tracking_databases")]))))

(def get-change-tracking-databases (memoize get-change-tracking-databases*))

(defn get-object-id-by-table-name [config dbname schema-name table-name]
  (let [sql-query ["SELECT OBJECT_ID(?) AS object_id"
                   (-> (partial format "%s.%s.%s")
                       (apply (map common/sanitize-names [dbname schema-name table-name])))]]
    (log/infof "Executing query: %s" sql-query)
    (->> (jdbc/query (assoc (config/->conn-map config) :dbname dbname) sql-query)
         first
         :object_id)))

(defn get-min-valid-version [config dbname schema-name table-name]
  (let [object-id (get-object-id-by-table-name config dbname schema-name table-name)
        sql-query (format "SELECT CHANGE_TRACKING_MIN_VALID_VERSION(%d) as min_valid_version" object-id)]
    (log/infof "Executing query: %s" sql-query)
    (-> (jdbc/query (assoc (config/->conn-map config) :dbname dbname) [sql-query])
        first
        :min_valid_version)))

(defn assert-log-based-is-enabled [config catalog stream-name state]
  (let [table-name        (get-in catalog ["streams" stream-name "table_name"])
        schema-name       (get-in catalog ["streams" stream-name "metadata" "schema-name"])
        dbname            (get-in catalog ["streams" stream-name "metadata" "database-name"])
        min-valid-version (get-min-valid-version config dbname schema-name table-name)]
    (when (not (contains? (get-change-tracking-databases config) dbname))
      (throw (UnsupportedOperationException.
              (format (str "Cannot sync stream: %s using log-based replication. "
                           "Change Tracking is not enabled for database: %s")
                      stream-name
                      dbname))))
    (when (not (contains? (-> (get-change-tracking-tables config dbname)
                              (get schema-name)) table-name))
      (throw (UnsupportedOperationException.
              (format (str "Cannot sync stream: %s using log-based replication. "
                           "Change Tracking is not enabled for table: %s")
                      stream-name
                      table-name))))
    (when (nil? min-valid-version)
      (throw (IllegalArgumentException.
              (format "The min_valid_version for table name %s was NULL."
                      table-name))))
    state))

(defn get-current-log-version [config catalog stream-name]
  (let [dbname (get-in catalog ["streams" stream-name "metadata" "database-name"])]
    (-> (jdbc/query (assoc (config/->conn-map config)
                           :dbname dbname)
                    ["SELECT current_version = CHANGE_TRACKING_CURRENT_VERSION()"])
        first
        :current_version)))

;; belongs in singer?
(defn update-current-log-version [stream-name version state]
  (let [previous-log-version (get-in state ["bookmarks" stream-name "current_log_version"])]
    (as-> (assoc-in state
                    ["bookmarks" stream-name "current_log_version"]
                    version)
        new-state
        (if (not= previous-log-version version)
          (update-in new-state ["bookmarks" stream-name] dissoc "last_pk_fetched")
          new-state))))

(defn log-based-init-state
  [config catalog stream-name state]
  (if (nil? (get-in state ["bookmarks" stream-name "initial_full_table_complete"]))
    (-> state
        (assoc-in ["bookmarks" stream-name "current_log_version"]
                  (get-current-log-version config catalog stream-name))
        (assoc-in ["bookmarks" stream-name "initial_full_table_complete"] false)
        ((partial singer-messages/write-state! stream-name)))
    state))

(defn min-valid-version-out-of-date?
  "Uses the CHANGE_TRACKING_MIN_VALID_VERSION function to check if our current log version is out of date and lost.
  Returns true if we have no current log version."
  [config catalog stream-name state]
  (let [schema-name         (get-in catalog ["streams" stream-name "metadata" "schema-name"])
        table-name          (get-in catalog ["streams" stream-name "table_name"])
        dbname              (get-in catalog ["streams" stream-name "metadata" "database-name"])
        current-log-version (get-in state ["bookmarks" stream-name "current_log_version"])
        min-valid-version   (get-min-valid-version config dbname schema-name table-name)]
    (if (nil? current-log-version)
      true
      (let [out-of-date? (> min-valid-version current-log-version)]
        (when out-of-date?
          (log/warn "CHANGE_TRACKING_MIN_VALID_VERSION has reported a value greater than current-log-version. Executing a full table sync."))
        out-of-date?))))

(defn log-based-initial-full-table
  [config catalog stream-name state]
  (if (or (= false (get-in state ["bookmarks" stream-name "initial_full_table_complete"]))
          (min-valid-version-out-of-date? config catalog stream-name state))
      (-> state
          ((partial full/sync! config catalog stream-name))
          (assoc-in ["bookmarks" stream-name "initial_full_table_complete"] true)
          ((partial singer-messages/write-state! stream-name)))
      state))

(defn build-log-based-sql-query [catalog stream-name state]
  (let [schema-name           (->  (get-in catalog ["streams" stream-name "metadata" "schema-name"])
                                   (common/sanitize-names))
        table-name            (-> (get-in catalog ["streams" stream-name "table_name"])
                                  (common/sanitize-names))
        primary-keys          (map common/sanitize-names (set (get-in catalog ["streams"
                                                                               stream-name
                                                                               "metadata"
                                                                               "table-key-properties"])))
        primary-key-bookmarks (get-in state ["bookmarks" stream-name "last_pk_fetched"])
        current-log-version   (get-in state ["bookmarks" stream-name "current_log_version"])
        _                     (log/infof "Syncing log-based stream at version: %d" current-log-version)
        record-keys           (map common/sanitize-names (clojure.set/difference (set (singer-fields/get-selected-fields catalog stream-name))
                                                                                 primary-keys))]
    ;; Assert state of the world
    (assert (or (not-empty primary-keys)
                (not-empty record-keys))
            "No selected keys found, you must have a primary key and/or select columns to replicate.")
    (assert (not (nil? current-log-version))
            "Invalid log-based state, need a value for `current-log-version`.")
    (assert (not (empty? primary-keys))
            "No primary key(s) found, must have a primary key to replicate")
    (let [select-clause (str "SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time"
                             (when (not-empty primary-keys)
                               (str ", " (string/join ", "
                                                      (map #(format "c.%s" %)
                                                           primary-keys))))
                             (when (not-empty record-keys)
                               (str ", " (string/join ", "
                                                      (map #(format "%s.%s.%s" schema-name table-name %)
                                                           record-keys)))))
          from-clause (format " FROM CHANGETABLE (CHANGES %s.%s, %s) as c " schema-name table-name (if (> current-log-version 0)
                                                                                                     (dec current-log-version)
                                                                                                     0))
          join-clause (format "LEFT JOIN %s.%s ON %s LEFT JOIN %s on %s"
                              schema-name
                              table-name
                              (string/join " AND "(map #(format "c.%s=%s.%s.%s" % schema-name table-name %) primary-keys))
                              "sys.dm_tran_commit_table tc"
                              "c.SYS_CHANGE_VERSION = tc.commit_ts")
          join-where-clause (when (not-empty primary-key-bookmarks)
                              (str (format " WHERE c.SYS_CHANGE_VERSION = %s AND "
                                           current-log-version)
                                   (string/join " AND "
                                                (map #(format "c.%s >= ?" %)
                                                     (-> (keys primary-key-bookmarks)
                                                         sort
                                                         vec)))))
          order-by-clause (str " ORDER BY c.SYS_CHANGE_VERSION"
                               (when (not-empty primary-keys)
                                 (str ", " (string/join ", "
                                                        (map #(format "c.%s" %)
                                                             (sort (vec primary-keys)))))))
          query-string (str select-clause from-clause join-clause join-where-clause order-by-clause)]
      (if (not-empty primary-key-bookmarks)
        (into [query-string] (-> (sort-by key primary-key-bookmarks)
                                 vals))
        [query-string]))))

(defn maybe-update-current-log-version [state stream-name db-log-version]
  "Updates the state's bookmark when current-log-version lags behind db-log-version and a sync has been executed"
  (let [current-log-version (get-in state ["bookmarks" stream-name "current_log_version"])]
    (if (< current-log-version db-log-version)
      (update-current-log-version stream-name db-log-version state)
      state)))

(defn get-commit-time [result]
  (or (get result "commit_time")
      (.toString (java.time.Instant/now))))

(defn log-based-sync
  [config catalog stream-name state]
  {:pre  [(= true (get-in state ["bookmarks" stream-name "initial_full_table_complete"]))]
   :post [(map? %)]}
  (let [record-keys    (singer-fields/get-selected-fields catalog stream-name)
        bookmark-keys  (singer-bookmarks/get-bookmark-keys catalog stream-name)
        dbname         (get-in catalog ["streams" stream-name "metadata" "database-name"])
        db-log-version (get-current-log-version config catalog stream-name)
        sql-params     (build-log-based-sql-query catalog stream-name state)]
    (log/infof "Executing query: %s" sql-params)
    (singer-messages/write-activate-version! stream-name catalog state)
    (-> (reduce (fn [st result]
                  (let [record (as-> (select-keys result record-keys) rec
                                 (if (= "D" (get result "sys_change_operation"))
                                   (do
                                     (when-not (get result "commit-time")
                                       (log/warn "Found deleted record with no timestamp, falling back to current time."))
                                     (assoc rec "_sdc_deleted_at" (get-commit-time result)))
                                   rec))]
                    (singer-messages/write-record! stream-name st record catalog)
                    (->> (singer-bookmarks/update-last-pk-fetched stream-name bookmark-keys st record)
                         (update-current-log-version stream-name
                                                     (get result "sys_change_version"))
                         (singer-messages/write-state-buffered! stream-name))))
                state
                (jdbc/reducible-query (assoc (config/->conn-map config)
                                             :dbname dbname)
                                      sql-params
                                      common/result-set-opts))
        ;; maybe-update in case no rows were synced
        (maybe-update-current-log-version stream-name db-log-version)
        ;; last_pk_fetched indicates an interruption, and should be gone
        ;; after a successful log sync
        (update-in ["bookmarks" stream-name] dissoc "last_pk_fetched"))))

(defn sync!
  [config catalog stream-name state]
  (->> state
       (assert-log-based-is-enabled config catalog stream-name)
       (log-based-init-state config catalog stream-name)
       (log-based-initial-full-table config catalog stream-name)
       (singer-messages/write-state! stream-name)
       (log-based-sync config catalog stream-name)))
