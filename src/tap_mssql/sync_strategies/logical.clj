(ns tap-mssql.sync-strategies.logical
  (:refer-clojure :exclude [sync])
  (:require [tap-mssql.config :as config]
            [tap-mssql.singer.fields :as singer-fields]
            [tap-mssql.singer.bookmarks :as singer-bookmarks]
            [tap-mssql.singer.messages :as singer-messages]
            [tap-mssql.singer.transform :as singer-transform]
            [tap-mssql.sync-strategies.full :as full]
            [tap-mssql.sync-strategies.common :as common]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure.java.jdbc :as jdbc]))

(defn get-change-tracking-tables* [config db-name]
  ;; TODO: What if it's the same named table in different schemas?
  (set (map #(:table_name %)
            (jdbc/query (assoc (config/->conn-map config)
                               :dbname db-name)
                        [(str "SELECT OBJECT_NAME(object_id) AS table_name "
                              "FROM sys.change_tracking_tables")]))))

(def get-change-tracking-tables (memoize get-change-tracking-tables*))

(defn get-change-tracking-databases* [conf]
  (set (map #(:db_name %)
            (jdbc/query (config/->conn-map conf)
                        [(str "SELECT DB_NAME(database_id) AS db_name "
                              "FROM sys.change_tracking_databases")]))))

(def get-change-tracking-databases (memoize get-change-tracking-databases*))

(defn assert-log-based-is-enabled [config catalog stream-name state]
  (let [table-name (get-in catalog ["streams" stream-name "table_name"])
        dbname (get-in catalog ["streams" stream-name "metadata" "database-name"])]
    (when (not (contains? (get-change-tracking-databases config) dbname))
      (throw (UnsupportedOperationException.
              (format (str "Cannot sync stream: %s using log-based replication. "
                           "Change Tracking is not enabled for database: %s")
                      stream-name
                      dbname))))
    (when (not (contains? (get-change-tracking-tables config dbname) table-name))
      (throw (UnsupportedOperationException.
              (format (str "Cannot sync stream: %s using log-based replication. "
                           "Change Tracking is not enabled for table: %s")
                      stream-name
                      table-name))))
    state))

(defn get-current-log-version [config catalog stream-name]
  (let [dbname (get-in catalog ["streams" stream-name "metadata" "database-name"])]
    (:current_version (first
                       (jdbc/query (assoc (config/->conn-map config)
                                          :dbname dbname)
                                   ["SELECT current_version = CHANGE_TRACKING_CURRENT_VERSION()"])))))

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
        table-name          (-> (get-in catalog ["streams" stream-name "table_name"])
                                (common/sanitize-names))
        dbname              (get-in catalog ["streams" stream-name "metadata" "database-name"])
        current-log-version (get-in state ["bookmarks" stream-name "current_log_version"])
        sql-query           (format "SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('%s.%s')) as min_valid_version" schema-name table-name)
        _                   (log/infof "Executing query: %s" sql-query)
        min-valid-version   (-> (jdbc/query (assoc (config/->conn-map config) :dbname dbname) [sql-query])
                                first
                                :min_valid_version)]

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
  (let [schema-name           (get-in catalog ["streams" stream-name "metadata" "schema-name"])
        table-name            (-> (get-in catalog ["streams" stream-name "table_name"])
                                  (common/sanitize-names))
        primary-keys          (map common/sanitize-names (set (get-in catalog ["streams"
                                                                               stream-name
                                                                               "metadata"
                                                                               "table-key-properties"])))
        primary-key-bookmarks (get-in state ["bookmarks" stream-name "last_pk_fetched"])
        current-log-version   (get-in state ["bookmarks" stream-name "current_log_version"])
        record-keys           (map common/sanitize-names (clojure.set/difference (set (singer-fields/get-selected-fields catalog stream-name))
                                                                                 primary-keys))]
    ;; Assert state of the world
    (assert (or (not-empty primary-keys)
                (not-empty record-keys))
            "No selected keys found, you must have a primary key and/or select columns to replicate.")
    (assert (not (nil? current-log-version))
            "Invalid log-based state, need a value for `current-log-version`.")
    (as-> (format
           (str "SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time"
                (when (not-empty primary-keys)
                  (str ", " (string/join ", "
                                         (map #(format "c.%s" %)
                                              primary-keys))))
                (when (not-empty record-keys)
                  (str ", " (string/join ", "
                                         (map #(format "%s.%s.%s" schema-name table-name %)
                                              record-keys))))
                " FROM CHANGETABLE (CHANGES %s.%s, %s) as c "
                "LEFT JOIN %s.%s ON %s LEFT JOIN %s on %s"
                ;; Existence of PK Bookmark indicates that a single change
                ;; was interrupted. Only get changes for this version to
                ;; finish it out.
                (when (not-empty primary-key-bookmarks)
                  (str (format " WHERE c.SYS_CHANGE_VERSION = %s AND "
                               current-log-version)
                       (string/join " AND "
                                    (map #(format "c.%s >= ?" %)
                                         (-> (keys primary-key-bookmarks)
                                             sort
                                             vec)))))
                " ORDER BY c.SYS_CHANGE_VERSION"
                (when (not-empty primary-keys)
                  (str ", " (string/join ", "
                                         (map #(format "c.%s" %)
                                              (sort (vec primary-keys)))))))
           schema-name
           table-name
           ;; CHANGETABLE is strictly greater than, so we decrement here
           ;; to keep the state showing the "current version"
           (if (> current-log-version 0)
             (dec current-log-version)
             0)
           schema-name
           table-name
           (string/join " AND "(map #(format "c.%s=%s.%s.%s" % schema-name table-name %) primary-keys))
           "sys.dm_tran_commit_table tc"
           "c.SYS_CHANGE_VERSION = tc.commit_ts")
        query-string
        (if (not-empty primary-key-bookmarks)
          (into [query-string] (-> (sort-by key primary-key-bookmarks)
                                   vals))
          [query-string]))))

(defn log-based-sync
  [config catalog stream-name state]
  {:pre  [(= true (get-in state ["bookmarks" stream-name "initial_full_table_complete"]))]
   :post [(map? %)]}
  (let [record-keys   (singer-fields/get-selected-fields catalog stream-name)
        bookmark-keys (singer-bookmarks/get-bookmark-keys catalog stream-name)
        dbname        (get-in catalog ["streams" stream-name "metadata" "database-name"])
        sql-params    (build-log-based-sql-query catalog stream-name state)]
    (log/infof "Executing query: %s" sql-params)
    (singer-messages/write-activate-version! stream-name state)
    (-> (reduce (fn [st result]
                  (let [record (as-> (select-keys result record-keys) rec
                                 (if (= "D" (get result "sys_change_operation"))
                                   (assoc rec "_sdc_deleted_at" (get result "commit_time"))
                                   rec)
                                 (singer-transform/transform catalog stream-name rec))]
                    (singer-messages/write-record! stream-name state record)
                    (->> (singer-bookmarks/update-last-pk-fetched stream-name bookmark-keys st record)
                         (update-current-log-version stream-name
                                                     (get result "sys_change_version"))
                         (singer-messages/write-state-buffered! stream-name))))
                state
                (jdbc/reducible-query (assoc (config/->conn-map config)
                                             :dbname dbname)
                                      sql-params
                                      {:raw? true}))
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
