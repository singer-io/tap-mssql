(ns tap-mssql.sync-strategies.incremental
  (:require [tap-mssql.config :as config]
            [tap-mssql.singer.fields :as singer-fields]
            [tap-mssql.singer.bookmarks :as singer-bookmarks]
            [tap-mssql.singer.messages :as singer-messages]
            [tap-mssql.sync-strategies.common :as common]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure.java.jdbc :as jdbc]))

(defn build-incremental-sync-query
  [stream-name schema-name table-name record-keys replication-key state]
  {:pre [(not (empty? record-keys))]} ;; Is there more incoming state that we think is worth asserting?
  (let [replication-key-name (get-in state ["bookmarks" stream-name "replication_key_name"])
        replication-key-value (get-in state ["bookmarks" stream-name "replication_key_value"])
        bookmarking-clause    (format "%s >= ?" replication-key)
        add-where-clause?     (and (some? replication-key-value)
                                   (= replication-key replication-key-name)) ;; if the replication-key in metadata changes, we negate our bookmark
        where-clause          (when add-where-clause?
                                (str " WHERE " bookmarking-clause))
        order-by              (str " ORDER BY " replication-key)
        sql-params            [(str (format "SELECT %s FROM %s.%s"
                                            (string/join ", " (map common/sanitize-names record-keys))
                                            (common/sanitize-names schema-name)
                                            (common/sanitize-names table-name))
                                    where-clause
                                    order-by)]]
    (if add-where-clause?
      (concat sql-params
              [replication-key-value])
      sql-params)))

(defn sync-and-write-messages!
  "Syncs all records, states, returns the latest state. Ensures that the
  bookmark we have for this stream matches our understanding of the fields
  defined in the catalog that are bookmark-able."
  [config catalog stream-name state]
  (let [dbname          (get-in catalog ["streams" stream-name "metadata" "database-name"])
        record-keys     (singer-fields/get-selected-fields catalog stream-name)
        table-name      (get-in catalog ["streams" stream-name "table_name"])
        schema-name     (get-in catalog ["streams" stream-name "metadata" "schema-name"])
        replication-key (get-in catalog ["streams" stream-name "metadata" "replication-key"])
        sql-params      (build-incremental-sync-query stream-name
                                                      schema-name
                                                      table-name
                                                      record-keys
                                                      replication-key
                                                      state)]
    (log/infof "Executing query: %s" (pr-str sql-params))
    (reduce (fn [acc result]
              (let [record (select-keys result record-keys)]
                (singer-messages/write-record! stream-name acc record catalog)
                (->> (singer-bookmarks/update-state stream-name replication-key record acc)
                     (singer-messages/write-state-buffered! stream-name))))
            state
            (jdbc/reducible-query (assoc (config/->conn-map config)
                                         :dbname dbname)
                                  sql-params
                                  common/result-set-opts))))

(defn sync!
  [config catalog stream-name state]
  (->> state
       (singer-messages/write-activate-version! stream-name catalog)
       (singer-messages/write-state! stream-name)
       (sync-and-write-messages! config catalog stream-name)
       (singer-messages/write-activate-version! stream-name catalog)))
