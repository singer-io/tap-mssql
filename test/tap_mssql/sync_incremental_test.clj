(ns tap-mssql.sync-incremental-test
  (:require [tap-mssql.catalog :as catalog]
            [tap-mssql.config :as config]
            [clojure.test :refer [is deftest]]
            [clojure.java.jdbc :as jdbc]
            [clojure.data.json :as json]
            [clojure.string :as string]
            [tap-mssql.core :refer :all]
            [tap-mssql.test-utils :refer [with-out-and-err-to-dev-null
                                          test-db-config
                                          test-db-configs
                                          with-matrix-assertions]]))

(defn get-destroy-database-command
  [database]
  (format "DROP DATABASE %s" (:table_cat database)))

(defn maybe-destroy-test-db
  [config]
  (let [destroy-database-commands (->> (catalog/get-databases config)
                                       (filter catalog/non-system-database?)
                                       (map get-destroy-database-command))]
    (let [db-spec (config/->conn-map config)]
      (jdbc/db-do-commands db-spec destroy-database-commands))))

(defn create-test-db
  [config]
  (let [db-spec (config/->conn-map config)]
    (jdbc/db-do-commands db-spec ["CREATE DATABASE incremental_sync_test"])
    (jdbc/db-do-commands (assoc db-spec :dbname "incremental_sync_test")
                         [(jdbc/create-table-ddl
                           "data_table"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]])])))

(defn populate-data
  [config]
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "incremental_sync_test"))
                      "data_table"
                      (take 200 (map (partial hash-map :value) (range)))))

(defn test-db-fixture [f config]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db config)
    (create-test-db config)
    (populate-data config)
    (f)))

(defn get-messages-from-output
  ([config]
   (get-messages-from-output config nil))
  ([config table]
   (get-messages-from-output
    config
    table
    (catalog/discover config)))
  ([config table catalog]
   (get-messages-from-output config table {} catalog))
  ([config table state catalog]
   (as-> (with-out-str
           (try
             (do-sync config catalog state)
             (catch Exception e (when (not (:ignore (ex-data e)))
                                  (throw e )))))
       output
       (string/split output #"\n")
       (filter (complement empty?) output)
       (map json/read-str
            output)
       (if table
         (filter (comp (partial = (name table)) #(% "stream"))
                 output)
         output)
       (vec output))))

(defn select-stream
  [stream-name catalog]
  (-> (assoc-in catalog ["streams" stream-name "metadata" "selected"] true)
      (assoc-in ["streams" stream-name "metadata" "replication-method"] "INCREMENTAL")))

(defn set-replication-key
  [stream-name replication-key catalog]
  (assoc-in catalog ["streams" stream-name "metadata" "replication-key"] replication-key))

(def record-count (atom 0))

(deftest ^:integration verify-incremental-sync-works
  (with-matrix-assertions test-db-configs test-db-fixture
    (let [selected-catalog (->> (catalog/discover test-db-config)
                                (select-stream "incremental_sync_test-dbo-data_table")
                                (set-replication-key "incremental_sync_test-dbo-data_table" "value"))
          first-messages (->> selected-catalog
                              (get-messages-from-output test-db-config nil))
          end-state (->> first-messages
                         (filter #(= "STATE" (% "type")))
                         last)]
      (is (= 200
             (->> first-messages
                 (filter #(= "RECORD" (% "type")))
                 count)))

      ;; Insert and update some rows
      (let [db-spec (config/->conn-map test-db-config)]
        (jdbc/db-do-commands (assoc db-spec :dbname "incremental_sync_test")
                             ["INSERT INTO dbo.data_table (value) VALUES (404)"])
        (jdbc/db-do-commands (assoc db-spec :dbname "incremental_sync_test")
                             ["UPDATE dbo.data_table SET value=205 WHERE value=199"]))

      ;; Sync again and inspect the results
      (let [second-messages (->> selected-catalog
                                 (get-messages-from-output test-db-config nil (get end-state "value")))
            end-state (->> second-messages
                           (filter #(= "STATE" (% "type")))
                           last)]
        (is (= 2 (->> second-messages
                      (filter #(= "RECORD" (% "type")))
                      count)))
        (is (= 404 (get-in end-state ["value" "bookmarks" "incremental_sync_test-dbo-data_table" "replication_key_value"])))))))
