(ns tap-mssql.sync-full-table-test
  (:require [clojure.test :refer [is deftest use-fixtures]]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as string]
            [tap-mssql.core :refer :all]
            [tap-mssql.test-utils :refer [with-out-and-err-to-dev-null]]))

(defn get-test-hostname
  []
  (let [hostname (.getHostName (java.net.InetAddress/getLocalHost))]
    (if (string/starts-with? hostname "taps-")
      hostname
      "circleci")))

(def test-db-config
  {"host" (format "%s-test-mssql-2017.db.test.stitchdata.com"
                  (get-test-hostname))
   "user" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_USER")
   "password" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD")
   "port" "1433"})

(defn get-destroy-database-command
  [database]
  (format "DROP DATABASE %s" (:table_cat database)))

(defn maybe-destroy-test-db
  []
  (let [destroy-database-commands (->> (get-databases test-db-config)
                                       (filter non-system-database?)
                                       (map get-destroy-database-command))]
    (let [db-spec (config->conn-map test-db-config)]
      (jdbc/db-do-commands db-spec destroy-database-commands))))

(defn create-test-db
  []
  (let [db-spec (config->conn-map test-db-config)]
    (jdbc/db-do-commands db-spec ["CREATE DATABASE full_table_sync_test"])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_sync_test")
                         [(jdbc/create-table-ddl
                           "data_table"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_sync_test")
                         [(jdbc/create-table-ddl
                           "data_table_2"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]])])))

(defn populate-data
  []
  (jdbc/insert-multi! (-> (config->conn-map test-db-config)
                          (assoc :dbname "full_table_sync_test"))
                      :data_table
                      (take 1000 (map (partial hash-map :value) (range))))
  (jdbc/insert-multi! (-> (config->conn-map test-db-config)
                          (assoc :dbname "full_table_sync_test"))
                      :data_table_2
                      (take 1000 (map (partial hash-map :value) (range)))))

(defn test-db-fixture [f]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db)
    (create-test-db)
    (populate-data)
    (f)))

(use-fixtures :each test-db-fixture)

(defn get-schema-message-from-output
  []
  ;; json/read-str simply grabs the first line of the string and
  ;; returns that rather than giving you a sequence of objects
  (json/read-str
   (with-out-str (do-sync test-db-config
                          (discover-catalog test-db-config)
                          {}))))

(defn get-messages-from-output
  [table]
  (as-> (with-out-str
          (do-sync test-db-config
                   (discover-catalog test-db-config)
                   {})) output
    (string/split output #"\n")
    (map json/read-str output)
    (filter (comp (partial = (name table)) #(% "stream"))
            output)
    (vec output)))

(deftest ^:integration verify-full-table-sync
  ;; do-sync prints a bunch of stuff and returns nil
  (is (valid-state? (do-sync test-db-config (discover-catalog test-db-config) {})))
  ;; Emits schema message
  (is (= "SCHEMA"
         ((get-schema-message-from-output) "type")))
  (is (= "full_table_sync_test-dbo-data_table"
         ((get-schema-message-from-output) "tap_stream_id")))
  (is (= "data_table"
         ((get-schema-message-from-output) "table_name")))
  (is (= {"type" "string"
          "pattern" "[A-F0-9]{8}-([A-F0-9]{4}-){3}[A-F0-9]{12}"}
         (get-in (get-schema-message-from-output) ["schema" "properties" "id"])))
  (is (not (contains? ((get-schema-message-from-output) "schema") "metadata")))
  ;; Emits the records expected
  (is (= 1002
         (count (get-messages-from-output :data_table))))
  (is (= 1002
         (count (get-messages-from-output :data_table_2))))
  (is (= "RECORD"
         (get-in (get-messages-from-output :data_table)
                 [1 "type"])))
  (is (= "RECORD"
         (get-in (get-messages-from-output :data_table_2)
                 [1 "type"])))
  ;; At the moment we're not ordering by anything so checking the actual
  ;; value here would be brittle, I think.
  (is (get-in (get-messages-from-output :data_table)
              [1 "record" "value"]))
  (is (get-in (get-messages-from-output :data_table_2)
              [1 "record" "value"]))
  (is (get-in (get-messages-from-output :data_table)
              [1000 "record" "value"]))
  (is (get-in (get-messages-from-output :data_table_2)
              [1000 "record" "value"]))
  (is (= "STATE"
         (get-in (get-messages-from-output :data_table)
                 [1001 "type"])))
  (is (= "STATE"
         (get-in (get-messages-from-output :data_table_2)
                 [1001 "type"]))))
