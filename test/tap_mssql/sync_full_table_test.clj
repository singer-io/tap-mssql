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
                            [:value "int"]
                            [:deselected_value "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_sync_test")
                         [(jdbc/create-table-ddl
                           "data_table_2"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]])])))

(defn populate-data
  []
  (jdbc/insert-multi! (-> (config->conn-map test-db-config)
                          (assoc :dbname "full_table_sync_test"))
                      "data_table"
                      (take 1000 (map (partial hash-map :deselected_value nil :value) (range))))
  (jdbc/insert-multi! (-> (config->conn-map test-db-config)
                          (assoc :dbname "full_table_sync_test"))
                      "data_table_2"
                      (take 1000 (map (partial hash-map :value) (range)))))

(defn test-db-fixture [f]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db)
    (create-test-db)
    (populate-data)
    (f)))

(use-fixtures :each test-db-fixture)

(defn get-messages-from-output
  ([]
   (get-messages-from-output nil))
  ([table]
   (get-messages-from-output
    (discover-catalog test-db-config)
    table))
  ([catalog table]
   (as-> (with-out-str
           (do-sync test-db-config catalog {}))
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

(deftest ^:integration verify-full-table-sync-with-no-tables-selected
  ;; do-sync prints a bunch of stuff and returns an empty state
  (is (valid-state? (do-sync test-db-config (discover-catalog test-db-config) {})))
  (is (empty? (get-messages-from-output))))

(defn select-stream
  [catalog stream-name]
  (assoc-in catalog [:streams stream-name :metadata :selected] true))

(defn deselect-field
  [catalog stream-name field-name]
  (assoc-in catalog [:streams stream-name :metadata :properties field-name :selected] false))

(deftest ^:integration verify-full-table-sync-with-one-table-selected
  ;; This also verifies selected-by-default
  ;; do-sync prints a bunch of stuff and returns nil
  (is (valid-state? (do-sync test-db-config (discover-catalog test-db-config) {})))
  ;; Emits schema message
  (is (= "full_table_sync_test-dbo-data_table"
         ((-> (discover-catalog test-db-config)
              (select-stream "data_table")
              (get-messages-from-output "data_table")
              first)
          "tap_stream_id")))
  (is (= "data_table"
         ((-> (discover-catalog test-db-config)
              (select-stream "data_table")
              (get-messages-from-output "data_table")
              first)
          "table_name")))
  (is (= {"type" "string"
          "pattern" "[A-F0-9]{8}-([A-F0-9]{4}-){3}[A-F0-9]{12}"}
         (get-in (-> (discover-catalog test-db-config)
                     (select-stream "data_table")
                     (get-messages-from-output "data_table")
                     first)
                 ["schema" "properties" "id"])))
  (is (not (contains? ((-> (discover-catalog test-db-config)
                           (select-stream "data_table")
                           (get-messages-from-output "data_table")
                           first)
                       "schema")
                      "metadata")))
  ;; Emits the records expected
  (is (= 1002
         (-> (discover-catalog test-db-config)
             (select-stream "data_table")
             (get-messages-from-output nil)
             count)))
  (is (every? (fn [rec]
                (= "data_table" (rec "stream")))
              (-> (discover-catalog test-db-config)
                  (select-stream "data_table")
                  (get-messages-from-output nil))))
  (is (= "RECORD"
         (get-in (-> (discover-catalog test-db-config)
                     (select-stream "data_table")
                     (get-messages-from-output nil))
                 [1 "type"])))
  ;; At the moment we're not ordering by anything so checking the actual
  ;; value here would be brittle, I think.
  (is (every? #(get-in % ["record" "value"])
              (as-> (discover-catalog test-db-config)
                  x
                  (select-stream x "data_table")
                  (get-messages-from-output x nil)
                  (drop 1 x)
                  (take 1000 x))))
  (is (every? #(contains? (% "record") "deselected_value")
              (as-> (discover-catalog test-db-config)
                  x
                (select-stream x "data_table")
                ;; Don't select or deselect any fields and let
                ;; selected-by-default do all the work
                #_(deselect-field x "data_table" "deselected_value")
                (get-messages-from-output x nil)
                (drop 1 x)
                (take 1000 x))))
  (is (= "STATE"
         (get-in (-> (discover-catalog test-db-config)
                     (select-stream "data_table")
                     (get-messages-from-output nil))
                 [1001 "type"]))))

(deftest ^:integration verify-full-table-sync-with-one-table-selected-and-one-field-deselected
  ;; do-sync prints a bunch of stuff and returns nil
  (is (valid-state? (do-sync test-db-config (discover-catalog test-db-config) {})))
  ;; Emits schema message
  (is (= "full_table_sync_test-dbo-data_table"
         ((-> (discover-catalog test-db-config)
              (select-stream "data_table")
              (get-messages-from-output "data_table")
              first)
          "tap_stream_id")))
  (is (= "data_table"
         ((-> (discover-catalog test-db-config)
              (select-stream "data_table")
              (get-messages-from-output "data_table")
              first)
          "table_name")))
  (is (= {"type" "string"
          "pattern" "[A-F0-9]{8}-([A-F0-9]{4}-){3}[A-F0-9]{12}"}
         (get-in (-> (discover-catalog test-db-config)
                     (select-stream "data_table")
                     (get-messages-from-output "data_table")
                     first)
                 ["schema" "properties" "id"])))
  (is (not (contains? ((-> (discover-catalog test-db-config)
                           (select-stream "data_table")
                           (get-messages-from-output "data_table")
                           first)
                       "schema")
                      "metadata")))
  ;; Emits the records expected
  (is (= 1002
         (-> (discover-catalog test-db-config)
             (select-stream "data_table")
             (get-messages-from-output nil)
             count)))
  (is (every? (fn [rec]
                (= "data_table" (rec "stream")))
              (-> (discover-catalog test-db-config)
                  (select-stream "data_table")
                  (get-messages-from-output nil))))
  (is (= "RECORD"
         (get-in (-> (discover-catalog test-db-config)
                     (select-stream "data_table")
                     (get-messages-from-output nil))
                 [1 "type"])))
  ;; At the moment we're not ordering by anything so checking the actual
  ;; value here would be brittle, I think.
  (is (every? #(get-in % ["record" "value"])
              (as-> (discover-catalog test-db-config)
                  x
                (select-stream x "data_table")
                (get-messages-from-output x nil)
                (drop 1 x)
                (take 1000 x))))
  (is (every? #(not (contains? (% "record") "deselected_value"))
              (as-> (discover-catalog test-db-config)
                  x
                (select-stream x "data_table")
                (deselect-field x "data_table" "deselected_value")
                (get-messages-from-output x nil)
                (drop 1 x)
                (take 1000 x))))
  (is (= "STATE"
         (get-in (-> (discover-catalog test-db-config)
                     (select-stream "data_table")
                     (get-messages-from-output nil))
                 [1001 "type"]))))
