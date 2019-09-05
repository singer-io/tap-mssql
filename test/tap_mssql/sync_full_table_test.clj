(ns tap-mssql.sync-full-table-test
  (:import  [microsoft.sql.DateTimeOffset])
  (:require [tap-mssql.catalog :as catalog]
            [tap-mssql.config :as config]
            [clojure.test :refer [is deftest]]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.data.json :as json]
            [clojure.set :as set]
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
                            [:value "int"]])])

   (jdbc/db-do-commands (assoc db-spec :dbname "full_table_sync_test")
                         [(jdbc/create-table-ddl
                           "data_table_3"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]
                            [:date1 "datetimeoffset"]
                            [:date2 "datetime2"]
                            [:date3 "smalldatetime"]])])))

(defn populate-data
  [config]
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "full_table_sync_test"))
                      "data_table"
                      (take 100 (map (partial hash-map :deselected_value nil :value) (range))))
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "full_table_sync_test"))
                      "data_table_2"
                      (take 100 (map (partial hash-map :value) (range))))
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "full_table_sync_test"))
                      "data_table_3"
                      (take 100
                            (map #(hash-map :value %
                                            :date1 (microsoft.sql.DateTimeOffset/valueOf (java.sql.Timestamp. (- (System/currentTimeMillis) (* % 10000))) 0)
                                            :date2 (java.sql.Timestamp. (-  (System/currentTimeMillis) (* % 10000)))
                                            :date3 (java.sql.Timestamp. (-  (System/currentTimeMillis) (* % 10000))))
                                 (range)))))


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
    (catalog/discover config)
    config
    table))
  ([catalog config table]
   (as-> (with-out-str
           (do-sync config catalog {}))
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
  (with-matrix-assertions test-db-configs test-db-fixture
    (is (valid-state? (do-sync test-db-config (catalog/discover test-db-config) {})))
    (is (empty? (get-messages-from-output test-db-config)))))

(defn select-stream
  ([catalog stream-name]
   (select-stream catalog stream-name "FULL_TABLE"))
  ([catalog stream-name method]
   (-> (assoc-in catalog ["streams" stream-name "metadata" "selected"] true)
       (assoc-in ["streams" stream-name "metadata" "replication-method"] method))))

(defn deselect-field
  [catalog stream-name field-name]
  (assoc-in catalog ["streams" stream-name "metadata" "properties" field-name "selected"] false))

(deftest ^:integration verify-full-table-sync-with-one-table-selected
  (with-matrix-assertions test-db-configs test-db-fixture
    (let [test-db-config (assoc test-db-config "include_schemas_in_destination_stream_name" "true")
          _ (set-include-db-and-schema-names-in-messages! test-db-config)
          all-messages  (-> (catalog/discover test-db-config)
                            (select-stream "full_table_sync_test_dbo_data_table")
                            (get-messages-from-output test-db-config "full_table_sync_test_dbo_data_table"))
          first-message (-> all-messages
                            first)]
      ;; REFERENCE: Current expected order of one table
      ;;     SCHEMA, ACTIVATE_VERSION, STATE, 1k x RECORD, ACTIVATE_VERSION, STATE

      ;; This also verifies selected-by-default
      ;; do-sync prints a bunch of stuff and returns nil
      (is (valid-state? (do-sync test-db-config (catalog/discover test-db-config) {})))
      ;; Emits schema message
      (is (= "full_table_sync_test_dbo_data_table"
             (first-message "stream")))
      (is (= ["id"]
             (first-message "key_properties")))
      (is (= {"type"    ["string"]
              "pattern" "[A-F0-9]{8}-([A-F0-9]{4}-){3}[A-F0-9]{12}"}
             (get-in first-message ["schema" "properties" "id"])))
      (is (not (contains? (first-message "schema") "metadata")))
      ;; Emits the records expected
      (is (= 100
             (->> all-messages
                  (filter #(= "RECORD" (% "type")))
                  count)))
      (is (every? (fn [rec]
                    (= "full_table_sync_test_dbo_data_table" (rec "stream")))
                  all-messages))
      ;; At the moment we're not ordering by anything so checking the actual
      ;; value here would be brittle, I think.
      (is (every? #(get-in % ["record" "value"])
                  (->> all-messages
                      (filter #(= "RECORD" (% "type"))))))
      (is (= "STATE"
             (-> all-messages
                 last
                 (get "type")))
          "Last message in a complete sync must be state"))))

(deftest ^:integration verify-full-table-sync-with-one-table-selected-and-one-field-deselected
  (with-matrix-assertions test-db-configs test-db-fixture ;; do-sync prints a bunch of stuff and returns nil
    (let [test-db-config (assoc test-db-config "include_schemas_in_destination_stream_name" "true")]
      (set-include-db-and-schema-names-in-messages! test-db-config)
      (is (valid-state? (do-sync test-db-config (catalog/discover test-db-config) {})))
      ;; Emits schema message
      (is (= "full_table_sync_test_dbo_data_table"
             ((-> (catalog/discover test-db-config)
                  (select-stream "full_table_sync_test_dbo_data_table")
                  (get-messages-from-output test-db-config "full_table_sync_test_dbo_data_table")
                  first)
              "stream")))
      (is (= ["id"]
             ((-> (catalog/discover test-db-config)
                  (select-stream "full_table_sync_test_dbo_data_table")
                  (get-messages-from-output test-db-config "full_table_sync_test_dbo_data_table")
                  first)
              "key_properties")))
      (is (= {"type" ["string"]
              "pattern" "[A-F0-9]{8}-([A-F0-9]{4}-){3}[A-F0-9]{12}"}
             (get-in (-> (catalog/discover test-db-config)
                         (select-stream "full_table_sync_test_dbo_data_table")
                         (get-messages-from-output test-db-config "full_table_sync_test_dbo_data_table")
                         first)
                     ["schema" "properties" "id"])))
      (is (not (contains? ((-> (catalog/discover test-db-config)
                               (select-stream "full_table_sync_test_dbo_data_table")
                               (get-messages-from-output test-db-config "full_table_sync_test_dbo_data_table")
                               first)
                           "schema")
                          "metadata")))
      ;; Emits the records expected
      (is (= 100
             (-> (catalog/discover test-db-config)
                 (select-stream "full_table_sync_test_dbo_data_table")
                 (get-messages-from-output test-db-config nil)
                 ((partial filter #(= "RECORD" (% "type"))))
                 count)))
      (is (every? (fn [rec]
                    (= "full_table_sync_test_dbo_data_table" (rec "stream")))
                  (-> (catalog/discover test-db-config)
                      (select-stream "full_table_sync_test_dbo_data_table")
                      (get-messages-from-output test-db-config nil))))
      ;; At the moment we're not ordering by anything so checking the actual
      ;; value here would be brittle, I think.
      (is (every? #(get-in % ["record" "value"])
                  (as-> (catalog/discover test-db-config)
                      x
                    (select-stream x "full_table_sync_test_dbo_data_table")
                    (get-messages-from-output x test-db-config nil)
                    (filter #(= "RECORD" (% "type")) x))))
      (is (every? #(not (contains? (% "record") "deselected_value"))
                  (as-> (catalog/discover test-db-config)
                      x
                    (select-stream x "full_table_sync_test_dbo_data_table")
                    (deselect-field x "full_table_sync_test_dbo_data_table" "deselected_value")
                    (get-messages-from-output x test-db-config nil)
                    (filter #(= "RECORD" (% "type")) x))))
      (is (= "STATE"
             ((-> (catalog/discover test-db-config)
                  (select-stream "full_table_sync_test_dbo_data_table")
                  (get-messages-from-output test-db-config nil)
                  last)
              "type"))
          "Last message in a complete sync must be state"))
    )
  )

(deftest ^:integration verify-activate-version-emitted-on-full-table-sync
  (with-matrix-assertions test-db-configs test-db-fixture ;; Emits Activate Version Messages at the right times
    (is (= "ACTIVATE_VERSION"
           (get (-> (catalog/discover test-db-config)
                    (select-stream "full_table_sync_test_dbo_data_table")
                    (get-messages-from-output test-db-config nil)
                    second)
                "type")))
    (is (= "STATE" ;; 3rd message should be a state with the table version
           (get (-> (catalog/discover test-db-config)
                    (select-stream "full_table_sync_test_dbo_data_table")
                    (get-messages-from-output test-db-config nil)
                    (nth 2))
                "type")))
    (is (= "ACTIVATE_VERSION" ;; Last activate version
           (get (as-> (catalog/discover test-db-config)
                    x
                    (select-stream x "full_table_sync_test_dbo_data_table")
                    (get-messages-from-output x test-db-config nil)
                    (take-last 2 x)
                    (first x))
                "type"))
        "Second to last message on full table sync should be Activate Version"))
  )




(deftest ^:integration verify-full-table-sync-with-datetimes
  (with-matrix-assertions test-db-configs test-db-fixture
    (let [test-db-config (assoc test-db-config "include_schemas_in_destination_stream_name" "true")
          _ (set-include-db-and-schema-names-in-messages! test-db-config)
          all-messages  (-> (catalog/discover test-db-config)
                            (select-stream "full_table_sync_test_dbo_data_table_3")
                            (get-messages-from-output test-db-config "full_table_sync_test_dbo_data_table_3"))
          first-message (-> all-messages
                            first)]
      ;; REFERENCE: Current expected order of one table
      ;;     SCHEMA, ACTIVATE_VERSION, STATE, 1k x RECORD, ACTIVATE_VERSION, STATE

      ;; This also verifies selected-by-default
      ;; do-sync prints a bunch of stuff and returns nil
      (is (valid-state? (do-sync test-db-config (catalog/discover test-db-config) {})))
      ;; Emits schema message
      (is (= "full_table_sync_test_dbo_data_table_3"
             (first-message "stream")))
      (is (= ["id"]
             (first-message "key_properties")))
      (is (= {"type"    ["string"]
              "pattern" "[A-F0-9]{8}-([A-F0-9]{4}-){3}[A-F0-9]{12}"}
             (get-in first-message ["schema" "properties" "id"])))
      (is (not (contains? (first-message "schema") "metadata")))
      ;; Emits the records expected
      (is (= 100
             (->> all-messages
                  (filter #(= "RECORD" (% "type")))
                  count)))
      (is (every? (fn [rec]
                    (= "full_table_sync_test_dbo_data_table_3" (rec "stream")))
                  all-messages))
      ;; At the moment we're not ordering by anything so checking the actual
      ;; value here would be brittle, I think.
      (is (every? #(get-in % ["record" "value"])
                  (->> all-messages
                      (filter #(= "RECORD" (% "type"))))))
      (is (= "STATE"
             (-> all-messages
                 last
                 (get "type")))
          "Last message in a complete sync must be state"))))
