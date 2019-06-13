(ns tap-mssql.sync-full-table-test
  (:require [clojure.test :refer [is deftest]]
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
  (let [destroy-database-commands (->> (get-databases config)
                                       (filter non-system-database?)
                                       (map get-destroy-database-command))]
    (let [db-spec (config->conn-map config)]
      (jdbc/db-do-commands db-spec destroy-database-commands))))

(defn create-test-db
  [config]
  (let [db-spec (config->conn-map config)]
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
  [config]
  (jdbc/insert-multi! (-> (config->conn-map config)
                          (assoc :dbname "full_table_sync_test"))
                      "data_table"
                      (take 100 (map (partial hash-map :deselected_value nil :value) (range))))
  (jdbc/insert-multi! (-> (config->conn-map config)
                          (assoc :dbname "full_table_sync_test"))
                      "data_table_2"
                      (take 100 (map (partial hash-map :value) (range)))))

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
    (discover-catalog config)
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
    (is (valid-state? (do-sync test-db-config (discover-catalog test-db-config) {})))
    (is (empty? (get-messages-from-output test-db-config)))))

(defn select-stream
  [catalog stream-name]
  (assoc-in catalog ["streams" stream-name "metadata" "selected"] true))

(defn deselect-field
  [catalog stream-name field-name]
  (assoc-in catalog ["streams" stream-name "metadata" "properties" field-name "selected"] false))

(deftest ^:integration verify-full-table-sync-with-one-table-selected
  (with-matrix-assertions test-db-configs test-db-fixture
    ;; REFERENCE: Current expected order of one table
    ;;     SCHEMA, ACTIVATE_VERSION, STATE, 1k x RECORD, ACTIVATE_VERSION, STATE

    ;; This also verifies selected-by-default
    ;; do-sync prints a bunch of stuff and returns nil
    (is (valid-state? (do-sync test-db-config (discover-catalog test-db-config) {})))
    ;; Emits schema message
    (is (= "full_table_sync_test-dbo-data_table"
           ((-> (discover-catalog test-db-config)
                (select-stream "full_table_sync_test-dbo-data_table")
                (get-messages-from-output test-db-config "full_table_sync_test-dbo-data_table")
                first)
            "stream")))
    (is (= ["id"]
           ((-> (discover-catalog test-db-config)
                (select-stream "full_table_sync_test-dbo-data_table")
                (get-messages-from-output test-db-config "full_table_sync_test-dbo-data_table")
                first)
            "key_properties")))
    (is (= {"type" ["string"]
            "pattern" "[A-F0-9]{8}-([A-F0-9]{4}-){3}[A-F0-9]{12}"}
           (get-in (-> (discover-catalog test-db-config)
                       (select-stream "full_table_sync_test-dbo-data_table")
                       (get-messages-from-output test-db-config "full_table_sync_test-dbo-data_table")
                       first)
                   ["schema" "properties" "id"])))
    (is (not (contains? ((-> (discover-catalog test-db-config)
                             (select-stream "full_table_sync_test-dbo-data_table")
                             (get-messages-from-output test-db-config "full_table_sync_test-dbo-data_table")
                             first)
                         "schema")
                        "metadata")))
    ;; Emits the records expected
    (is (= 100
           (-> (discover-catalog test-db-config)
               (select-stream "full_table_sync_test-dbo-data_table")
               (get-messages-from-output test-db-config nil)
               ((partial filter #(= "RECORD" (% "type"))))
               count)))
    (is (every? (fn [rec]
                  (= "full_table_sync_test-dbo-data_table" (rec "stream")))
                (-> (discover-catalog test-db-config)
                    (select-stream "full_table_sync_test-dbo-data_table")
                    (get-messages-from-output test-db-config nil))))
    ;; At the moment we're not ordering by anything so checking the actual
    ;; value here would be brittle, I think.
    (is (every? #(get-in % ["record" "value"])
                (as-> (discover-catalog test-db-config)
                    x
                    (select-stream x "full_table_sync_test-dbo-data_table")
                    (get-messages-from-output x test-db-config nil)
                    (filter #(= "RECORD" (% "type")) x))))
    (is (= "STATE"
           ((-> (discover-catalog test-db-config)
                (select-stream "full_table_sync_test-dbo-data_table")
                (get-messages-from-output test-db-config nil)
                last)
            "type"))
        "Last message in a complete sync must be state")))

(deftest ^:integration verify-full-table-sync-with-one-table-selected-and-one-field-deselected
  (with-matrix-assertions test-db-configs test-db-fixture ;; do-sync prints a bunch of stuff and returns nil
    (is (valid-state? (do-sync test-db-config (discover-catalog test-db-config) {})))
    ;; Emits schema message
    (is (= "full_table_sync_test-dbo-data_table"
           ((-> (discover-catalog test-db-config)
                (select-stream "full_table_sync_test-dbo-data_table")
                (get-messages-from-output test-db-config "full_table_sync_test-dbo-data_table")
                first)
            "stream")))
    (is (= {"type" ["string"]
            "pattern" "[A-F0-9]{8}-([A-F0-9]{4}-){3}[A-F0-9]{12}"}
           (get-in (-> (discover-catalog test-db-config)
                       (select-stream "full_table_sync_test-dbo-data_table")
                       (get-messages-from-output test-db-config "full_table_sync_test-dbo-data_table")
                       first)
                   ["schema" "properties" "id"])))
    (is (not (contains? ((-> (discover-catalog test-db-config)
                             (select-stream "full_table_sync_test-dbo-data_table")
                             (get-messages-from-output test-db-config "full_table_sync_test-dbo-data_table")
                             first)
                         "schema")
                        "metadata")))
    ;; Emits the records expected
    (is (= 100
           (-> (discover-catalog test-db-config)
               (select-stream "full_table_sync_test-dbo-data_table")
               (get-messages-from-output test-db-config nil)
               ((partial filter #(= "RECORD" (% "type"))))
               count)))
    (is (every? (fn [rec]
                  (= "full_table_sync_test-dbo-data_table" (rec "stream")))
                (-> (discover-catalog test-db-config)
                    (select-stream "full_table_sync_test-dbo-data_table")
                    (get-messages-from-output test-db-config nil))))
    ;; At the moment we're not ordering by anything so checking the actual
    ;; value here would be brittle, I think.
    (is (every? #(get-in % ["record" "value"])
                (as-> (discover-catalog test-db-config)
                    x
                    (select-stream x "full_table_sync_test-dbo-data_table")
                    (get-messages-from-output x test-db-config nil)
                    (filter #(= "RECORD" (% "type")) x))))
    (is (every? #(not (contains? (% "record") "deselected_value"))
                (as-> (discover-catalog test-db-config)
                    x
                    (select-stream x "full_table_sync_test-dbo-data_table")
                    (deselect-field x "full_table_sync_test-dbo-data_table" "deselected_value")
                    (get-messages-from-output x test-db-config nil)
                    (filter #(= "RECORD" (% "type")) x))))
    (is (= "STATE"
           ((-> (discover-catalog test-db-config)
                (select-stream "full_table_sync_test-dbo-data_table")
                (get-messages-from-output test-db-config nil)
                last)
            "type"))
        "Last message in a complete sync must be state")
    )
  )

(deftest ^:integration verify-activate-version-emitted-on-full-table-sync
  (with-matrix-assertions test-db-configs test-db-fixture ;; Emits Activate Version Messages at the right times
    (is (= "ACTIVATE_VERSION"
           (get (-> (discover-catalog test-db-config)
                    (select-stream "full_table_sync_test-dbo-data_table")
                    (get-messages-from-output test-db-config nil)
                    second)
                "type")))
    (is (= "STATE" ;; 3rd message should be a state with the table version
           (get (-> (discover-catalog test-db-config)
                    (select-stream "full_table_sync_test-dbo-data_table")
                    (get-messages-from-output test-db-config nil)
                    (nth 2))
                "type")))
    (is (= "ACTIVATE_VERSION" ;; Last activate version
           (get (as-> (discover-catalog test-db-config)
                    x
                    (select-stream x "full_table_sync_test-dbo-data_table")
                    (get-messages-from-output x test-db-config nil)
                    (take-last 2 x)
                    (first x))
                "type"))
        "Second to last message on full table sync should be Activate Version"))
  )
