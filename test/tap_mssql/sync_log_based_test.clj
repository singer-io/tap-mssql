(ns tap-mssql.sync-log-based-test
  (:require
            [tap-mssql.catalog :as catalog]
            [tap-mssql.config :as config]
            [clojure.test :refer [is deftest]]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.data]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as string]
            [tap-mssql.core :refer :all]
            [tap-mssql.sync-strategies.logical :as logical]
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
    (jdbc/db-do-commands db-spec ["CREATE DATABASE log_based_sync_test"])
    (jdbc/db-do-commands (assoc db-spec :dbname "log_based_sync_test")
                         ["CREATE SCHEMA schema_with_conflict"])
    (jdbc/db-do-commands (assoc db-spec :dbname "log_based_sync_test")
                         ["CREATE SCHEMA schema_with_table"])
    (jdbc/db-do-commands (assoc db-spec :dbname "log_based_sync_test")
                         [(jdbc/create-table-ddl
                           "data_table"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]
                            [:deselected_value "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "log_based_sync_test")
                         [(jdbc/create-table-ddl
                           "data_table_2"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]])])
    ;; Same table as below, created first to check for unexpected failures with ignoring schema
    (jdbc/db-do-commands (assoc db-spec :dbname "log_based_sync_test")
                         ["CREATE TABLE schema_with_conflict.data_table (id uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID(), value int)"])
    (jdbc/db-do-commands (assoc db-spec :dbname "log_based_sync_test")
                         ["CREATE TABLE schema_with_table.data_table (id uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID(), value int)"])))

(defn populate-data
  [config]
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "log_based_sync_test"))
                      "dbo.data_table"
                      (take 100 (map (partial hash-map :deselected_value nil :value) (range))))
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "log_based_sync_test"))
                      "dbo.data_table_2"
                      (take 100 (map (partial hash-map :value) (range))))
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "log_based_sync_test"))
                      "schema_with_table.data_table"
                      (take 100 (map (partial hash-map :value) (range)))))

(defn insert-data [config schema-name]
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "log_based_sync_test"))
                      (format "%s.data_table" schema-name)
                      (map (partial hash-map :deselected_value nil :value) (range 100 200)))
  )

(defn update-data [config schema-name]
  (jdbc/execute! (assoc (config/->conn-map config)
                        :dbname "log_based_sync_test")
                 [(str (format "UPDATE %s.data_table " schema-name)
                       "SET value = value + 1 "
                       "WHERE value >= 90")])
  )

(defn delete-data [config schema-name]
  (jdbc/execute! (assoc (config/->conn-map config)
                        :dbname "log_based_sync_test")
                 [(str (format "DELETE FROM %s.data_table " schema-name)
                       "WHERE value >= 90")])
  )

(defn setup-change-tracking-for-database [config]
  (jdbc/execute! (assoc (config/->conn-map config)
                        :dbname "log_based_sync_test")
                 [(str "ALTER DATABASE log_based_sync_test "
                       "SET CHANGE_TRACKING = ON "
                       "(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)")])
  config)

(defn setup-change-tracking-for-table [config]
  (jdbc/execute! (assoc (config/->conn-map config)
                        :dbname "log_based_sync_test")
                 [(str "ALTER TABLE dbo.data_table "
                       "ENABLE CHANGE_TRACKING "
                       "WITH (TRACK_COLUMNS_UPDATED = ON)")])
  (jdbc/execute! (assoc (config/->conn-map config)
                        :dbname "log_based_sync_test")
                 [(str "ALTER TABLE schema_with_table.data_table "
                       "ENABLE CHANGE_TRACKING "
                       "WITH (TRACK_COLUMNS_UPDATED = ON)")])
  config)

(defn test-db-fixture
  ([f config]
   (test-db-fixture identity f config))
  ([specific-setup f config]
   (with-out-and-err-to-dev-null
     (maybe-destroy-test-db config)
     (create-test-db config)
     (populate-data config)
     (setup-change-tracking-for-database config)
     (setup-change-tracking-for-table config)
     (specific-setup config)
     (f))))

(defn get-messages-from-output
  ([config]
   (get-messages-from-output config nil))
  ([config table]
   (get-messages-from-output
    (catalog/discover config)
    config
    table
    {}))
  ([catalog config table]
   (get-messages-from-output
    catalog
    config
    table
    {}))
  ([catalog config table state]
   (as-> (with-out-str
           (do-sync config catalog state))
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
  ([catalog stream-name]
   (select-stream catalog stream-name "FULL_TABLE"))
  ([catalog stream-name method]
   (-> (assoc-in catalog ["streams" stream-name "metadata" "selected"] true)
       (assoc-in ["streams" stream-name "metadata" "replication-method"] method))))

(defn null-fixture [f config]
  ;; Memoization messes with these because they are different
  (with-out-and-err-to-dev-null
    (with-redefs [logical/get-change-tracking-databases logical/get-change-tracking-databases*
                 logical/get-change-tracking-tables logical/get-change-tracking-tables*]
     (f))))

(deftest ^:integration verify-log-based-replication-throws-if-not-enabled-for-database
  (with-matrix-assertions test-db-configs null-fixture
    (do (maybe-destroy-test-db test-db-config)
        (create-test-db test-db-config))
    (is (thrown? UnsupportedOperationException
                 (-> (catalog/discover test-db-config)
                     (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                     (get-messages-from-output test-db-config nil))))))

(deftest ^:integration verify-log-based-replication-throws-if-not-enabled-for-table
  (with-matrix-assertions test-db-configs null-fixture
    (do (maybe-destroy-test-db test-db-config)
        (create-test-db test-db-config)
        (setup-change-tracking-for-database test-db-config))
    (is (thrown? UnsupportedOperationException
                 (-> (catalog/discover test-db-config)
                     (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                     (get-messages-from-output test-db-config nil))))))

(deftest ^:integration verify-log-based-replication-throws-if-no-key-properties
  (with-matrix-assertions test-db-configs null-fixture
    (do (maybe-destroy-test-db test-db-config)
        (create-test-db test-db-config)
        (setup-change-tracking-for-database test-db-config))
    (is (thrown? UnsupportedOperationException
                 (-> (catalog/discover test-db-config)
                     (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                     (assoc-in  ["streams"
                                 "log_based_sync_test_dbo_data_table"
                                 "metadata"
                                 "table-key-properties"]
                               #{})
                     (get-messages-from-output test-db-config nil))))))

(deftest ^:integration verify-log-based-replication-performs-initial-full-table
  ;; TODO: Fixture might need to have another function appended to set up change tracking
  (with-matrix-assertions test-db-configs test-db-fixture
    ;; Check that third state has all keys - log position and full table complete
    (is (nil?
         (let [third-state (as-> (catalog/discover test-db-config)
                               x
                               (select-stream x "log_based_sync_test_dbo_data_table" "LOG_BASED")
                               (get-messages-from-output x test-db-config nil)
                               (filter #(= "STATE" (% "type")) x)
                               (nth x 2))]
           (second ;; In diff, second is the things in second param but not first
            (clojure.data/diff
             (set (keys (get-in third-state ["value"
                                             "bookmarks"
                                             "log_based_sync_test_dbo_data_table"])))
             #{"max_pk_values" "version" "initial_full_table_complete" "current_log_version" "last_pk_fetched"})))))
    ;; Check that second state has false for fulltable complete
    (is (= false
           (let [second-state (as-> (catalog/discover test-db-config)
                                  x
                                  (select-stream x "log_based_sync_test_dbo_data_table" "LOG_BASED")
                                  (get-messages-from-output x test-db-config nil)
                                  (filter #(= "STATE" (% "type")) x)
                                  (second x))]
             (get-in second-state ["value" "bookmarks" "log_based_sync_test_dbo_data_table" "initial_full_table_complete"]))))
    ;; Check that final state does not have full-table keys
    (is (let [last-state (as-> (catalog/discover test-db-config)
                             x
                             (select-stream x "log_based_sync_test_dbo_data_table" "LOG_BASED")
                             (get-messages-from-output x test-db-config nil)
                             (filter #(= "STATE" (% "type")) x)
                             (last x))]
          (= {} (select-keys (last-state "value") ["max_pk_values" "last_pk_fetched"]))))
    ;; Check that final state has full table complete
    (is (= true
           (let [last-state (as-> (catalog/discover test-db-config)
                                x
                                (select-stream x "log_based_sync_test_dbo_data_table" "LOG_BASED")
                                (get-messages-from-output x test-db-config nil)
                                (filter #(= "STATE" (% "type")) x)
                                (last x))]
             (get-in last-state ["value" "bookmarks" "log_based_sync_test_dbo_data_table" "initial_full_table_complete"]))))
    ;; Verify qualities of an initial full table sync
    ;; Copied from sync_full_table_test.clj
    (is (= "log_based_sync_test_dbo_data_table"
           ((-> (catalog/discover test-db-config)
                (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                (get-messages-from-output test-db-config "log_based_sync_test_dbo_data_table")
                first)
            "stream")))
    (is (= ["id"]
           ((-> (catalog/discover test-db-config)
                (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                (get-messages-from-output test-db-config "log_based_sync_test_dbo_data_table")
                first)
            "key_properties")))
    (is (= {"type" ["string"]
            "pattern" "[A-F0-9]{8}-([A-F0-9]{4}-){3}[A-F0-9]{12}"}
           (get-in (-> (catalog/discover test-db-config)
                       (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                       (get-messages-from-output test-db-config "log_based_sync_test_dbo_data_table")
                       first)
                   ["schema" "properties" "id"])))
    (is (not (contains? ((-> (catalog/discover test-db-config)
                             (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                             (get-messages-from-output test-db-config "log_based_sync_test_dbo_data_table")
                             first)
                         "schema")
                        "metadata")))
    ;; Emits the records expected
    (is (= 100
           (-> (catalog/discover test-db-config)
               (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
               (get-messages-from-output test-db-config nil)
               ((partial filter #(= "RECORD" (% "type"))))
               count)))
    (is (every? (fn [rec]
                  (= "log_based_sync_test_dbo_data_table" (rec "stream")))
                (-> (catalog/discover test-db-config)
                    (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                    (get-messages-from-output test-db-config nil))))
    ;; At the moment we're not ordering by anything so checking the actual
    ;; value here would be brittle, I think.
    (is (every? #(get-in % ["record" "value"])
                (as-> (catalog/discover test-db-config)
                    x
                    (select-stream x "log_based_sync_test_dbo_data_table" "LOG_BASED")
                    (get-messages-from-output x test-db-config nil)
                    (filter #(= "RECORD" (% "type")) x))))
    (is (= "STATE"
           ((-> (catalog/discover test-db-config)
                (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                (get-messages-from-output test-db-config nil)
                last)
            "type"))
        "Last message in a complete sync must be state")
    ))



(deftest ^:integration verify-log-based-replication-inserts
  (with-matrix-assertions test-db-configs test-db-fixture
    (insert-data test-db-config "dbo")
    (is (= 100
           (let [test-state {"bookmarks"
                             {"log_based_sync_test_dbo_data_table"
                              {"version" 1560965962084
                               "initial_full_table_complete" true
                               "current_log_version" 0}}}]
             (-> (catalog/discover test-db-config)
                 (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                 (get-messages-from-output test-db-config nil test-state)
                 ((partial filter #(= "RECORD" (% "type"))))
                 count))))
    ;; Verify that they are all 100->199 value
    (is (every? (set (range 100 200))
                (let [test-state {"bookmarks"
                                  {"log_based_sync_test_dbo_data_table"
                                   {"version" 1560965962084
                                    "initial_full_table_complete" true
                                    "current_log_version" 0}}}]
                  (-> (catalog/discover test-db-config)
                      (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                      (get-messages-from-output test-db-config nil test-state)
                      ((partial filter #(= "RECORD" (% "type"))))
                      ((partial map #(get-in % ["record" "value"])))))))
    ;; Verify current_log_version in state after sync
    (let [test-state {"bookmarks"
                      {"log_based_sync_test_dbo_data_table"
                       {"version" 1560965962084
                        "initial_full_table_complete" true
                        "current_log_version" 0}}}
          end-state (-> (catalog/discover test-db-config)
                        (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                        (get-messages-from-output test-db-config nil test-state)
                        ((partial filter #(= "STATE" (% "type"))))
                        last)]
      (is (= 1
             (get-in end-state ["value"
                                "bookmarks"
                                "log_based_sync_test_dbo_data_table"
                                "current_log_version"])))
      ;; The end-state's version needs to stay the same
      (is (= 1560965962084
             (get-in end-state ["value"
                                "bookmarks"
                                "log_based_sync_test_dbo_data_table"
                                "version"]))))))

(deftest ^:integration verify-log-based-replication-updates
  (with-matrix-assertions test-db-configs test-db-fixture
    (update-data test-db-config "dbo")
    ;; schema, state, activate_version
    (is (= "ACTIVATE_VERSION"
           (let [test-state {"bookmarks"
                             {"log_based_sync_test_dbo_data_table"
                              {"version" 1560965962084
                               "initial_full_table_complete" true
                               "current_log_version" 0}}}]
             (-> (catalog/discover test-db-config)
                 (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                 (get-messages-from-output test-db-config nil test-state)
                 (nth 2)
                 (get "type")))))
    (is (= 10
           (let [test-state {"bookmarks"
                             {"log_based_sync_test_dbo_data_table"
                              {"version" 1560965962084
                               "initial_full_table_complete" true
                               "current_log_version" 0}}}]
             (-> (catalog/discover test-db-config)
                 (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                 (get-messages-from-output test-db-config nil test-state)
                 ((partial filter #(= "RECORD" (% "type"))))
                 count))))
    ;; Verify that they are all 91->100 value (since we incremented 90-99)
    (is (every? (set (range 91 101))
                (let [test-state {"bookmarks"
                                  {"log_based_sync_test_dbo_data_table"
                                   {"version" 1560965962084
                                    "initial_full_table_complete" true
                                    "current_log_version" 0}}}]
                  (-> (catalog/discover test-db-config)
                      (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                      (get-messages-from-output test-db-config nil test-state)
                      ((partial filter #(= "RECORD" (% "type"))))
                      ((partial map #(get-in % ["record" "value"])))))))
    ;; Verify current_log_version in state after sync
    (is (= 1
           (let [test-state {"bookmarks"
                             {"log_based_sync_test_dbo_data_table"
                              {"version" 1560965962084
                               "initial_full_table_complete" true
                               "current_log_version" 0}}}]
             (-> (catalog/discover test-db-config)
                 (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                 (get-messages-from-output test-db-config nil test-state)
                 ((partial filter #(= "STATE" (% "type"))))
                 last
                 (get-in ["value"
                          "bookmarks"
                          "log_based_sync_test_dbo_data_table"
                          "current_log_version"])))))
    ))

(deftest ^:integration verify-log-based-replication-deletes
  (with-matrix-assertions test-db-configs test-db-fixture
    (delete-data test-db-config "dbo")
    (is (= 10
           (let [test-state {"bookmarks"
                             {"log_based_sync_test_dbo_data_table"
                              {"version" 1560965962084
                               "initial_full_table_complete" true
                               "current_log_version" 0}}}]
             (-> (catalog/discover test-db-config)
                (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                (get-messages-from-output test-db-config nil test-state)
                ((partial filter #(= "RECORD" (% "type"))))
                count))))
    ;; Verify current_log_version in state after sync
    (is (= 1
           (let [test-state {"bookmarks"
                             {"log_based_sync_test_dbo_data_table"
                              {"version" 1560965962084
                               "initial_full_table_complete" true
                               "current_log_version" 0}}}]
             (-> (catalog/discover test-db-config)
                (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                (get-messages-from-output test-db-config nil test-state)
                ((partial filter #(= "STATE" (% "type"))))
                last
                (get-in ["value"
                         "bookmarks"
                         "log_based_sync_test_dbo_data_table"
                         "current_log_version"])))))
    ))

(deftest ^:integration verify-min-valid-version-out-of-date?
  (with-matrix-assertions test-db-configs test-db-fixture
    (let [test-state {"bookmarks" {"log_based_sync_test_dbo_data_table"{"version" 1560965962084
                                                                        "initial_full_table_complete" true}}}
          catalog (catalog/discover test-db-config)
          stream-name "log_based_sync_test_dbo_data_table"]
      (is (= true
             (logical/min-valid-version-out-of-date? test-db-config catalog stream-name test-state))
          "Missing current_log_version means we never synced using change tracking")
      (is (= false
             (logical/min-valid-version-out-of-date? test-db-config catalog stream-name (assoc-in test-state ["bookmarks" stream-name "current_log_version"] 0)))
          "CHANGE_TRACKING_MIN_VALID_VERSION function returned a value greater than current_log_version"))))

(deftest ^:integration verify-min-valid-version-behavior
  (with-matrix-assertions test-db-configs test-db-fixture
    (let [test-state {"bookmarks" {"log_based_sync_test_dbo_data_table"{"version" 1560965962084
                                                                        "initial_full_table_complete" true
                                                                        "current_log_version" 0}}}
          messages (with-redefs [logical/min-valid-version-out-of-date? (constantly true)]
                     (-> (catalog/discover test-db-config)
                         (select-stream "log_based_sync_test_dbo_data_table" "LOG_BASED")
                         (get-messages-from-output test-db-config nil test-state)))]
      ;; Assert that records came out
      (is (= 100
             (->> messages
                  (filter #(= "RECORD" (% "type")))
                  count)))

      ;; Assert the state looks like we think it should
      (is (= {"bookmarks"
              {"log_based_sync_test_dbo_data_table"
               {"version" 1560965962084,
                "initial_full_table_complete" true,
                "current_log_version" 0}}}
             (get (->> messages
                       (filter #(= "STATE" (% "type")))
                       last) "value"))))))

(deftest ^:integration test-null-commit_time
  "instant1.compareTo(instant2) returns
     - a negative number if instant1 < instant2
     - a zero if instant1 = instant2
     - a positive number if instant1 > instant2"
  (let [our-time (java.time.Instant/parse "2000-01-30T15:37:20.895Z")
        our-str-time (.toString our-time)]
    (is (neg? (.compareTo our-time
                          (java.time.Instant/parse (logical/get-commit-time {"commit_time" nil})))))
    (is (neg? (.compareTo our-time
                          (java.time.Instant/parse (logical/get-commit-time {})))))
    (is (= 0
           (.compareTo our-time
                       (java.time.Instant/parse (logical/get-commit-time {"commit_time" our-str-time})))))))
