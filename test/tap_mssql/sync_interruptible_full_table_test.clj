(ns tap-mssql.sync-interruptible-full-table-test
  (:require [clojure.data.generators :as generators]
            [tap-mssql.catalog :as catalog]
            [tap-mssql.config :as config]
            [clojure.test :refer [is deftest]]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as string]
            [tap-mssql.core :refer :all]
            [tap-mssql.sync-strategies.full :as sync]
            [tap-mssql.singer.transform :as transform]
            [tap-mssql.singer.messages :as singer-messages]
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
    (jdbc/db-do-commands db-spec ["CREATE DATABASE full_table_interruptible_sync_test"])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_interruptible_sync_test")
                         [(jdbc/create-table-ddl
                           "data_table"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]
                            [:deselected_value "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_interruptible_sync_test")
                         [(jdbc/create-table-ddl
                           "data_table_rowversion"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]
                            [:rowversion "rowversion"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_interruptible_sync_test")
                         [(jdbc/create-table-ddl
                           "table_with_unsupported_pk"
                           [[:id "smallmoney NOT NULL PRIMARY KEY"]
                            [:value "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_interruptible_sync_test")
                         [(jdbc/create-table-ddl
                           "table_with_unsupported_column"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "xml"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_interruptible_sync_test")
                         [(jdbc/create-table-ddl
                           "table_with_composite_pks"
                           [[:id "int NOT NULL"]
                            [:number "int NOT NULL"]
                            [:datetime "datetime2 NOT NULL"]
                            [:value "varchar(5000)"]
                            ["PRIMARY KEY (id, number, datetime)"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_interruptible_sync_test")
                         [(jdbc/create-table-ddl
                           "table_with_timestamp_bookmark_key"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:number "int NOT NULL"]
                            [:timestamp "timestamp NOT NULL"]
                            [:value "varchar(5000)"]])])
   (jdbc/db-do-commands (assoc db-spec :dbname "full_table_interruptible_sync_test")
                         ["CREATE VIEW view_with_table_ids
                           AS
                           SELECT id FROM data_table"])))

(defn populate-data
  [config]
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "full_table_interruptible_sync_test"))
                      "data_table"
                      (take 200 (map (partial hash-map :deselected_value nil :value) (range))))
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "full_table_interruptible_sync_test"))
                      "data_table_rowversion"
                      (take 200 (map (partial hash-map :value) (range))))
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "full_table_interruptible_sync_test"))
                      "table_with_unsupported_pk"
                      (take 200 (map #(hash-map :id (+ % 0.05) :value %) (range))))
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "full_table_interruptible_sync_test"))
                      "table_with_unsupported_column"
                      (take 200 (map #(hash-map :value (str "<xmlbody>" % "</xmlbody>")) (range))))
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "full_table_interruptible_sync_test"))
                      "table_with_composite_pks"
                      (take 2000 (map #(hash-map :id (rand-int 1000000)
                                                 :number (rand-int 1000000)
                                                 :datetime (-> (generators/date)
                                                               .toInstant
                                                               .toString)
                                                 :value (str %)) (range))))
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "full_table_interruptible_sync_test"))
                      "table_with_timestamp_bookmark_key"
                      (take 2000 (map #(hash-map :number (rand-int 1000000)
                                                 :value (str %)) (range)))))

(defn test-db-fixture [f config]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db config)
    (create-test-db config)
    (populate-data config)
    ;; TODO: This atom is used by write-state-buffered! and this should be fixed in a better way
    (reset! singer-messages/records-since-last-state 0)
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
      (assoc-in ["streams" stream-name "metadata" "replication-method"] "FULL_TABLE")))

(defn add-view-key-properties
  [stream-name column-name catalog]
  (assoc-in catalog ["streams" stream-name "metadata" "view-key-properties"] [column-name]))

(defn deselect-field
  [stream-name field-name catalog]
  (assoc-in catalog ["streams" stream-name "metadata" "properties" field-name "selected"] false))

(def record-count (atom 0))

(deftest ^:integration verify-unsupported-column-has-empty-schema
  (with-matrix-assertions test-db-configs test-db-fixture
    (let [test-db-config (assoc test-db-config "include_schemas_in_destination_stream_name" "true")
          _ (set-include-db-and-schema-names-in-messages! test-db-config)]
      (is (= {}
             (get-in (first (filter #(= "SCHEMA" (% "type"))
                                    (->> (catalog/discover test-db-config)
                                         (select-stream "full_table_interruptible_sync_test_dbo_table_with_unsupported_column")
                                         (get-messages-from-output test-db-config
                                                                   "full_table_interruptible_sync_test_dbo_table_with_unsupported_column"))))
                     ["schema" "properties" "value"]))))))

(deftest ^:integration verify-unsupported-primary-key-throws
  (with-matrix-assertions test-db-configs test-db-fixture
    (let [test-db-config (assoc test-db-config "include_schemas_in_destination_stream_name" "true")
          _ (set-include-db-and-schema-names-in-messages! test-db-config)]
      (is (thrown-with-msg? java.lang.Exception
                                  #"has unsupported primary key"
                                  (->> test-db-config
                                       (catalog/discover)
                                       ((fn [catalog] ;; Fake the pk being unsupported
                                          (-> catalog (assoc-in ["streams"
                                                                 "full_table_interruptible_sync_test_dbo_table_with_unsupported_pk"
                                                                 "metadata"
                                                                 "properties"
                                                                 "id"
                                                                 "inclusion"]
                                                                "unsupported")
                                              (assoc-in  ["streams"
                                                          "full_table_interruptible_sync_test_dbo_table_with_unsupported_pk"
                                                          "metadata"
                                                          "properties"
                                                          "id"
                                                          "selected-by-default"]
                                                         false))))
                                       (select-stream "full_table_interruptible_sync_test_dbo_table_with_unsupported_pk")
                                       (get-messages-from-output test-db-config "full_table_interruptible_sync_test_dbo_table_with_unsupported_pk")))))))

(deftest ^:integration verify-full-table-sync-with-rowversion-resumes-on-interruption
  (with-matrix-assertions test-db-configs test-db-fixture
    ;; Steps:
    ;; Sync partially, a table with row version, interrupted at some point
    ;;     -- e.g., (with-redefs [valid-message? (fn [msg] (if (some-atom-thing-changes-after x calls) (throw...) (valid-message? msg)))] ... )
    (let [test-db-config (assoc test-db-config "include_schemas_in_destination_stream_name" "true")
          _ (set-include-db-and-schema-names-in-messages! test-db-config)
          old-write-record singer-messages/write-record!]
      (with-redefs [singer-messages/write-record! (fn [stream-name state record catalog]
                                                    (swap! record-count inc)
                                                    (if (> @record-count 120)
                                                      (do
                                                        (reset! record-count 0)
                                                        (throw (ex-info "Interrupting!" {:ignore true})))
                                                      (old-write-record stream-name state record catalog)))]
        (let [first-messages (->> (catalog/discover test-db-config)
                                  (select-stream "full_table_interruptible_sync_test_dbo_data_table_rowversion")
                                  (get-messages-from-output test-db-config
                                                            "full_table_interruptible_sync_test_dbo_data_table_rowversion"))
              first-state (last first-messages)]
          (is (valid-state? first-state))
          (is (= "SCHEMA"
                 ((-> first-messages
                      first)
                  "type")))
          ;; Strictly increasing rowversion
          (is (every? (comp (partial > 0)
                            (partial apply compare))
                      (->> first-messages
                           (filter #(= "RECORD" (% "type")))
                           (map #(get-in % ["record" "rowversion"]))
                           (partition 2 1))))
          ;; Format of transformed rowversion field
          (is (every? (partial re-matches #"0x[0-9A-F]{16}")
                      (->> first-messages
                           (filter #(= "RECORD" (% "type")))
                           (map #(get-in % ["record" "rowversion"])))))
          ;; Next state emitted has the pk and version of the last record emitted before that state
          (let [[last-record last-state] (->> first-messages
                                              (drop 5) ;; Ignore first state
                                              (partition 2 1)
                                              (drop-while (fn [[a b]] (not= "STATE" (b "type"))))
                                              first)]
            (is (= (get-in last-record ["record" "rowversion"])
                   (transform/transform-binary (get-in last-state ["value" "bookmarks" "full_table_interruptible_sync_test_dbo_data_table_rowversion" "last_pk_fetched" "rowversion"]))))
            (is (= (last-record "version") (get-in last-state ["value" "bookmarks" "full_table_interruptible_sync_test_dbo_data_table_rowversion" "version"]))))
          ;; Activate Version on first sync
          (is (= "ACTIVATE_VERSION"
                 ((->> first-messages
                       (second)) "type")))
          ;;   - Record count (Equal to the counter of the atom before exception)
          (is (= 120
                 (count (filter #(= "RECORD" (% "type")) first-messages)))))
        ))
    ))

(deftest ^:integration verify-full-table-interruptible-bookmark-clause
  (with-matrix-assertions test-db-configs test-db-fixture
    (let [test-db-config (assoc test-db-config "include_schemas_in_destination_stream_name" "true")
          _ (set-include-db-and-schema-names-in-messages! test-db-config)
          stream-name "schema_name_table_name"
          schema-name "schema_name"
          table-name "table_name"
          record-keys ["id" "number" "datetime" "value"]]
      (is (= (sync/build-sync-query stream-name schema-name table-name record-keys
                                    {"bookmarks"
                                     {"schema_name_table_name"
                                      {"version" 1570539559650
                                       "max_pk_values" {"id" 999999
                                                        "number" 999999
                                                        "datetime" "2018-10-08T00:00:00.000Z"}
                                       "last_pk_fetched" {"id" 1
                                                          "number" 1
                                                          "datetime" "2000-01-01T00:00:00.000Z"}
                                       }}})
             '("SELECT [id], [number], [datetime], [value] FROM [schema_name].[table_name] WHERE (([id] > ?) OR ([id] = ? AND [number] > ?) OR ([id] = ? AND [number] = ? AND [datetime] > ?)) AND [id] <= ? AND [number] <= ? AND [datetime] <= ? ORDER BY [id], [number], [datetime]"
               1 1
               1 1 1 "2000-01-01T00:00:00.000Z"
               999999
               999999 "2018-10-08T00:00:00.000Z")))
      (is (= (sync/build-sync-query stream-name
                                    schema-name
                                    table-name
                                    record-keys
                                    {"bookmarks"
                                     {"schema_name_table_name"
                                      {"version" 1570539559650
                                       "max_pk_values" {"id" 999999
                                                        "number" 999999}
                                       "last_pk_fetched" {"id" 1 "number" 1}
                                       }}})
             '("SELECT [id], [number], [datetime], [value] FROM [schema_name].[table_name] WHERE (([id] > ?) OR ([id] = ? AND [number] > ?)) AND [id] <= ? AND [number] <= ? ORDER BY [id], [number]"
               1 1 1  999999 999999)))
      (is (= (sync/build-sync-query stream-name
                                    schema-name
                                    table-name
                                    record-keys
                                    {"bookmarks"
                                     {"schema_name_table_name"
                                      {"version" 1570539559650
                                       "max_pk_values" {"id" 999999}
                                       "last_pk_fetched" {"id" 1}
                                       }}})
             '("SELECT [id], [number], [datetime], [value] FROM [schema_name].[table_name] WHERE (([id] > ?)) AND [id] <= ? ORDER BY [id]"
               1 999999)))
      (is (= (sync/build-sync-query stream-name
                                    schema-name
                                    table-name
                                    record-keys
                                    {"bookmarks"
                                     {"schema_name_table_name"
                                      {"version" 1570539559650
                                       "max_pk_values" {}
                                       "last_pk_fetched" {}
                                       }}})
             '("SELECT [id], [number], [datetime], [value] FROM [schema_name].[table_name]")))
      )))


(deftest ^:integration verify-full-table-sync-with-composite-pk-resumes-on-interruption
  (with-matrix-assertions test-db-configs test-db-fixture
    ;; Steps:
    ;; 1. Sync the full table and make sure it returns all the records.
    (let [test-db-config (assoc test-db-config "include_schemas_in_destination_stream_name" "true")
          _ (set-include-db-and-schema-names-in-messages! test-db-config)
          first-messages (->> (catalog/discover test-db-config)
                              (select-stream "full_table_interruptible_sync_test_dbo_table_with_composite_pks")
                              (get-messages-from-output test-db-config
                                                        "full_table_interruptible_sync_test_dbo_table_with_composite_pks"
                                                        {}))
          first-state (last first-messages)
          largest-message (map #())]
      (is (= 2000 (->> first-messages
                       (filter #(= "RECORD" (% "type")))
                       count))))
    ;; Steps:
    ;; 2. Using the same data, sync 1000 rows, capture state, and sync the remaining. Assert that both syncs total 2000 records.
    (let [old-write-record singer-messages/write-record!
          first-messages (with-redefs [singer-messages/write-record! (fn [stream-name state record catalog]
                                                                       (swap! record-count inc)
                                                                       (if (> @record-count 1000)
                                                                         (do
                                                                           (reset! record-count 0)
                                                                           (throw (ex-info "Interrupting!" {:ignore true})))
                                                                         (old-write-record stream-name state record catalog)))]
                           (->> (catalog/discover test-db-config)
                                (select-stream "full_table_interruptible_sync_test_dbo_table_with_composite_pks")
                                (get-messages-from-output test-db-config
                                                          "full_table_interruptible_sync_test_dbo_table_with_composite_pks")))
          first-state (get (->> first-messages
                                (filter #(= "STATE" (% "type")))
                                last)
                           "value")
          second-messages (->> (catalog/discover test-db-config)
                               (select-stream "full_table_interruptible_sync_test_dbo_table_with_composite_pks")
                               (get-messages-from-output test-db-config
                                                         "full_table_interruptible_sync_test_dbo_table_with_composite_pks"
                                                         first-state))]
      ;; Count unique records
      (is (= 2000 (count  (reduce
                           (fn [acc rec]
                             (conj acc (str (get-in rec ["record" "id"])
                                            (get-in rec ["record" "number"])
                                            (get-in rec ["record" "datetime"]))))
                           #{}
                           (concat
                            (->> second-messages
                                 (filter #(= "RECORD" (% "type"))))
                            (->> first-messages
                                 (filter #(= "RECORD" (% "type")))))))))
      ;; Make sure last state has no last_pk_fetched or max_pk_value bookmarks, indicating complete full table
      (is (= "STATE" (get (last second-messages) "type")))
      (is (nil?
           (get-in (last second-messages) ["value" "bookmarks" "full_table_interruptible_sync_test_dbo_table_with_composite_pks" "last_pk_fetched"])))
      (is (nil?
           (get-in (last second-messages) ["value" "bookmarks" "full_table_interruptible_sync_test_dbo_table_with_composite_pks" "max_pk_value"]))))))

(deftest ^:integration verify-interrupted-full-table-sync-with-timestamp-pk-succeeds
  (with-matrix-assertions test-db-configs test-db-fixture
    ;; Steps:
    ;; 1. Sync 1000 rows, capture state, and sync the remaining. A timestamp should be used in the WHERE clause of the query
    (let [test-db-config (assoc test-db-config "include_schemas_in_destination_stream_name" "true")
          _ (set-include-db-and-schema-names-in-messages! test-db-config)
          old-write-record singer-messages/write-record!
          first-messages (with-redefs [singer-messages/write-record! (fn [stream-name state record catalog]
                                                                       (swap! record-count inc)
                                                                       (if (> @record-count 1000)
                                                                         (do
                                                                           (reset! record-count 0)
                                                                           (throw (ex-info "Interrupting!" {:ignore true})))
                                                                         (old-write-record stream-name state record catalog)))]
                           (->> (catalog/discover test-db-config)
                                (select-stream "full_table_interruptible_sync_test_dbo_table_with_timestamp_bookmark_key")
                                (get-messages-from-output test-db-config
                                                          "full_table_interruptible_sync_test_dbo_table_with_timestamp_bookmark_key")))
          first-state (get (->> first-messages
                                (filter #(= "STATE" (% "type")))
                                last)
                           "value")
          second-messages (->> (catalog/discover test-db-config)
                               (select-stream "full_table_interruptible_sync_test_dbo_table_with_timestamp_bookmark_key")
                               (get-messages-from-output test-db-config
                                                         "full_table_interruptible_sync_test_dbo_table_with_timestamp_bookmark_key"
                                                         first-state))]

      (is (= 2000 (count  (reduce
                           (fn [acc rec]
                             (conj acc (str (get-in rec ["record" "id"]))))
                           #{}
                           (concat
                            (->> second-messages
                                 (filter #(= "RECORD" (% "type"))))
                            (->> first-messages
                                 (filter #(= "RECORD" (% "type")))))))))

      ;; Make sure last state has no last_pk_fetched or max_pk_value bookmarks, indicating complete full table
      (is (= "STATE" (get (last second-messages) "type")))
      (is (nil?
           (get-in (last second-messages) ["value" "bookmarks" "full_table_interruptible_sync_test_dbo_table_with_timestamp_bookmark_key" "last_pk_fetched"])))
      (is (nil?
           (get-in (last second-messages) ["value" "bookmarks" "full_table_interruptible_sync_test_dbo_table_with_timestamp_bookmark_key" "max_pk_value"]))))))

(deftest ^:integration verify-full-table-view-with-view-key-properties-is-interruptible
  (with-matrix-assertions test-db-configs test-db-fixture
    ;; Steps:
    ;; 1. Sync 150 rows, capture state, and sync the remaining
    (let [test-db-config (assoc test-db-config "include_schemas_in_destination_stream_name" "true")
          _ (set-include-db-and-schema-names-in-messages! test-db-config)
          table-name "full_table_interruptible_sync_test_dbo_view_with_table_ids"
          old-write-record singer-messages/write-record!
          first-messages (with-redefs [singer-messages/write-record! (fn [stream-name state record catalog]
                                                                       (swap! record-count inc)
                                                                       (if (> @record-count 150)
                                                                         (do
                                                                           (reset! record-count 0)
                                                                           (throw (ex-info "Interrupting!" {:ignore true})))
                                                                         (old-write-record stream-name state record catalog)))]
                           (->> (catalog/discover test-db-config)
                                (select-stream table-name)
                                (add-view-key-properties table-name "id")
                                (get-messages-from-output test-db-config
                                                          table-name)))
          first-state (get (->> first-messages
                                (filter #(= "STATE" (% "type")))
                                last)
                           "value")
          ;; Run the 2nd sync which resumes from the first
          second-messages (->> (catalog/discover test-db-config)
                               (select-stream table-name)
                               (add-view-key-properties table-name "id")
                               (get-messages-from-output test-db-config
                                                         table-name
                                                         first-state))]

      (is (= 200 (count  (reduce
                          (fn [acc rec]
                            (conj acc (str (get-in rec ["record" "id"]))))
                          #{}
                          (concat
                           (->> second-messages
                                (filter #(= "RECORD" (% "type"))))
                           (->> first-messages
                                (filter #(= "RECORD" (% "type")))))))))

      ;; Make sure last state has no last_pk_fetched or max_pk_value bookmarks, indicating complete full table
      (is (= "STATE" (get (last second-messages) "type")))
      (is (nil?
           (get-in (last second-messages) ["value" "bookmarks" table-name "last_pk_fetched"])))
      (is (nil?
           (get-in (last second-messages) ["value" "bookmarks" table-name "max_pk_value"]))))))

(deftest ^:integration verify-full-table-view-without-view-key-properties-leaves-valid-state
  (with-matrix-assertions test-db-configs test-db-fixture
    ;; Steps:
    ;; 1. Sync 150 rows, capture state, and sync the remaining
    (let [test-db-config (assoc test-db-config "include_schemas_in_destination_stream_name" "true")
          _ (set-include-db-and-schema-names-in-messages! test-db-config)
          table-name "full_table_interruptible_sync_test_dbo_view_with_table_ids"
          old-write-record singer-messages/write-record!
          first-messages (with-redefs [singer-messages/write-record! (fn [stream-name state record catalog]
                                                                       (swap! record-count inc)
                                                                       (if (> @record-count 150)
                                                                         (do
                                                                           (reset! record-count 0)
                                                                           (throw (ex-info "Interrupting!" {:ignore true})))
                                                                         (old-write-record stream-name state record catalog)))]
                           ;; No view-key-properties added
                           (->> (catalog/discover test-db-config)
                                (select-stream table-name)
                                (get-messages-from-output test-db-config
                                                          table-name)))
          first-state (get (->> first-messages
                                (filter #(= "STATE" (% "type")))
                                last)
                           "value")]
      ;; Make sure the interrupted state is valid
      (is (= true (sync/valid-full-table-state? first-state table-name))))))
