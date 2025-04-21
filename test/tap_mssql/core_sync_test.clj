(ns tap-mssql.core-sync-test
  (:require [tap-mssql.config :as config]
            [clojure.test :refer [is deftest]]
            [clojure.java.jdbc :as jdbc]
            [tap-mssql.test-utils :refer [with-out-and-err-to-dev-null
                                          test-db-config
                                          test-db-configs
                                          with-matrix-assertions]]
            [tap-mssql.core :refer :all]
            [tap-mssql.sync-strategies.full :as full]
            [tap-mssql.sync-strategies.logical :as logical]
            [tap-mssql.catalog :as catalog]
            [tap-mssql.singer.transform :as singer-transform]
            [tap-mssql.singer.messages :as singer-messages]
            [tap-mssql.config :as config])
  (:import [java.sql Date]))

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
                           "basic_table"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_sync_test")
                         [(jdbc/create-table-ddl
                           "composite_key_table"
                           [[:id "uniqueidentifier NOT NULL DEFAULT NEWID()"]
                            [:second_id "int NOT NULL"]
                            [:value "int"]
                            ["primary key (id, second_id)"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_sync_test")
                         [(jdbc/create-table-ddl
                           "no_pk_table"
                           [[:value "int"]])])))

(defn test-db-fixture [f config]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db config)
    (create-test-db config)
    (f)))

(deftest build-sync-query-test
  (is (thrown? AssertionError
               (full/build-sync-query "craftsmanship_dbo_mahogany" "dbo" "mahogany" [] {})))
  ;; No bookmark, no pk = Full Table sync query
  (is (= ["SELECT [legs], [tabletop], [leaf] FROM [dbo].[mahogany]"]
         (full/build-sync-query "craftsmanship_dbo_mahogany" "dbo" "mahogany" ["legs", "tabletop", "leaf"] {})))
  ;; No bookmark, yes pk = First FT Interruptible query
  (is (= '("SELECT [legs], [tabletop], [leaf] FROM [dbo].[mahogany] WHERE [legs] <= ? AND [leaf] <= ? ORDER BY [legs], [leaf]"
           4
           "birch")
         (full/build-sync-query "craftsmanship_dbo_mahogany" "dbo" "mahogany" ["legs", "tabletop", "leaf"]
                                {"bookmarks" {"craftsmanship_dbo_mahogany" {"max_pk_values" {"legs" 4 "leaf" "birch"}}}})))
  ;; Bookmark, no pk = ??? Invalid state
  (is (thrown? AssertionError
               (full/build-sync-query "craftsmanship_dbo_mahogany" "dbo" "mahogany" ["legs", "tabletop", "leaf"]
                                      {"bookmarks" {"craftsmanship_dbo_mahogany" {"last_pk_fetched" {"legs" 2 "leaf" "balsa"}}}})))

  ;; Max-pk-value is _actually_ null (e.g., empty table)
  (is (= '("SELECT [legs], [tabletop], [leaf] FROM [dbo].[mahogany] ORDER BY [legs]")
         (full/build-sync-query "craftsmanship_dbo_mahogany" "dbo" "mahogany" ["legs", "tabletop", "leaf"]
                                {"bookmarks"
                                 {"craftsmanship_dbo_mahogany"
                                  {"max_pk_values" {"legs" nil}}}}))))

(deftest ^:integration build-log-based-sql-query-test
  (with-matrix-assertions test-db-configs test-db-fixture
    ;; No PK
    (is (thrown? AssertionError (logical/build-log-based-sql-query
                                 (catalog/discover test-db-config)
                                 "full_table_sync_test_dbo_no_pk_table"
                                 {} )))
    ;; No current_log_version bookmark
    (is (thrown? AssertionError (logical/build-log-based-sql-query
                                 (catalog/discover test-db-config)
                                 "full_table_sync_test_dbo_basic_table"
                                 {} )))
    ;; Has primary key, no record Keys, no primary key bookmark
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.[id], [dbo].[basic_table].[id] FROM CHANGETABLE (CHANGES [dbo].[basic_table], 0) as c LEFT JOIN [dbo].[basic_table] ON c.[id]=[dbo].[basic_table].[id] LEFT JOIN sys.dm_tran_commit_table tc on c.SYS_CHANGE_VERSION = tc.commit_ts ORDER BY c.SYS_CHANGE_VERSION, c.[id]"]
         (logical/build-log-based-sql-query
          (update-in (catalog/discover test-db-config)
                     ["streams" "full_table_sync_test_dbo_basic_table" "metadata" "properties" "value"]
                     assoc
                     "selected" false)
          "full_table_sync_test_dbo_basic_table"
          {"bookmarks" {"full_table_sync_test_dbo_basic_table" {"current_log_version" 0}}})))
    ;; Has PK, No Selected Fields, Has Bookmark
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.[id], [dbo].[basic_table].[id] FROM CHANGETABLE (CHANGES [dbo].[basic_table], 0) as c LEFT JOIN [dbo].[basic_table] ON c.[id]=[dbo].[basic_table].[id] LEFT JOIN sys.dm_tran_commit_table tc on c.SYS_CHANGE_VERSION = tc.commit_ts WHERE c.SYS_CHANGE_VERSION = 0 AND c.id >= ? ORDER BY c.SYS_CHANGE_VERSION, c.[id]" "foo"]
         (logical/build-log-based-sql-query
          (update-in (catalog/discover test-db-config)
                     ["streams" "full_table_sync_test_dbo_basic_table" "metadata" "properties" "value"]
                     assoc
                     "selected" false)
          "full_table_sync_test_dbo_basic_table"
          {"bookmarks"
           {"full_table_sync_test_dbo_basic_table"
            {"current_log_version" 0
             "last_pk_fetched"     {"id" "foo"}}}})))
    ;; Has primary key, selected fields, no primary key bookmark
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.[id], [dbo].[basic_table].[id], [dbo].[basic_table].[value] FROM CHANGETABLE (CHANGES [dbo].[basic_table], 0) as c LEFT JOIN [dbo].[basic_table] ON c.[id]=[dbo].[basic_table].[id] LEFT JOIN sys.dm_tran_commit_table tc on c.SYS_CHANGE_VERSION = tc.commit_ts ORDER BY c.SYS_CHANGE_VERSION, c.[id]"]
         (logical/build-log-based-sql-query
          (catalog/discover test-db-config)
          "full_table_sync_test_dbo_basic_table"
          {"bookmarks"
           {"full_table_sync_test_dbo_basic_table"
            {"current_log_version" 0}}})))

    ;; Has primary key, selected fields, primary key bookmark
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.[id], [dbo].[basic_table].[id], [dbo].[basic_table].[value] FROM CHANGETABLE (CHANGES [dbo].[basic_table], 0) as c LEFT JOIN [dbo].[basic_table] ON c.[id]=[dbo].[basic_table].[id] LEFT JOIN sys.dm_tran_commit_table tc on c.SYS_CHANGE_VERSION = tc.commit_ts WHERE c.SYS_CHANGE_VERSION = 0 AND c.id >= ? ORDER BY c.SYS_CHANGE_VERSION, c.[id]" "foo"]
         (logical/build-log-based-sql-query
          (catalog/discover test-db-config)
          "full_table_sync_test_dbo_basic_table"
          {"bookmarks"
           {"full_table_sync_test_dbo_basic_table"
            {"current_log_version" 0
             "last_pk_fetched"     {"id" "foo"}}}})))
    ;; Has composite primary keys, selected fields, bookmarks for both pks
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.[id], c.[second_id], [dbo].[composite_key_table].[id], [dbo].[composite_key_table].[second_id], [dbo].[composite_key_table].[value] FROM CHANGETABLE (CHANGES [dbo].[composite_key_table], 0) as c LEFT JOIN [dbo].[composite_key_table] ON c.[id]=[dbo].[composite_key_table].[id] AND c.[second_id]=[dbo].[composite_key_table].[second_id] LEFT JOIN sys.dm_tran_commit_table tc on c.SYS_CHANGE_VERSION = tc.commit_ts WHERE c.SYS_CHANGE_VERSION = 0 AND c.id >= ? AND c.second_id >= ? ORDER BY c.SYS_CHANGE_VERSION, c.[id], c.[second_id]" "foo" "bar"]
         (logical/build-log-based-sql-query
          (catalog/discover test-db-config)
          "full_table_sync_test_dbo_composite_key_table"
          {"bookmarks"
           {"full_table_sync_test_dbo_composite_key_table"
            {"current_log_version" 0
             "last_pk_fetched"     {"id"        "foo"
                                    "second_id" "bar"}}}})))
    )
  )

(deftest transform-test
  (is (= (singer-transform/transform
          {"streams"
           {"cuyahoga" {"metadata" {"properties" {"test1" {"sql-datatype" "varbinary"}
                                                  "test2" {"sql-datatype" "timestamp"}
                                                  "test3" {"sql-datatype" "date"}
                                                  "test4" {"sql-datatype" "binary"}
                                                  "regular" {"sql-datatype" "fish"}}}}}}
          "cuyahoga"
          {"test1" (byte-array [0 0 0 0 0 0 0 10])
           "test2" (byte-array [0 0 0 0 0 0 0 10])
           "test3" (Date. 1565222400000)
           "test4" (byte-array [0 0 0 0 0 0 0 10])
           "regular" {"should be" "unchanged"}})
         {"test1" "0x000000000000000A"
          "test2" "0x000000000000000A"
          "test3" "2019-08-08T00:00:00+00:00"
          "test4" "0x000000000000000A"
          "regular" {"should be" "unchanged"}})))

(deftest transform-binary-test
  ;; Convert to hex number (8 bytes)
  (is (= "0x000000000000000A"
         (singer-transform/transform-binary (byte-array [0 0 0 0 0 0 0 10])))))

(deftest transform-date-test
  ;; Convert to string and add time and tz offset (T00:00:00+00:00)
  (is (= "2019-08-08T00:00:00+00:00"
         (singer-transform/transform-date (Date. 1565222400000)))))

(deftest maybe-write-activate-version!-test
  ;; FULL_TABLE replication
  (let [catalog {"streams" {"jet_stream" {"tap_stream_id" "jet_stream" "table_name" "jet_stream"}}}]
    (is (= "{\"type\":\"ACTIVATE_VERSION\",\"stream\":\"jet_stream\",\"version\":1560363676948}\n"
           (with-redefs [singer-messages/now (constantly 1560363676948)]
             (with-out-str (singer-messages/maybe-write-activate-version! "jet_stream" "FULL_TABLE" catalog {}))))
        "Write activate version if no version has started loading yet and doing full table replication")
    (is (= ""
           (with-redefs [singer-messages/now (constantly 1560363676948)]
             (with-out-str (singer-messages/maybe-write-activate-version! "jet_stream" "FULL_TABLE" catalog {"bookmarks" {"jet_stream" {"version" 999}}}))))
        "Don't emit an activate_version message if a version exists in state and syncning full table replication")
    ;; Resuming an interrupted full table sync
    (is (= {"bookmarks" {"jet_stream" {"last_pk_fetched" 1 "version" 1560363676948}}}
           (with-out-and-err-to-dev-null
             (singer-messages/maybe-write-activate-version! "jet_stream" "FULL_TABLE" catalog {"bookmarks" {"jet_stream" {"last_pk_fetched" 1 "version" 1560363676948}}})))
        "Resuming an interrupted full table sync should keep the old state")

    ;; LOG_BASED
    (is (= "{\"type\":\"ACTIVATE_VERSION\",\"stream\":\"jet_stream\",\"version\":1560363676948}\n"
           (with-redefs [singer-messages/now (constantly 1560363676948)]
             (with-out-str (singer-messages/maybe-write-activate-version! "jet_stream" "LOG_BASED" catalog {}))))
        "Write activate version if no version has started loading yet and doing log based replication")

    ;; INCREMENTAL / LOG_BASED
    (is (= {"bookmarks" {"jet_stream" {"version" 1560363676948}}}
           (with-redefs [singer-messages/now (constantly 1560363676948)]
             (with-out-and-err-to-dev-null
               (singer-messages/maybe-write-activate-version! "jet_stream" "INCREMENTAL" catalog {}))))
        "Add a version into the state if none exists already and syncing incrementally")
    (is (= {"bookmarks" {"jet_stream" {"version" 1560363676948}}}
           (with-out-and-err-to-dev-null
             (singer-messages/maybe-write-activate-version! "jet_stream" "LOG_BASED" catalog {"bookmarks" {"jet_stream" {"version" 1560363676948}}})))
        "Keep the version in the state and syncing incrementally or log based")))

(deftest verify-full-table-interruptible-bookmark-clause
  (let [stream-name "schema_name_table_name"
        schema-name "schema_name"
        table-name "table_name"
        record-keys ["id" "number" "datetime" "value"]]
    (is (= '("SELECT [id], [number], [datetime], [value] FROM [schema_name].[table_name] WHERE (([id] > ?) OR ([id] = ? AND [number] > ?) OR ([id] = ? AND [number] = ? AND [datetime] > ?)) AND [id] <= ? AND [number] <= ? AND [datetime] <= ? ORDER BY [id], [number], [datetime]"
             1 1
             1 1 1 "2000-01-01T00:00:00.000Z"
             999999
             999999 "2018-10-08T00:00:00.000Z")
           (full/build-sync-query stream-name schema-name table-name record-keys
                                  {"bookmarks"
                                   {"schema_name_table_name"
                                    {"version" 1570539559650
                                     "max_pk_values" {"id" 999999
                                                      "number" 999999
                                                      "datetime" "2018-10-08T00:00:00.000Z"}
                                     "last_pk_fetched" {"id" 1
                                                        "number" 1
                                                        "datetime" "2000-01-01T00:00:00.000Z"}
                                     }}})))
    (is (= '("SELECT [id], [number], [datetime], [value] FROM [schema_name].[table_name] WHERE (([id] > ?) OR ([id] = ? AND [number] > ?)) AND [id] <= ? AND [number] <= ? ORDER BY [id], [number]"
             1 1 1  999999 999999)
           (full/build-sync-query stream-name
                                  schema-name
                                  table-name
                                  record-keys
                                  {"bookmarks"
                                   {"schema_name_table_name"
                                    {"version" 1570539559650
                                     "max_pk_values" {"id" 999999
                                                      "number" 999999}
                                     "last_pk_fetched" {"id" 1 "number" 1}
                                     }}})))
    (is (= '("SELECT [id], [number], [datetime], [value] FROM [schema_name].[table_name] WHERE (([id] > ?)) AND [id] <= ? ORDER BY [id]"
             1 999999)
           (full/build-sync-query stream-name
                                  schema-name
                                  table-name
                                  record-keys
                                  {"bookmarks"
                                   {"schema_name_table_name"
                                    {"version" 1570539559650
                                     "max_pk_values" {"id" 999999}
                                     "last_pk_fetched" {"id" 1}
                                     }}})))
    (is (= '("SELECT [id], [number], [datetime], [value] FROM [schema_name].[table_name]")
           (full/build-sync-query stream-name
                                  schema-name
                                  table-name
                                  record-keys
                                  {"bookmarks"
                                   {"schema_name_table_name"
                                    {"version" 1570539559650
                                     "max_pk_values" {}
                                     "last_pk_fetched" {}
                                     }}})))
    ))
