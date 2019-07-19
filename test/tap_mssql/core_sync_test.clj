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
            [tap-mssql.config :as config]))

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
               (full/build-sync-query "craftsmanship-dbo-mahogany" "dbo" "mahogany" [] {})))
  ;; No bookmark, no pk = Full Table sync query
  (is (= ["SELECT legs, tabletop, leaf FROM dbo.mahogany"]
         (full/build-sync-query "craftsmanship-dbo-mahogany" "dbo" "mahogany" ["legs", "tabletop", "leaf"] {})))
  ;; No bookmark, yes pk = First FT Interruptible query
  (is (= '("SELECT legs, tabletop, leaf FROM dbo.mahogany WHERE legs <= ? AND leaf <= ? ORDER BY legs, leaf"
          4
          "birch")
         (full/build-sync-query "craftsmanship-dbo-mahogany" "dbo" "mahogany" ["legs", "tabletop", "leaf"]
                           {"bookmarks" {"craftsmanship-dbo-mahogany" {"max_pk_values" {"legs" 4 "leaf" "birch"}}}})))
  ;; Bookmark, no pk = ??? Invalid state
  (is (thrown? AssertionError
               (full/build-sync-query "craftsmanship-dbo-mahogany" "dbo" "mahogany" ["legs", "tabletop", "leaf"]
                                 {"bookmarks" {"craftsmanship-dbo-mahogany" {"last_pk_fetched" {"legs" 2 "leaf" "balsa"}}}})))
  ;; Bookmark and PK = Resuming Full Table Sync
  (is (= '("SELECT legs, tabletop, leaf FROM dbo.mahogany WHERE legs >= ? AND leaf >= ? AND legs <= ? AND leaf <= ? ORDER BY legs, leaf"
          2
          "balsa"
          4
          "birch")
         (full/build-sync-query "craftsmanship-dbo-mahogany" "dbo" "mahogany" ["legs", "tabletop", "leaf"]
                           {"bookmarks"
                            {"craftsmanship-dbo-mahogany"
                             {"last_pk_fetched" {"legs" 2 "leaf" "balsa"}
                              "max_pk_values" {"legs" 4 "leaf" "birch"}}}})))
  )

(deftest ^:integration build-log-based-sql-query-test
  (with-matrix-assertions test-db-configs test-db-fixture
    ;; No PK
    (is (thrown? AssertionError (logical/build-log-based-sql-query
                                 (catalog/discover test-db-config)
                                 "full_table_sync_test-dbo-no_pk_table"
                                 {} )))
    ;; No current_log_version bookmark
    (is (thrown? AssertionError (logical/build-log-based-sql-query
                                 (catalog/discover test-db-config)
                                 "full_table_sync_test-dbo-basic_table"
                                 {} )))
    ;; Has primary key, no record Keys, no primary key bookmark
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.id FROM CHANGETABLE (CHANGES dbo.basic_table, 0) as c LEFT JOIN dbo.basic_table ON c.id=dbo.basic_table.id LEFT JOIN sys.dm_tran_commit_table tc on c.SYS_CHANGE_VERSION = tc.commit_ts ORDER BY c.SYS_CHANGE_VERSION, c.id"]
         (logical/build-log-based-sql-query
          (update-in (catalog/discover test-db-config)
                     ["streams" "full_table_sync_test-dbo-basic_table" "metadata" "properties" "value"]
                     assoc
                     "selected" false)
          "full_table_sync_test-dbo-basic_table"
          {"bookmarks" {"full_table_sync_test-dbo-basic_table" {"current_log_version" 0}}})))
    ;; Has PK, No Selected Fields, Has Bookmark
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.id FROM CHANGETABLE (CHANGES dbo.basic_table, 0) as c LEFT JOIN dbo.basic_table ON c.id=dbo.basic_table.id LEFT JOIN sys.dm_tran_commit_table tc on c.SYS_CHANGE_VERSION = tc.commit_ts WHERE c.SYS_CHANGE_VERSION = 0 AND c.id >= ? ORDER BY c.SYS_CHANGE_VERSION, c.id" "foo"]
         (logical/build-log-based-sql-query
          (update-in (catalog/discover test-db-config)
                     ["streams" "full_table_sync_test-dbo-basic_table" "metadata" "properties" "value"]
                     assoc
                     "selected" false)
          "full_table_sync_test-dbo-basic_table"
          {"bookmarks"
           {"full_table_sync_test-dbo-basic_table"
            {"current_log_version" 0
             "last_pk_fetched"     {"id" "foo"}}}})))
    ;; Has primary key, selected fields, no primary key bookmark
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.id, dbo.basic_table.value FROM CHANGETABLE (CHANGES dbo.basic_table, 0) as c LEFT JOIN dbo.basic_table ON c.id=dbo.basic_table.id LEFT JOIN sys.dm_tran_commit_table tc on c.SYS_CHANGE_VERSION = tc.commit_ts ORDER BY c.SYS_CHANGE_VERSION, c.id"]
         (logical/build-log-based-sql-query
          (catalog/discover test-db-config)
          "full_table_sync_test-dbo-basic_table"
          {"bookmarks"
           {"full_table_sync_test-dbo-basic_table"
            {"current_log_version" 0}}})))

    ;; Has primary key, selected fields, primary key bookmark
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.id, dbo.basic_table.value FROM CHANGETABLE (CHANGES dbo.basic_table, 0) as c LEFT JOIN dbo.basic_table ON c.id=dbo.basic_table.id LEFT JOIN sys.dm_tran_commit_table tc on c.SYS_CHANGE_VERSION = tc.commit_ts WHERE c.SYS_CHANGE_VERSION = 0 AND c.id >= ? ORDER BY c.SYS_CHANGE_VERSION, c.id" "foo"]
         (logical/build-log-based-sql-query
          (catalog/discover test-db-config)
          "full_table_sync_test-dbo-basic_table"
          {"bookmarks"
           {"full_table_sync_test-dbo-basic_table"
            {"current_log_version" 0
             "last_pk_fetched"     {"id" "foo"}}}})))
    ;; Has composite primary keys, selected fields, bookmarks for both pks
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.id, c.second_id, dbo.composite_key_table.value FROM CHANGETABLE (CHANGES dbo.composite_key_table, 0) as c LEFT JOIN dbo.composite_key_table ON c.id=dbo.composite_key_table.id AND c.second_id=dbo.composite_key_table.second_id LEFT JOIN sys.dm_tran_commit_table tc on c.SYS_CHANGE_VERSION = tc.commit_ts WHERE c.SYS_CHANGE_VERSION = 0 AND c.id >= ? AND c.second_id >= ? ORDER BY c.SYS_CHANGE_VERSION, c.id, c.second_id" "foo" "bar"]
         (logical/build-log-based-sql-query
          (catalog/discover test-db-config)
          "full_table_sync_test-dbo-composite_key_table"
          {"bookmarks"
           {"full_table_sync_test-dbo-composite_key_table"
            {"current_log_version" 0
             "last_pk_fetched"     {"id"        "foo"
                                    "second_id" "bar"}}}})))
    )
  )


(deftest transform-rowversion-test
  ;; Convert to hex number (8 bytes)
  (is (= "0x000000000000000A"
         (singer-transform/transform-rowversion (byte-array [0 0 0 0 0 0 0 10])))))

(deftest maybe-write-activate-version!-test
  ;; FULL_TABLE replication
  (is (= "{\"type\":\"ACTIVATE_VERSION\",\"stream\":\"jet_stream\",\"version\":1560363676948}\n"
         (with-redefs [singer-messages/now (constantly 1560363676948)]
           (with-out-str (singer-messages/maybe-write-activate-version! "jet_stream" "FULL_TABLE" {}))))
      "Write activate version if no version has started loading yet and doing full table replication")
  (is (= ""
         (with-redefs [singer-messages/now (constantly 1560363676948)]
           (with-out-str (singer-messages/maybe-write-activate-version! "jet_stream" "FULL_TABLE" {"bookmarks" {"jet_stream" {"version" 999}}}))))
      "Don't emit an activate_version message if a version exists in state and syncning full table replication")
  ;; Resuming an interrupted full table sync
  (is (= {"bookmarks" {"jet_stream" {"last_pk_fetched" 1 "version" 1560363676948}}}
         (with-out-and-err-to-dev-null
           (singer-messages/maybe-write-activate-version! "jet_stream" "FULL_TABLE" {"bookmarks" {"jet_stream" {"last_pk_fetched" 1 "version" 1560363676948}}})))
      "Resuming an interrupted full table sync should keep the old state")

  ;; LOG_BASED
  (is (= "{\"type\":\"ACTIVATE_VERSION\",\"stream\":\"jet_stream\",\"version\":1560363676948}\n"
         (with-redefs [singer-messages/now (constantly 1560363676948)]
           (with-out-str (singer-messages/maybe-write-activate-version! "jet_stream" "LOG_BASED" {}))))
      "Write activate version if no version has started loading yet and doing log based replication")

  ;; INCREMENTAL / LOG_BASED
  (is (= {"bookmarks" {"jet_stream" {"version" 1560363676948}}}
         (with-redefs [singer-messages/now (constantly 1560363676948)]
           (with-out-and-err-to-dev-null
             (singer-messages/maybe-write-activate-version! "jet_stream" "INCREMENTAL" {}))))
      "Add a version into the state if none exists already and syncing incrementally")
  (is (= {"bookmarks" {"jet_stream" {"version" 1560363676948}}}
         (with-out-and-err-to-dev-null
           (singer-messages/maybe-write-activate-version! "jet_stream" "LOG_BASED" {"bookmarks" {"jet_stream" {"version" 1560363676948}}})))
      "Keep the version in the state and syncing incrementally or log based"))
