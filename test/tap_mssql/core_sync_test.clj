(ns tap-mssql.core-sync-test
  (:require [clojure.test :refer [is deftest]]
            [clojure.java.jdbc :as jdbc]
            [tap-mssql.test-utils :refer [with-out-and-err-to-dev-null
                                          test-db-config
                                          test-db-configs
                                          with-matrix-assertions]]
            [tap-mssql.core :refer :all]))

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
                           "basic_table"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_sync_test")
                         [(jdbc/create-table-ddl
                           "composite_key_table"
                           [[:id "uniqueidentifier NOT NULL DEFAULT NEWID()"]
                            [:second_id "int NOT NULL IDENTITY"]
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
               (build-sync-query "craftsmanship-dbo-mahogany" "mahogany" [] {})))
  ;; No bookmark, no pk = Full Table sync query
  (is (= ["SELECT legs, tabletop, leaf FROM mahogany"]
         (build-sync-query "craftsmanship-dbo-mahogany" "mahogany" ["legs", "tabletop", "leaf"] {})))
  ;; No bookmark, yes pk = First FT Interruptible query
  (is (= '("SELECT legs, tabletop, leaf FROM mahogany WHERE legs <= ? AND leaf <= ? ORDER BY legs, leaf"
          4
          "birch")
         (build-sync-query "craftsmanship-dbo-mahogany" "mahogany" ["legs", "tabletop", "leaf"]
                           {"bookmarks" {"craftsmanship-dbo-mahogany" {"max_pk_values" {"legs" 4 "leaf" "birch"}}}})))
  ;; Bookmark, no pk = ??? Invalid state
  (is (thrown? AssertionError
               (build-sync-query "craftsmanship-dbo-mahogany" "mahogany" ["legs", "tabletop", "leaf"]
                                 {"bookmarks" {"craftsmanship-dbo-mahogany" {"last_pk_fetched" {"legs" 2 "leaf" "balsa"}}}})))
  ;; Bookmark and PK = Resuming Full Table Sync
  (is (= '("SELECT legs, tabletop, leaf FROM mahogany WHERE legs >= ? AND leaf >= ? AND legs <= ? AND leaf <= ? ORDER BY legs, leaf"
          2
          "balsa"
          4
          "birch")
         (build-sync-query "craftsmanship-dbo-mahogany" "mahogany" ["legs", "tabletop", "leaf"]
                           {"bookmarks"
                            {"craftsmanship-dbo-mahogany"
                             {"last_pk_fetched" {"legs" 2 "leaf" "balsa"}
                              "max_pk_values" {"legs" 4 "leaf" "birch"}}}})))
  )

(deftest ^:integration build-log-based-sql-query-test
  (with-matrix-assertions test-db-configs test-db-fixture
    ;; No PK
    (is (thrown? AssertionError (build-log-based-sql-query
                                 (discover-catalog test-db-config)
                                 "full_table_sync_test-dbo-no_pk_table"
                                 {} )))
    ;; No current_log_version bookmark
    (is (thrown? AssertionError (build-log-based-sql-query
                                 (discover-catalog test-db-config)
                                 "full_table_sync_test-dbo-basic_table"
                                 {} )))
    ;; Has primary key, no record Keys, no primary key bookmark
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.id FROM CHANGETABLE (CHANGES basic_table, 0) as c LEFT JOIN basic_table ON c.id=basic_table.id LEFT JOIN sys.dm_tran_commit_table tc on c.sys_change_version = tc.commit_ts ORDER BY c.SYS_CHANGE_VERSION, c.id"]
         (build-log-based-sql-query
          (update-in (discover-catalog test-db-config)
                     ["streams" "full_table_sync_test-dbo-basic_table" "metadata" "properties" "value"]
                     assoc
                     "selected" false)
          "full_table_sync_test-dbo-basic_table"
          {"bookmarks" {"full_table_sync_test-dbo-basic_table" {"current_log_version" 0}}})))
    ;; Has PK, No Selected Fields, Has Bookmark
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.id FROM CHANGETABLE (CHANGES basic_table, 0) as c LEFT JOIN basic_table ON c.id=basic_table.id LEFT JOIN sys.dm_tran_commit_table tc on c.sys_change_version = tc.commit_ts WHERE c.SYS_CHANGE_VERSION = 0 AND c.id >= ? ORDER BY c.SYS_CHANGE_VERSION, c.id" "foo"]
         (build-log-based-sql-query
          (update-in (discover-catalog test-db-config)
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
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.id, basic_table.value FROM CHANGETABLE (CHANGES basic_table, 0) as c LEFT JOIN basic_table ON c.id=basic_table.id LEFT JOIN sys.dm_tran_commit_table tc on c.sys_change_version = tc.commit_ts ORDER BY c.SYS_CHANGE_VERSION, c.id"]
         (build-log-based-sql-query
          (discover-catalog test-db-config)
          "full_table_sync_test-dbo-basic_table"
          {"bookmarks"
           {"full_table_sync_test-dbo-basic_table"
            {"current_log_version" 0}}})))

    ;; Has primary key, selected fields, primary key bookmark
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.id, basic_table.value FROM CHANGETABLE (CHANGES basic_table, 0) as c LEFT JOIN basic_table ON c.id=basic_table.id LEFT JOIN sys.dm_tran_commit_table tc on c.sys_change_version = tc.commit_ts WHERE c.SYS_CHANGE_VERSION = 0 AND c.id >= ? ORDER BY c.SYS_CHANGE_VERSION, c.id" "foo"]
         (build-log-based-sql-query
          (discover-catalog test-db-config)
          "full_table_sync_test-dbo-basic_table"
          {"bookmarks"
           {"full_table_sync_test-dbo-basic_table"
            {"current_log_version" 0
             "last_pk_fetched"     {"id" "foo"}}}})))
    ;; Has composite primary keys, selected fields, bookmarks for both pks
    (is (=
         ["SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time, c.id, c.second_id, composite_key_table.value FROM CHANGETABLE (CHANGES composite_key_table, 0) as c LEFT JOIN composite_key_table ON c.id=composite_key_table.id AND c.second_id=composite_key_table.second_id LEFT JOIN sys.dm_tran_commit_table tc on c.sys_change_version = tc.commit_ts WHERE c.SYS_CHANGE_VERSION = 0 AND c.id >= ? AND c.second_id >= ? ORDER BY c.SYS_CHANGE_VERSION, c.id, c.second_id" "foo" "bar"]
         (build-log-based-sql-query
          (discover-catalog test-db-config)
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
         (transform-rowversion (byte-array [0 0 0 0 0 0 0 10])))))

(deftest maybe-write-activate-version!-test
  ;; Fresh State
  (is (= "{\"type\":\"ACTIVATE_VERSION\",\"stream\":\"jet_stream\",\"version\":1560363676948}\n"
         (with-redefs [now (constantly 1560363676948)]
           (with-out-str (maybe-write-activate-version! "jet_stream" {}))))
      "Write activate version if no version has started loading yet (none exists in state)")
  (is (= {"bookmarks" {"jet_stream" {"version" 1560363676948}}}
         (with-redefs [now (constantly 1560363676948)]
           (with-out-and-err-to-dev-null
             (maybe-write-activate-version! "jet_stream" {}))))
      "Add a version into the state if none exists already")
  ;; State with existing version
  (is (= ""
         (with-redefs [now (constantly 1560363676948)]
           (with-out-str (maybe-write-activate-version! "jet_stream" {"bookmarks" {"jet_stream" {"version" 999}}}))))
      "Don't emit an activate_version message if a version exists in state.")
  (is (= {"bookmarks" {"jet_stream" {"version" 1560363676948}}}
         (with-redefs [now (constantly 1560363676948)]
           (with-out-and-err-to-dev-null
             (maybe-write-activate-version! "jet_stream" {"bookmarks" {"jet_stream" {"version" 999}}}))))
      "Always add a new version into state")
  )

(deftest update-state-test
  (is (= {"bookmarks" {"jet_stream" {"updated_at" "this time"}}}
         (update-state "jet_stream" ["updated_at"] {} {"id" 123, "updated_at" "this time"})))
  (is (= {"bookmarks" {"jet_stream" {"updated_at" "that time"}}}
         (update-state "jet_stream"
                       ["updated_at"]
                       {"bookmarks" {"jet_stream" {"updated_at" "this time"}}}
                       {"id" 123, "updated_at" "that time"})))
  (is (= {"bookmarks" {"jet_stream" {"updated_at" "this time", "id" 123}}}
         (update-state "jet_stream" ["id" "updated_at"] {} {"id" 123, "updated_at" "this time"}))))
