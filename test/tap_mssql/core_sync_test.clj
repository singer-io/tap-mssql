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
                           "basic_table"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_sync_test")
                         [(jdbc/create-table-ddl
                           "composite_key_table"
                           [[:id "uniqueidentifier NOT NULL DEFAULT NEWID()"]
                            [:second_id "int NOT NULL IDENTITY"]
                            [:value "int"]
                            ["primary key (id, second_id)"]])])))

(defn test-db-fixture [f]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db)
    (create-test-db)
    (f)))

(deftest build-sync-query-test
  (is (thrown? AssertionError
               (build-sync-query "mahogany" [] {})))
  ;; No bookmark, no pk = Full Table sync query
  (is (= ["SELECT legs, tabletop, leaf FROM mahogany"]
         (build-sync-query "mahogany" ["legs", "tabletop", "leaf"] {})))
  ;; No bookmark, yes pk = First FT Interruptible query
  (is (= '("SELECT legs, tabletop, leaf FROM mahogany WHERE legs <= ? AND leaf <= ? ORDER BY legs, leaf"
          4
          "birch")
         (build-sync-query "mahogany" ["legs", "tabletop", "leaf"]
                           {"bookmarks" {"mahogany" {"max_pk_values" {"legs" 4 "leaf" "birch"}}}})))
  ;; Bookmark, no pk = ??? Invalid state
  (is (thrown? AssertionError
               (build-sync-query "mahogany" ["legs", "tabletop", "leaf"]
                                 {"bookmarks" {"mahogany" {"last_pk_fetched" {"legs" 2 "leaf" "balsa"}}}})))
  ;; Bookmark and PK = Resuming Full Table Sync
  (is (= '("SELECT legs, tabletop, leaf FROM mahogany WHERE legs >= ? AND leaf >= ? AND legs <= ? AND leaf <= ? ORDER BY legs, leaf"
          2
          "balsa"
          4
          "birch")
         (build-sync-query "mahogany" ["legs", "tabletop", "leaf"]
                           {"bookmarks"
                            {"mahogany"
                             {"last_pk_fetched" {"legs" 2 "leaf" "balsa"}
                              "max_pk_values" {"legs" 4 "leaf" "birch"}}}})))
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
