(ns tap-mssql.discover-populated-catalog-with-multiple-non-system-databases-test
  (:require [tap-mssql.catalog :as catalog]
            [tap-mssql.config :as config]
            [clojure.test :refer [is deftest use-fixtures]]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.string :as string]
            [tap-mssql.core :refer :all]
            [tap-mssql.test-utils :refer [maybe-destroy-test-db
                                          with-out-and-err-to-dev-null
                                          test-db-config]]))

(defn create-test-db
  []
  (let [db-spec (config/->conn-map test-db-config)]
    (jdbc/db-do-commands db-spec ["CREATE DATABASE empty_database"
                                  "CREATE DATABASE database_with_a_table"
                                  "CREATE DATABASE another_database_with_a_table"])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_with_a_table")
                         [(jdbc/create-table-ddl :empty_table [[:id "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_with_a_table")
                         ["CREATE VIEW empty_table_ids
                           AS
                           SELECT id FROM empty_table"])
    (jdbc/db-do-commands (assoc db-spec :dbname "another_database_with_a_table")
                         [(jdbc/create-table-ddl "another_empty_table" [[:id "int"]])])))

(defn test-db-fixture [f]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db)
    (create-test-db)
    (f)))

(use-fixtures :each test-db-fixture)

(deftest ^:integration verify-populated-catalog
  (is (let [stream-names (set (map #(get % "stream") (vals ((catalog/discover test-db-config) "streams"))))]
        (stream-names "empty_table")))
  (is (let [stream-names (set (map #(get % "stream") (vals ((catalog/discover test-db-config) "streams"))))]
        (stream-names "empty_table_ids")))
  (is (let [stream-names (set (map #(get % "stream") (vals ((catalog/discover test-db-config) "streams"))))]
        (stream-names "another_empty_table"))))
