(ns tap-mssql.discover-populated-catalog-test
  (:require [tap-mssql.catalog :as catalog]
            [tap-mssql.config :as config]
            [clojure.test :refer [is deftest use-fixtures]]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.string :as string]
            [tap-mssql.core :refer :all]
            [tap-mssql.test-utils :refer [with-out-and-err-to-dev-null
                                          test-db-config]]))

(defn get-destroy-database-command
  [database]
  (format "DROP DATABASE %s" (:table_cat database)))

(defn maybe-destroy-test-db
  []
  (let [destroy-database-commands (->> (catalog/get-databases test-db-config)
                                       (filter catalog/non-system-database?)
                                       (map get-destroy-database-command))]
    (let [db-spec (config/->conn-map test-db-config)]
      (jdbc/db-do-commands db-spec destroy-database-commands))))

(defn create-test-db
  []
  (let [db-spec (config/->conn-map test-db-config)]
    (jdbc/db-do-commands db-spec ["CREATE DATABASE empty_database"
                                  "CREATE DATABASE database_with_a_table"
                                  "CREATE DATABASE database_with_table_valued_function"])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_with_a_table")
                         [(jdbc/create-table-ddl :empty_table [[:id "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_with_a_table")
                         ["CREATE VIEW empty_table_ids
                           AS
                           SELECT id FROM empty_table"])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_with_a_table")
                         ["CREATE FUNCTION table_valued_test(@input_value int)
                           RETURNS @result table (a_value int)
                           AS
                           BEGIN
                               INSERT INTO @result VALUES(@input_value + 1)
                               RETURN
                           END"])))

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
  ;; Table-Valued functions should not be discovered
  (is (nil? (let [stream-names (set (map #(get % "stream") (vals ((catalog/discover test-db-config) "streams"))))]
              (stream-names "table_valued_test")))))
