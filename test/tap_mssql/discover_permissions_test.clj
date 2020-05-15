(ns tap-mssql.discover-permissions-test
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
  (format "DROP DATABASE IF EXISTS %s" (:table_cat database)))

(defn maybe-destroy-test-db
  []
  (let [destroy-database-commands (->> [{:table_cat "not_authorized_database"}
                                        {:table_cat "database_with_a_table"}
                                        {:table_cat "not_authorized_database_too"}]
                                       (filter catalog/non-system-database?)
                                       (map get-destroy-database-command))]
    (let [db-spec (config/->conn-map test-db-config)]
      (jdbc/db-do-commands db-spec destroy-database-commands))))

(defn create-test-db
  []
  (let [db-spec (config/->conn-map test-db-config)]
    (jdbc/db-do-commands db-spec ["CREATE DATABASE not_authorized_database"
                                  "CREATE DATABASE database_with_a_table"
                                  "CREATE DATABASE not_authorized_database_too"])
    ;; Create Tables
    (jdbc/db-do-commands (assoc db-spec :dbname "database_with_a_table")
                         [(jdbc/create-table-ddl :empty_table [[:id "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "not_authorized_database")
                         [(jdbc/create-table-ddl :empty_table [[:id "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "not_authorized_database_too")
                         [(jdbc/create-table-ddl :empty_table [[:id "int"]])])
    ;; Create view/tvf for fun
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
                           END"])
    ;; Create User if not exists
    (when (empty? (jdbc/query (assoc db-spec :dbname "database_with_a_table")
                              "SELECT principal_id FROM sys.server_principals WHERE name = 'SingerTestUser'"))
      (jdbc/db-do-commands (assoc db-spec :dbname "database_with_a_table")
                           ["CREATE LOGIN SingerTestUser WITH PASSWORD = 'ABCD12345$%'"]))
    (when (empty? (jdbc/query (assoc db-spec :dbname "database_with_a_table")
                              "SELECT principal_id FROM sys.database_principals WHERE name = 'SingerTestUser'"))
      (jdbc/db-do-commands (assoc db-spec :dbname "database_with_a_table")
                           ["CREATE USER SingerTestUser FOR LOGIN SingerTestUser"
                            "GRANT SELECT ON dbo.empty_table TO SingerTestUser"
                            "GRANT SELECT ON dbo.empty_table_ids TO SingerTestUser"]))))

(defn test-db-fixture [f]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db)
    (create-test-db)
    (f)))

(use-fixtures :each test-db-fixture)

(deftest ^:integration verify-populated-catalog
  (is (let [stream-names (set (map #(get % "stream") (vals ((catalog/discover (assoc test-db-config
                                                                                     "user" "SingerTestUser"
                                                                                     "password" "ABCD12345$%"))
                                                            "streams"))))]
        (stream-names "empty_table")))
  (is (let [stream-names (set (map #(get % "stream") (vals ((catalog/discover (assoc test-db-config
                                                                                     "user" "SingerTestUser"
                                                                                     "password" "ABCD12345$%"))
                                                            "streams"))))]
        (stream-names "empty_table_ids")))
  ;; Table-Valued functions should not be discovered
  (is (nil? (let [stream-names (set (map #(get % "stream") (vals ((catalog/discover (assoc test-db-config
                                                                                           "user" "SingerTestUser"
                                                                                           "password" "ABCD12345$%"))
                                                                  "streams"))))]
              (stream-names "table_valued_test"))))

  ;; Should not discover tables in non-permitted databases
  (is (let [discovered-dbs (->> (get (catalog/discover (assoc test-db-config
                                                              "user" "SingerTestUser"
                                                              "password" "ABCD12345$%")) "streams")
                                (map (fn [[_ schema]] (get-in schema ["metadata" "database-name"])))
                                set)]
        (empty? (clojure.set/intersection
                 discovered-dbs
                 #{"not_authorized_database" "not_authorized_database_too"})))))
