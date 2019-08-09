(ns tap-mssql.discover-populated-catalog-metadata-test
  (:require
            [tap-mssql.catalog :as catalog]
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
    (jdbc/db-do-commands db-spec ["CREATE DATABASE database_for_metadata"])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_for_metadata")
                         [(jdbc/create-table-ddl :table_with_a_primary_key [[:id "int primary key"]
                                                                            [:name "varchar"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_for_metadata")
                         ["CREATE TABLE table_with_a_composite_key (id int, col_b varchar, name varchar, primary key (id, col_b))"])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_for_metadata")
                         ["CREATE VIEW view_of_table_with_a_primary_key_id
                           AS
                           SELECT id FROM table_with_a_primary_key"])))

(defn populate-data
  []
  (jdbc/insert! (-> (config/->conn-map test-db-config)
                          (assoc :dbname "database_for_metadata"))
                      "dbo.table_with_a_primary_key"
                      {:id 1 :name "t"}))

(defn test-db-fixture [f]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db)
    (create-test-db)
    (populate-data)
    (f)))

(use-fixtures :each test-db-fixture)

(deftest ^:integration verify-metadata
  (is (= "automatic"
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_table_with_a_primary_key" "metadata" "properties" "id" "inclusion"])))
    (is (= "int"
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_table_with_a_primary_key" "metadata" "properties" "id" "sql-datatype"])))
    (is (= true
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_table_with_a_primary_key" "metadata" "properties" "id" "selected-by-default"])))
    (is (= "available"
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_table_with_a_primary_key" "metadata" "properties" "name" "inclusion"])))
    (is (= "varchar"
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_table_with_a_primary_key" "metadata" "properties" "name" "sql-datatype"])))
    (is (= true
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_table_with_a_primary_key" "metadata" "properties" "name" "selected-by-default"])))
    (is (= "database_for_metadata"
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_table_with_a_primary_key" "metadata"  "database-name"])))
    (is (= "dbo"
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_table_with_a_primary_key" "metadata"  "schema-name"])))
    (is (= false
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_table_with_a_primary_key" "metadata"  "is-view"])))
    (is (= #{"id"}
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_table_with_a_primary_key" "metadata"  "table-key-properties"])))
    (is (= true
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_view_of_table_with_a_primary_key_id" "metadata" "is-view"])))
    (is (= 1
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_table_with_a_primary_key" "metadata" "row-count"])))
    (is (= 0
           (get-in (catalog/discover test-db-config)
                   ["streams" "database_for_metadata_dbo_view_of_table_with_a_primary_key_id" "metadata" "row-count"]))))

(deftest ^:integration verify-metadata-when-composite-keys
  (let [catalog (catalog/discover test-db-config)]
    (is (= #{"id" "col_b"}
           (get-in catalog
                   ["streams" "database_for_metadata_dbo_table_with_a_composite_key" "metadata"  "table-key-properties"])))))
