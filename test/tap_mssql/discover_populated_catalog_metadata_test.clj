(ns tap-mssql.discover-populated-catalog-metadata-test
  (:require [clojure.test :refer [is deftest use-fixtures]]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.string :as string]
            [tap-mssql.core :refer :all]
            [tap-mssql.test-utils :refer [with-out-and-err-to-dev-null]]))

(defn get-test-hostname
  []
  (let [hostname (.getHostName (java.net.InetAddress/getLocalHost))]
    (if (string/starts-with? hostname "taps-")
      hostname
      "circleci")))

(def test-db-config
  {"host" (format "%s-test-mssql-2017.db.test.stitchdata.com"
                  (get-test-hostname))
   "user" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_USER")
   "password" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD")
   "port" "1433"})

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
    (jdbc/db-do-commands db-spec ["CREATE DATABASE database_for_metadata"])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_for_metadata")
                         [(jdbc/create-table-ddl :table_with_a_primary_key [[:id "int primary key"]
                                                                            [:name "varchar"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_for_metadata")
                         ["CREATE VIEW view_of_table_with_a_primary_key_id
                           AS
                           SELECT id FROM table_with_a_primary_key"])))

(defn test-db-fixture [f]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db)
    (create-test-db)
    (f)))

(use-fixtures :each test-db-fixture)

(comment
  (get-columns test-db-config)
  )

(deftest ^:integration verify-metadata
  (is (= "automatic"
         (get-in (discover-catalog test-db-config)
                 [:streams "table_with_a_primary_key" :metadata :properties "id" :inclusion])))
  (is (= "int"
         (get-in (discover-catalog test-db-config)
                 [:streams "table_with_a_primary_key" :metadata :properties "id" :sql-datatype])))
  (is (= true
         (get-in (discover-catalog test-db-config)
                 [:streams "table_with_a_primary_key" :metadata :properties "id" :selected-by-default])))
  (is (= "available"
         (get-in (discover-catalog test-db-config)
                 [:streams "table_with_a_primary_key" :metadata :properties "name" :inclusion])))
  (is (= "varchar"
         (get-in (discover-catalog test-db-config)
                 [:streams "table_with_a_primary_key" :metadata :properties "name" :sql-datatype])))
  (is (= true
         (get-in (discover-catalog test-db-config)
                 [:streams "table_with_a_primary_key" :metadata :properties "name" :selected-by-default])))
  (is (= "database_for_metadata"
         (get-in (discover-catalog test-db-config)
                 [:streams "table_with_a_primary_key" :metadata  :database-name])))
  (is (= "dbo"
         (get-in (discover-catalog test-db-config)
                 [:streams "table_with_a_primary_key" :metadata  :schema-name])))
  (is (= false
         (get-in (discover-catalog test-db-config)
                 [:streams "table_with_a_primary_key" :metadata  :is-view])))
  (is (= #{"id"}
         (get-in (discover-catalog test-db-config)
                 [:streams "table_with_a_primary_key" :metadata  :table-key-properties])))
  (is (= true
         (get-in (discover-catalog test-db-config)
                 [:streams "view_of_table_with_a_primary_key_id" :metadata :is-view]))))
