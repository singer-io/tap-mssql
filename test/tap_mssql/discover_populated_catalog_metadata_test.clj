(ns tap-mssql.discover-populated-catalog-metadata-test
  (:require [clojure.test :refer [is deftest use-fixtures]]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.string :as string]
            [tap-mssql.core :refer :all]))

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
   "password" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD")})

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
                                                                            [:name "varchar"]])])))

(defn test-db-fixture [f]
  (maybe-destroy-test-db)
  (create-test-db)
  (f))

(use-fixtures :each test-db-fixture)

(deftest ^:integration verify-primary-key-metadata
  (is (= "automatic"
         (get-in (discover-catalog test-db-config)
                 [:streams "table_with_a_primary_key" :metadata :properties "id" :inclusion])))
  (is (= "available"
         (get-in (discover-catalog test-db-config)
                 [:streams "table_with_a_primary_key" :metadata :properties "name" :inclusion]))))
