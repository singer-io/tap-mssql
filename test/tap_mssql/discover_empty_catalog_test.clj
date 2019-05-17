(ns tap-mssql.discover-empty-catalog-test
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
  {:host (format "%s-test-mssql-2017.db.test.stitchdata.com"
                 (get-test-hostname))
   :user (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_USER")
   :password (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD")})

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
    (jdbc/db-do-commands db-spec ["CREATE DATABASE empty_database"])))

(defn test-db-fixture [f]
  (maybe-destroy-test-db)
  (create-test-db)
  (f))

(use-fixtures :each test-db-fixture)

(defmacro with-out-and-err-to-dev-null
  [& body]
  `(let [null-out# (io/writer
                    (proxy [java.io.OutputStream] []
                      (write [& args#])))]
     (binding [*err* null-out#
               *out* null-out#]
       ~@body)))

(deftest ^:integration verify-mssql-version
  (is (nil?
       (with-out-and-err-to-dev-null
         (do-discovery test-db-config)))
      "Discovery ran succesfully and did not throw an exception"))

(deftest ^:integration verify-empty-catalog
  (is (= empty-catalog (discover-catalog test-db-config))
      "Databases without any tables (like empty_database) do not show up in the catalog"))
