(ns tap-mssql.discover-empty-catalog-test
  (:require [clojure.test :refer [is deftest use-fixtures]]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.string :as string]
            [tap-mssql.core :refer :all]
            [tap-mssql.test-utils :refer [with-out-and-err-to-dev-null
                                          test-db-config
                                          test-db-configs
                                          def-matrix-tests
                                          *test-db-config*]]))

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
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db)
    (create-test-db)
    (f)))

;; TODO: These functions should use the config from the binding somehow
;;       Otherwise, the database won't be set up for the tests
(use-fixtures :each test-db-fixture)

;; TODO: For some reason these are failing. 2017 seems to be using the 2014 definition? (5:45ish)
;; -- Follow-up: This seems to be working now, inexplicably. (6:24, no changes, just redef'd)
(def-matrix-tests verify-mssql-version test-db-configs
  (is (nil? (do-discovery *test-db-config*))
      "Discovery ran succesfully and did not throw an exception"))

#_(deftest ^:integration verify-mssql-version
  (is (nil?
       (do-discovery test-db-config))
      "Discovery ran succesfully and did not throw an exception"))

(deftest ^:integration verify-empty-catalog
  (is (= empty-catalog (discover-catalog test-db-config))
      "Databases without any tables (like empty_database) do not show up in the catalog"))

(comment
  (map (comp (partial ns-unmap *ns*) #(.sym %)) (filter (comp :test meta) (vals (ns-publics *ns*))))

  (ns-unmap *ns* (.sym #'tap-mssql.discover-empty-catalog-test/*test-db-config*))
  )
