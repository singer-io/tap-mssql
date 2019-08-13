(ns tap-mssql.discover-empty-catalog-test
  (:require [clojure.test :refer [is deftest use-fixtures]]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.string :as string]
            [tap-mssql.core :refer :all]
            [tap-mssql.catalog :as catalog]
            [tap-mssql.config :as config]
            [tap-mssql.test-utils :refer [with-out-and-err-to-dev-null
                                          test-db-config
                                          test-db-configs
                                          with-matrix-assertions]]))

(defn get-destroy-database-command
  [database]
  (format "DROP DATABASE %s" (:table_cat database)))

(defn maybe-destroy-test-db
  [config]
  (let [destroy-database-commands (->> (catalog/get-databases config)
                                       (filter catalog/non-system-database?)
                                       (map get-destroy-database-command))]
    (let [db-spec (config/->conn-map config)]
      (jdbc/db-do-commands db-spec destroy-database-commands))))

(defn create-test-db
  [config]
  (let [db-spec (config/->conn-map config)]
    (jdbc/db-do-commands db-spec ["CREATE DATABASE empty_database"])))

(defn test-db-fixture [f config]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db config)
    (create-test-db config)
    (f)))

(deftest ^:integration verify-throw-on-empty-catalog
  (with-matrix-assertions test-db-configs test-db-fixture
    (is (thrown-with-msg? java.lang.Exception
                          #"Empty Catalog: did not discover any streams"
                          (catalog/discover test-db-config)))))

(comment
  ;; TODO Can these be helper functions?
  ;; Clear all tests from namespace
  (map (comp (partial ns-unmap *ns*) #(.sym %)) (filter (comp :test meta) (vals (ns-publics *ns*))))
  ;; Clear entire namespace
  (map (comp (partial ns-unmap *ns*) #(.sym %)) (vals (ns-publics *ns*)))
  )
