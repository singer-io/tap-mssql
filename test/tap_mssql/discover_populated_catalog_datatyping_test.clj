(ns tap-mssql.discover-populated-catalog-datatyping-test
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
    (jdbc/db-do-commands db-spec ["CREATE DATABASE empty_database"
                                  "CREATE DATABASE database_with_a_table"
                                  "CREATE DATABASE another_database_with_a_table"
                                  "CREATE DATABASE datatyping"])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_with_a_table")
                         [(jdbc/create-table-ddl :empty_table [[:id "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_with_a_table")
                         ["CREATE VIEW empty_table_ids
                           AS
                           SELECT id FROM empty_table"])
    (jdbc/db-do-commands (assoc db-spec :dbname "another_database_with_a_table")
                         [(jdbc/create-table-ddl "another_empty_table" [[:id "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "datatyping")
                         [(jdbc/create-table-ddl :integers
                                                 [[:bigint "bigint"]
                                                  [:int "int"]
                                                  [:smallint "smallint"]
                                                  [:tinyint "tinyint"]])

                          (jdbc/create-table-ddl :bits
                                                 [[:bit "bit"]])
                          (jdbc/create-table-ddl :texts
                                                 [[:char "char"]
                                                  [:char_one "char(1)"]
                                                  [:char_max "char(8000)"]
                                                  [:binary "binary"]
                                                  [:binary_one "binary(1)"]
                                                  ;; Values as small as
                                                  ;; 100 here failed with
                                                  ;; the following error:
                                                  ;; `com.microsoft.sqlserver.jdbc.SQLServerException:
                                                  ;; Creating or altering
                                                  ;; table 'texts' failed
                                                  ;; because the minimum
                                                  ;; row size would be
                                                  ;; 8111, including 7
                                                  ;; bytes of internal
                                                  ;; overhead. This
                                                  ;; exceeds the maximum
                                                  ;; allowable table row
                                                  ;; size of 8060 bytes.`
                                                  [:binary_max "binary(10)"]
                                                  [:varbinary "varbinary"]
                                                  [:varbinary_one "varbinary(1)"]
                                                  [:varbinary_8000 "varbinary(8000)"]
                                                  [:varbinary_max "varbinary(max)"]])])))

(defn test-db-fixture [f]
  (maybe-destroy-test-db)
  (create-test-db)
  (f))

(use-fixtures :each test-db-fixture)

(deftest ^:integration verify-integers
  (is (= {:type "integer"
          :minimum -2147483648
          :maximum  2147483647}
         (get-in (discover-catalog test-db-config)
                 [:streams "integers" :schema :properties "int"])))
  (is (= {:type "integer"
          :minimum -9223372036854775808
          :maximum  9223372036854775807}
         (get-in (discover-catalog test-db-config)
                 [:streams "integers" :schema :properties "bigint"])))
  (is (= {:type "integer"

          :minimum -32768
          :maximum  32767}
         (get-in (discover-catalog test-db-config)
                 [:streams "integers" :schema :properties "smallint"])))
  (is (= {:type "integer"
          :minimum 0
          :maximum 255}
         (get-in (discover-catalog test-db-config)
                 [:streams "integers" :schema :properties "tinyint"]))))

(deftest ^:integration verify-bits
  (is (= {:type "boolean"}
         (get-in (discover-catalog test-db-config)
                 [:streams "bits" :schema :properties "bit"]))))

(deftest ^:integration verify-text
  (is (= {:type "string"
          :minLength 1
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "texts" :schema :properties "char"])))
  (is (= {:type "string"
          :minLength 1
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "texts" :schema :properties "char_one"])))
  (is (= {:type "string"
          :minLength 8000
          :maxLength 8000}
         (get-in (discover-catalog test-db-config)
                 [:streams "texts" :schema :properties "char_max"])))
  (is (= {:type "string"
          :minLength 1
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "texts" :schema :properties "binary"])))
  (is (= {:type "string"
          :minLength 1
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "texts" :schema :properties "binary_one"])))
  (is (= {:type "string"
          :minLength 10
          :maxLength 10}
         (get-in (discover-catalog test-db-config)
                 [:streams "texts" :schema :properties "binary_max"])))
  (is (= {:type "string"
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "texts" :schema :properties "varbinary"])))
  (is (= {:type "string"
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "texts" :schema :properties "varbinary_one"])))
  (is (= {:type "string"
          :maxLength 2147483647}
         (get-in (discover-catalog test-db-config)
                 [:streams "texts" :schema :properties "varbinary_max"]))))

(comment
  (map select-keys (get-columns test-db-config) (repeat [:column_name :type_name :sql_data_type]))
  (jdbc/with-db-metadata [md (assoc (config->conn-map test-db-config)
                                    :dbname "another_database_with_a_table")]
    (jdbc/metadata-result (.getColumns md nil "dbo" nil nil)))

  (clojure.test/run-tests *ns*)
  )
