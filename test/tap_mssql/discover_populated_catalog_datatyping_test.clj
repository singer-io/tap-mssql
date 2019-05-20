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
                         [(jdbc/create-table-ddl :exact_numerics
                                                 ;; https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-2017#exact-numerics
                                                 [[:bigint "bigint"]
                                                  [:int "int"]
                                                  [:smallint "smallint"]
                                                  [:tinyint "tinyint"]
                                                  [:bit "bit"]])
                          (jdbc/create-table-ddl :approximate_numerics
                                                 ;; https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-2017#approximate-numerics
                                                 [[:float "float"]
                                                  [:float_1 "float(1)"]
                                                  [:float_24 "float(24)"]
                                                  [:float_25 "float(25)"]
                                                  [:float_53 "float(53)"]
                                                  [:double_precision "double precision"]
                                                  [:real "real"]])
                          (jdbc/create-table-ddl :character_strings
                                                 ;; https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-2017#character-strings
                                                 [[:char "char"]
                                                  [:char_one "char(1)"]
                                                  [:char_8000 "char(8000)"]
                                                  [:varchar "varchar"]
                                                  [:varchar_one "varchar(1)"]
                                                  [:varchar_8000 "varchar(8000)"]
                                                  [:varchar_max "varchar(max)"]])
                          (jdbc/create-table-ddl :unicode_character_strings
                                                 ;; https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-2017#unicode-character-strings
                                                 [[:nchar "nchar"]
                                                  [:nchar_1 "nchar(1)"]
                                                  [:nchar_4000 "nchar(4000)"]
                                                  [:nvarchar "nvarchar"]
                                                  [:nvarchar_1 "nvarchar(1)"]
                                                  [:nvarchar_4000 "nvarchar(4000)"]
                                                  [:nvarchar_max "nvarchar(max)"]])
                          (jdbc/create-table-ddl :binary_strings
                                                 ;; https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-2017#binary-strings
                                                 [[:binary "binary"]
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
                                                  [:binary_10 "binary(10)"]
                                                  [:varbinary "varbinary"]
                                                  [:varbinary_one "varbinary(1)"]
                                                  [:varbinary_8000 "varbinary(8000)"]
                                                  [:varbinary_max "varbinary(max)"]])])))

(defn test-db-fixture [f]
  (maybe-destroy-test-db)
  (create-test-db)
  (f))

(use-fixtures :each test-db-fixture)

(deftest ^:integration verify-approximate-numerics
  (is (= {:type "number"}
         (get-in (discover-catalog test-db-config)
                 [:streams "approximate_numerics" :schema :properties "float"])))
  (is (= {:type "number"}
         (get-in (discover-catalog test-db-config)
                 [:streams "approximate_numerics" :schema :properties "float_1"])))
  (is (= {:type "number"}
         (get-in (discover-catalog test-db-config)
                 [:streams "approximate_numerics" :schema :properties "float_24"])))
  (is (= {:type "number"}
         (get-in (discover-catalog test-db-config)
                 [:streams "approximate_numerics" :schema :properties "float_25"])))
  (is (= {:type "number"}
         (get-in (discover-catalog test-db-config)
                 [:streams "approximate_numerics" :schema :properties "float_53"])))
  (is (= {:type "number"}
         (get-in (discover-catalog test-db-config)
                 [:streams "approximate_numerics" :schema :properties "double_precision"])))
  (is (= {:type "number"}
         (get-in (discover-catalog test-db-config)
                 [:streams "approximate_numerics" :schema :properties "real"]))))

(deftest ^:integration verify-unicode-strings
  (is (= {:type "string"
          :minLength 1
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "unicode_character_strings" :schema :properties "nchar"])))
  (is (= {:type "string"
          :minLength 1
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "unicode_character_strings" :schema :properties "nchar_1"])))
  (is (= {:type "string"
          :minLength 4000
          :maxLength 4000}
         (get-in (discover-catalog test-db-config)
                 [:streams "unicode_character_strings" :schema :properties "nchar_4000"])))
  (is (= {:type "string"
          :minLength 0
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "unicode_character_strings" :schema :properties "nvarchar"])))
  (is (= {:type "string"
          :minLength 0
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "unicode_character_strings" :schema :properties "nvarchar_1"])))
  (is (= {:type "string"
          :minLength 0
          :maxLength 4000}
         (get-in (discover-catalog test-db-config)
                 [:streams "unicode_character_strings" :schema :properties "nvarchar_4000"])))
  (is (= {:type "string"
          :minLength 0
          :maxLength 2147483647}
         (get-in (discover-catalog test-db-config)
                 [:streams "unicode_character_strings" :schema :properties "nvarchar_max"]))))

(deftest ^:integration verify-exact-numerics
  (is (= {:type "integer"
          :minimum -2147483648
          :maximum  2147483647}
         (get-in (discover-catalog test-db-config)
                 [:streams "exact_numerics" :schema :properties "int"])))
  (is (= {:type "integer"
          :minimum -9223372036854775808
          :maximum  9223372036854775807}
         (get-in (discover-catalog test-db-config)
                 [:streams "exact_numerics" :schema :properties "bigint"])))
  (is (= {:type "integer"

          :minimum -32768
          :maximum  32767}
         (get-in (discover-catalog test-db-config)
                 [:streams "exact_numerics" :schema :properties "smallint"])))
  (is (= {:type "integer"
          :minimum 0
          :maximum 255}
         (get-in (discover-catalog test-db-config)
                 [:streams "exact_numerics" :schema :properties "tinyint"])))
  (is (= {:type "boolean"}
         (get-in (discover-catalog test-db-config)
                 [:streams "exact_numerics" :schema :properties "bit"]))))

(deftest ^:integration verify-character-strings
  (is (= {:type "string"
          :minLength 1
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "character_strings" :schema :properties "char"])))
  (is (= {:type "string"
          :minLength 1
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "character_strings" :schema :properties "char_one"])))
  (is (= {:type "string"
          :minLength 8000
          :maxLength 8000}
         (get-in (discover-catalog test-db-config)
                 [:streams "character_strings" :schema :properties "char_8000"])))
  (is (= {:type "string"
          :minLength 0
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "character_strings" :schema :properties "varchar"])))
  (is (= {:type "string"
          :minLength 0
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "character_strings" :schema :properties "varchar_one"])))
  (is (= {:type "string"
          :minLength 0
          :maxLength 8000}
         (get-in (discover-catalog test-db-config)
                 [:streams "character_strings" :schema :properties "varchar_8000"])))
  (is (= {:type "string"
          :minLength 0
          :maxLength 2147483647}
         (get-in (discover-catalog test-db-config)
                 [:streams "character_strings" :schema :properties "varchar_max"]))))

(deftest ^:integration verify-binary-strings
  (is (= {:type "string"
          :minLength 1
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "binary_strings" :schema :properties "binary"])))
  (is (= {:type "string"
          :minLength 1
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "binary_strings" :schema :properties "binary_one"])))
  (is (= {:type "string"
          :minLength 10
          :maxLength 10}
         (get-in (discover-catalog test-db-config)
                 [:streams "binary_strings" :schema :properties "binary_10"])))
  (is (= {:type "string"
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "binary_strings" :schema :properties "varbinary"])))
  (is (= {:type "string"
          :maxLength 1}
         (get-in (discover-catalog test-db-config)
                 [:streams "binary_strings" :schema :properties "varbinary_one"])))
  (is (= {:type "string"
          :maxLength 2147483647}
         (get-in (discover-catalog test-db-config)
                 [:streams "binary_strings" :schema :properties "varbinary_max"]))))

(comment
  (map select-keys (get-columns test-db-config) (repeat [:column_name :type_name :sql_data_type]))
  (jdbc/with-db-metadata [md (assoc (config->conn-map test-db-config)
                                    :dbname "another_database_with_a_table")]
    (jdbc/metadata-result (.getColumns md nil "dbo" nil nil)))

  (clojure.test/run-tests *ns*)
  )
