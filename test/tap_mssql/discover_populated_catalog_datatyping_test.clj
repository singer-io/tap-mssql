(ns tap-mssql.discover-populated-catalog-datatyping-test
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
    (jdbc/db-do-commands db-spec ["CREATE DATABASE empty_database"
                                  "CREATE DATABASE database_with_a_table"
                                  "CREATE DATABASE another_database_with_a_table"
                                  "CREATE DATABASE datatyping"])
    ;; assoc db-spec :dbname … is essentially USE database_with_a_table
    ;; for the enclosed commands
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
                                                 [[:bigint "bigint not null"]
                                                  [:int "int"]
                                                  [:smallint "smallint"]
                                                  [:tinyint "tinyint"]
                                                  [:bit "bit"]
                                                  [:decimal "decimal"]
                                                  [:money "money"]
                                                  [:smallmoney "smallmoney"]
                                                  [:numeric "numeric"]
                                                  [:numeric_9_3 "numeric(9,3)"]
                                                  [:numeric_19_8 "numeric(19,8)"]
                                                  [:numeric_28_1 "numeric(28,1)"]
                                                  [:numeric_38_22 "numeric(38,22)"]])
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
                                                  [:varbinary_max "varbinary(max)"]])
                          (jdbc/create-table-ddl :date_and_time
                                                 ;; https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-2017#date-and-time
                                                 [[:date "date"]
                                                  [:time "time"]
                                                  [:datetime "datetime"]
                                                  [:datetime2 "datetime2"]
                                                  [:datetimeoffset "datetimeoffset"]
                                                  [:smalldatetime "smalldatetime"]])
                          (jdbc/create-table-ddl :uniqueidentifiers
                                                 [[:uniqueidentifier "uniqueidentifier"]])
                          (jdbc/create-table-ddl :timestamps
                                                 ;; timestamp is a synonym for rowversion,
                                                 ;; neither of which are analogous to the ISO
                                                 ;; Standard timestamp type.
                                                 ;;
                                                 ;; https://docs.microsoft.com/en-us/sql/t-sql/data-types/rowversion-transact-sql?view=sql-server-2017
                                                 [[:timestamp "timestamp"]])
                          (jdbc/create-table-ddl :rowversions
                                                 [[:rowversion "rowversion"]])
                          ;; Identity types of non-int types come back like 'numeric() identity'
                          ;; Ensure these are supported
                          (jdbc/create-table-ddl :bigint_identity
                                                 [[:bigint_identity "bigint identity"]])
                          (jdbc/create-table-ddl :int_identity
                                                 [[:int_identity "int identity"]])
                          (jdbc/create-table-ddl :smallint_identity
                                                 [[:smallint_identity "smallint identity"]])
                          (jdbc/create-table-ddl :tinyint_identity
                                                 [[:tinyint_identity "tinyint identity"]])
                          (jdbc/create-table-ddl :numeric_identity
                                                 [[:numeric_identity "numeric identity"]])
                          (jdbc/create-table-ddl :numeric_3_0_identity
                                                 [[:numeric_3_0_identity "numeric(3,0) identity"]])
                          (jdbc/create-table-ddl :decimal_identity
                                                 [[:decimal_identity "decimal identity"]])])))

(defn test-db-fixture [f]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db)
    (create-test-db)
    (f)))

(use-fixtures :each test-db-fixture)

(deftest ^:integration verify-date-and-time
  (is (= {"type" ["string" "null"]
          "format" "date-time"}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_date_and_time" "schema" "properties" "date"])))
  (is (= {"type" ["string" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_date_and_time" "schema" "properties" "time"])))
  (is (= {"type" ["string" "null"]
          "format" "date-time"}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_date_and_time" "schema" "properties" "datetime"])))
  (is (= {"type" ["string" "null"]
          "format" "date-time"}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_date_and_time" "schema" "properties" "datetime2"])))
  (is (= {"type" ["string" "null"]
          "format" "date-time"}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_date_and_time" "schema" "properties" "datetimeoffset"])))
  (is (= {"type" ["string" "null"]
          "format" "date-time"}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_date_and_time" "schema" "properties" "smalldatetime"]))))

(deftest ^:integration verify-approximate-numerics
  (is (= {"type" ["number" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_approximate_numerics" "schema" "properties" "float"])))
  (is (= {"type" ["number" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_approximate_numerics" "schema" "properties" "float_1"])))
  (is (= {"type" ["number" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_approximate_numerics" "schema" "properties" "float_24"])))
  (is (= {"type" ["number" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_approximate_numerics" "schema" "properties" "float_25"])))
  (is (= {"type" ["number" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_approximate_numerics" "schema" "properties" "float_53"])))
  (is (= {"type" ["number" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_approximate_numerics" "schema" "properties" "double_precision"])))
  (is (= {"type" ["number" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_approximate_numerics" "schema" "properties" "real"]))))

(deftest ^:integration verify-unicode-strings
  (is (= {"type" ["string" "null"]
          "maxLength" 1}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_unicode_character_strings" "schema" "properties" "nchar"])))
  (is (= {"type" ["string" "null"]
          "maxLength" 1}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_unicode_character_strings" "schema" "properties" "nchar_1"])))
  (is (= {"type" ["string" "null"]
          "maxLength" 4000}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_unicode_character_strings" "schema" "properties" "nchar_4000"])))
  (is (= {"type" ["string" "null"]
          "maxLength" 1}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_unicode_character_strings" "schema" "properties" "nvarchar"])))
  (is (= {"type" ["string" "null"]
          "maxLength" 1}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_unicode_character_strings" "schema" "properties" "nvarchar_1"])))
  (is (= {"type" ["string" "null"]
          "maxLength" 4000}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_unicode_character_strings" "schema" "properties" "nvarchar_4000"])))
  (is (= {"type" ["string" "null"]
          "maxLength" 2147483647}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_unicode_character_strings" "schema" "properties" "nvarchar_max"]))))

(deftest ^:integration verify-exact-numerics
  (is (= {"type" ["integer" "null"]
          "minimum" -2147483648
          "maximum"  2147483647}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "int"])))
  (is (= {"type" ["integer" "null"]
          "minimum" -9223372036854775808
          "maximum"  9223372036854775807}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "bigint"])))
  (is (= {"type" ["integer" "null"]
          "minimum" -32768
          "maximum"  32767}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "smallint"])))
  (is (= {"type" ["integer" "null"]
          "minimum" 0
          "maximum" 255}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "tinyint"])))
  (is (= {"type" ["boolean" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "bit"])))
  (is (= {"type" ["number" "null"],
          "multipleOf" 1.0,
          "minimum" -1.0E18,
          "maximum" 1.0E18,
          "exclusiveMinimum" true,
          "exclusiveMaximum" true}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "decimal"])))
  (is (= {"type" ["number" "null"],
          "multipleOf" 0.0001,
          "minimum" -922337203685477.5808
          "maximum" 922337203685477.5807}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "money"])))
  (is (= {"type" ["number" "null"],
          "minimum" -214748.3648
          "maximum" 214748.3647
          "multipleOf" 0.0001}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "smallmoney"])))
  (is (= {"type" ["number" "null"],
          "multipleOf" 1.0,
          "minimum" -1.0E18,
          "maximum" 1.0E18,
          "exclusiveMinimum" true,
          "exclusiveMaximum" true}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "numeric"])))
  (is (= {"type" ["number" "null"],
          "multipleOf" 0.001,
          "minimum" -1000000.0,
          "maximum" 1000000.0,
          "exclusiveMinimum" true,
          "exclusiveMaximum" true}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "numeric_9_3"])))
  (is (= {"type" ["number" "null"],
          "multipleOf" 1.0E-8,
          "minimum" -1.0E11,
          "maximum" 1.0E11,
          "exclusiveMinimum" true,
          "exclusiveMaximum" true}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "numeric_19_8"])))
  (is (= {"type" ["number" "null"],
          "multipleOf" 0.1,
          "minimum" -1.0E27,
          "maximum" 1.0E27,
          "exclusiveMinimum" true,
          "exclusiveMaximum" true}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "numeric_28_1"])))
  (is (= {"type" ["number" "null"],
          "multipleOf" 1.0E-22,
          "minimum" -1.0E16,
          "maximum" 1.0E16,
          "exclusiveMinimum" true,
          "exclusiveMaximum" true}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_exact_numerics" "schema" "properties" "numeric_38_22"]))))

(deftest ^:integration verify-character-strings
  (is (= {"type" ["string" "null"]
          "maxLength" 1}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_character_strings" "schema" "properties" "char"])))
  (is (= {"type" ["string" "null"]
          "maxLength" 1}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_character_strings" "schema" "properties" "char_one"])))
  (is (= {"type" ["string" "null"]
          "maxLength" 8000}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_character_strings" "schema" "properties" "char_8000"])))
  (is (= {"type" ["string" "null"]
          "maxLength" 1}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_character_strings" "schema" "properties" "varchar"])))
  (is (= {"type" ["string" "null"]
          "maxLength" 1}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_character_strings" "schema" "properties" "varchar_one"])))
  (is (= {"type" ["string" "null"]
          "maxLength" 8000}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_character_strings" "schema" "properties" "varchar_8000"])))
  (is (= {"type" ["string" "null"]
          "maxLength" 2147483647}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_character_strings" "schema" "properties" "varchar_max"]))))

(deftest ^:integration verify-binary-strings
  ;; NB: Because we convert binary strings to hex, max_length is not accurate from the DB
  ;; - Thus, it should not be included in the schemas
  (is (= {"type" ["string" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_binary_strings" "schema" "properties" "binary"])))
  (is (= {"type" ["string" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_binary_strings" "schema" "properties" "binary_one"])))
  (is (= {"type" ["string" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_binary_strings" "schema" "properties" "binary_10"])))
  (is (= {"type" ["string" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_binary_strings" "schema" "properties" "varbinary"])))
  (is (= {"type" ["string" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_binary_strings" "schema" "properties" "varbinary_one"])))
  (is (= {"type" ["string" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_binary_strings" "schema" "properties" "varbinary_max"]))))

(deftest ^:integration verify-uniqueidentifiers-are-supported
  (is (= {"type" ["string" "null"]
          "pattern" "[A-F0-9]{8}-([A-F0-9]{4}-){3}[A-F0-9]{12}"}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_uniqueidentifiers" "schema" "properties" "uniqueidentifier"])))
  (is (= "uniqueidentifier"
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_uniqueidentifiers" "metadata" "properties"
                  "uniqueidentifier" "sql-datatype"])))
  (is (= "available"
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_uniqueidentifiers" "metadata" "properties"
                  "uniqueidentifier" "inclusion"])))
  (is (= true
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_uniqueidentifiers" "metadata" "properties"
                  "uniqueidentifier" "selected-by-default"]))))

(deftest ^:integration verify-timestamps-are-supported
  (is (= {"type" ["string" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_timestamps" "schema" "properties" "timestamp"])))
  (is (= "timestamp"
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_timestamps" "metadata" "properties"
                  "timestamp" "sql-datatype"])))
  (is (= "available"
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_timestamps" "metadata" "properties"
                  "timestamp" "inclusion"])))
  (is (= true
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_timestamps" "metadata" "properties"
                  "timestamp" "selected-by-default"]))))

(deftest ^:integration verify-rowversions-are-supported
  (is (= {"type" ["string" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_rowversions" "schema" "properties" "rowversion"])))
  (is (= "timestamp" ;; rowversion is an alias for timestamp
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_rowversions" "metadata" "properties"
                  "rowversion" "sql-datatype"])))
  (is (= "available"
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_rowversions" "metadata" "properties"
                  "rowversion" "inclusion"])))
  (is (= true
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_rowversions" "metadata" "properties"
                  "rowversion" "selected-by-default"]))))

(deftest ^:integration verify-all-identity-types-are-supported
  ;; "integer" types
  (is (= {"type" ["integer" "null"]
          "minimum" -9223372036854775808
          "maximum" 9223372036854775807}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_bigint_identity" "schema" "properties" "bigint_identity"])))
  (is (= {"type" ["integer" "null"]
          "minimum" -2147483648
          "maximum" 2147483647}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_int_identity" "schema" "properties" "int_identity"])))
  (is (= {"type" ["integer" "null"]
          "minimum" -32768
          "maximum" 32767}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_smallint_identity" "schema" "properties" "smallint_identity"])))
  (is (= {"type" ["integer" "null"]
          "minimum" 0
          "maximum" 255}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_tinyint_identity" "schema" "properties" "tinyint_identity"])))
  ;; "numeric" types
  (is (= {"type" ["number" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_numeric_identity" "schema" "properties" "numeric_identity"])))
  (is (= {"type" ["number" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_numeric_3_0_identity" "schema" "properties" "numeric_3_0_identity"])))
  (is (= {"type" ["number" "null"]}
         (get-in (catalog/discover test-db-config)
                 ["streams" "datatyping_dbo_decimal_identity" "schema" "properties" "decimal_identity"]))))

(comment
  (map select-keys
       (get-columns test-db-config)
       (repeat ["column_name" "type_name" "sql_data_type"]))
  (jdbc/with-db-metadata [md (assoc (config/->conn-map test-db-config)
                                    "dbname" "another_database_with_a_table")]
    (jdbc/metadata-result (.getColumns md nil "dbo" nil nil)))

  (clojure.test/run-tests *ns*)
  )
