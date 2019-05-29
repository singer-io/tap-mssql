(ns tap-mssql.discover-config-test
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
    (jdbc/db-do-commands (assoc db-spec :dbname "datatyping") ;; This is scopethe command within the database, e.g USE MY_DB;
                         [(jdbc/create-table-ddl :exact_numerics
                                                 ;; https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-2017#exact-numerics
                                                 [[:bigint "bigint"]
                                                  [:int "int"]
                                                  [:smallint "smallint"]
                                                  [:tinyint "tinyint"]
                                                  [:bit "bit"]
                                                  [:decimal "decimal"]
                                                  [:numeric "numeric"]])
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
                                                  [:time "time"]])])))

(defn test-db-fixture [f]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db)
    (create-test-db)
    (f)))

(use-fixtures :each test-db-fixture)

(deftest ^:integration verify-database-config-limits-catalog
  (let [specific-db-config (assoc test-db-config "database" "empty_database")]
    (is (= {:streams {}}
           (discover-catalog specific-db-config))))
  (is (= ["datatyping"]
         (let [specific-db-config (assoc test-db-config "database" "datatyping")]
           (->> (:streams (discover-catalog specific-db-config))
                (map (fn [[stream-name catalog-entry]]
                       (get-in catalog-entry [:metadata :database-name])))
                distinct)))))

(deftest ^:integration verify-full-catalog
  (let [expected-stream-names #{"another_empty_table"
                                "empty_table"
                                "empty_table_ids"
                                "approximate_numerics"
                                "binary_strings"
                                "character_strings"
                                "date_and_time"
                                "exact_numerics"
                                "unicode_character_strings"}
        discovered-streams (:streams (discover-catalog test-db-config))]
    (dorun
     (for [stream-name (keys discovered-streams)]
       (is (expected-stream-names stream-name))))))

(deftest ^:integration verify-system-databases-are-undiscoverable
  (is (thrown? IllegalArgumentException
               (with-redefs [parse-config (constantly {"database" "master"})]
                 (parse-opts ["--config" "foobar.json"])))))

(deftest ^:integration verify-ssl-activates-ssl-properties-on-conn-map
  ;; It would be best to somehow verify that we are _actually_ connected
  ;; over SSL rather than just that we intend to be connected over SSL. So
  ;; far we haven't found a good way to do this though.
  ;;
  ;; We've tried:
  ;;
  ;; (jdbc/query conn-map
  ;;             ["SELECT session_id, encrypt_option
  ;;               FROM sys.dm_exec_connections
  ;;               WHERE session_id = @@SPID"])
  ;;
  ;; This option works, returning "TRUE" for the `encrypt_option`, but the
  ;; stored procedure relied upon to get the information is not exposed to
  ;; non-admin users which we will almost always be.
  ;;
  ;; https://docs.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-connections-transact-sql?view=sql-server-2017
  ;;
  ;; (jdbc/query conn-map
  ;;             ["SELECT ConnectionProperty('protocol_type')"])
  ;;
  ;; This option shows some promise in [documentation][2], but we haven't
  ;; been able to get it to return anything but "TCP".
  ;;
  ;; [2]: https://docs.microsoft.com/en-us/sql/t-sql/functions/connectionproperty-transact-sql?view=sql-server-2017
  ;;
  ;; For now, we'll stick with verifying that we've requested
  ;; authentication.
  (is (= "SqlPassword"
         (-> test-db-config
             (assoc "ssl" "true")
             config->conn-map
             :authentication)))
  (is (= true
         (-> test-db-config
             (assoc "ssl" "true")
             config->conn-map
             :trustServerCertificate))))

(deftest ^:integration verify-ssl-returns-full-catalog
  (let [ssl-config (assoc test-db-config "ssl" "true")
        expected-stream-names #{"another_empty_table"
                                "empty_table"
                                "empty_table_ids"
                                "approximate_numerics"
                                "binary_strings"
                                "character_strings"
                                "date_and_time"
                                "exact_numerics"
                                "unicode_character_strings"}
        discovered-streams (:streams (discover-catalog ssl-config))]
    (dorun
     (for [expected-stream-name expected-stream-names]
       (is (contains? discovered-streams expected-stream-name))))))

