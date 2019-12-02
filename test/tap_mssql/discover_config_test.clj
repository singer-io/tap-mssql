(ns tap-mssql.discover-config-test
  (:require [clojure.test :refer [is deftest use-fixtures]]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.set :as set]
            [clojure.string :as string]
            [tap-mssql.core :refer :all]
            [tap-mssql.catalog :as catalog]
            [tap-mssql.config :as config]
            [tap-mssql.singer.parse :as singer-parse]
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
    (jdbc/db-do-commands (assoc db-spec :dbname "database_with_a_table")
                         [(jdbc/create-table-ddl :empty_table [[:id "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "database_with_a_table")
                         ["CREATE VIEW empty_table_ids
                           AS
                           SELECT id FROM empty_table"])
    (jdbc/db-do-commands (assoc db-spec :dbname "another_database_with_a_table")
                         [(jdbc/create-table-ddl "another_empty_table" [[:id "int"]])])
    ;; assoc-ing dbname into the db-spec is equivalent to USE MY_DB;
    (jdbc/db-do-commands (assoc db-spec :dbname "datatyping")
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
    (is (thrown-with-msg? java.lang.Exception
                          #"Empty Catalog: did not discover any streams"
                          (catalog/discover specific-db-config))))
  (is (= ["datatyping"]
         (let [specific-db-config (assoc test-db-config "database" "datatyping")]
           (->> ((catalog/discover specific-db-config) "streams")
                (map (fn [[stream-name catalog-entry]]
                       (get-in catalog-entry ["metadata" "database-name"])))
                distinct)))))

(deftest ^:integration verify-nil-database-succeeds
  (let [nil-db-config (assoc test-db-config "database" nil)]
    ;; This used to throw a java.lang.IllegalArgumentException
    (is (map? (catalog/discover nil-db-config)))))

(deftest ^:integration verify-full-catalog
  (let [expected-stream-names #{"another_database_with_a_table_dbo_another_empty_table"
                                "database_with_a_table_dbo_empty_table"
                                "database_with_a_table_dbo_empty_table_ids"
                                "datatyping_dbo_approximate_numerics"
                                "datatyping_dbo_binary_strings"
                                "datatyping_dbo_character_strings"
                                "datatyping_dbo_date_and_time"
                                "datatyping_dbo_exact_numerics"
                                "datatyping_dbo_unicode_character_strings"}
        discovered-streams ((catalog/discover test-db-config) "streams")]
    (dorun
     (for [stream-name (keys discovered-streams)]
       (is (expected-stream-names stream-name))))))

(deftest ^:integration verify-system-databases-are-undiscoverable
  (is (thrown? IllegalArgumentException
               (with-redefs [singer-parse/config (constantly {"database" "master"})]
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
  ;;
  ;; Note - We are redef'ing config/->conn-map to remove the connection
  ;; check since it will try to connect using SSL and fail
  (is (= "SqlPassword"
         (with-redefs [config/check-connection (fn [conn-map] conn-map)]
           (-> test-db-config
               (assoc "ssl" "true")
               config/->conn-map*
               :authentication))))
  (is (= false
         (with-redefs [config/check-connection (fn [conn-map] conn-map)]
           (-> test-db-config
               (assoc "ssl" "true")
               config/->conn-map*
               :trustServerCertificate)))))

(deftest ^:integration verify-ssl-true-throws-on-attempted-connection
  (let [ssl-config (assoc test-db-config "ssl" "true")]
    (is (thrown-with-msg?
         com.microsoft.sqlserver.jdbc.SQLServerException
         #"The driver could not establish a secure connection to SQL Server by using Secure Sockets Layer"
         (config/->conn-map* ssl-config)))))

;; Once SSL support is implemented, the below test should be uncommented
;; so that we are testing to verify that we can correctly discover a
;; catalog with SSL enabled
;; (deftest ^:integration verify-ssl-returns-full-catalog
;;   (let [ssl-config (assoc test-db-config "ssl" "true")
;;         expected-stream-names #{"another_database_with_a_table-dbo-another_empty_table"
;;                                 "database_with_a_table-dbo-empty_table"
;;                                 "database_with_a_table-dbo-empty_table_ids"
;;                                 "datatyping-dbo-approximate_numerics"
;;                                 "datatyping-dbo-binary_strings"
;;                                 "datatyping-dbo-character_strings"
;;                                 "datatyping-dbo-date_and_time"
;;                                 "datatyping-dbo-exact_numerics"
;;                                 "datatyping-dbo-unicode_character_strings"}
;;         discovered-streams ((catalog/discover ssl-config) "streams")]
;;     (dorun
;;      (for [expected-stream-name expected-stream-names]
;;        (is (contains? discovered-streams expected-stream-name))))))
