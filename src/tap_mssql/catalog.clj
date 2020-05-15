(ns tap-mssql.catalog
  (:require [tap-mssql.config :as config]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure.java.jdbc :as jdbc]))

(def system-database-names #{"master" "tempdb" "model" "msdb" "rdsadmin"})

(defn non-system-database?
  [database]
  (-> database
      :table_cat
      system-database-names
      not))

(defn non-system-database-name?
  [config]
  (if (config "database")
    (non-system-database? {:table_cat (config "database")})
    config))

;;; Note: This is different than the serialized form of the the catalog.
;;; The catalog serialized is "streams" → [stream1 … streamN]. This will be
;;; "streams" → :streamName → stream definition and will be serialized like
;;; {"streams" (vals (get catalog "streams"))}.
(def empty-catalog {"streams" {}})

(defn config-specific-database?
  [config database]
  (if (config "database")
    (= (config "database") (:table_cat database))
    true))

(defn get-schemas-for-db
  ;; Returns a lazy seq of maps containing:
  ;;   :table_catalog (database name)
  ;;   :table_schem   (schema name)
  [config database]
  (try
    (conj (filter #(not (nil? (:table_catalog %)))
                  (jdbc/with-db-metadata [md (assoc (config/->conn-map config)
                                                    :dbname
                                                    (:table_cat database))]
                    (jdbc/metadata-result (.getSchemas md))))
          ;; Calling getSchemas does not return dbo so that's added
          ;; for each database
          {:table_catalog (:table_cat database) :table_schem "dbo"})
    (catch com.microsoft.sqlserver.jdbc.SQLServerException ex
      ;; NB: 4060 is indicative of a lack of permissions or failed login
      ;; https://docs.microsoft.com/en-us/sql/relational-databases/errors-events/database-engine-events-and-errors?view=sql-server-ver15#errors-4000-to-4999
      (when-not (= 4060 (.getErrorCode ex))
        (throw ex))

      (log/warnf "%s - Skipping due to error discovering tables: %s" (:table_cat database) (.getMessage ex))
      [])))

(defn get-databases
  [config]
  (log/info "Discovering  databases...")
  (let [conn-map (config/->conn-map config)
        databases (filter (every-pred non-system-database?
                                      (partial config-specific-database? config))
                          (jdbc/with-db-metadata [md conn-map]
                            (jdbc/metadata-result (.getCatalogs md))))]
    (log/infof "Found %s non-system databases." (count databases))
    databases))

(defn get-databases-with-schemas
  [config]
  (let [databases (get-databases config)
        schemas (flatten (map (fn [db] (get-schemas-for-db config db)) databases))]
    ;; Calling getSchemas returns :table_catalog instead of table_cat so
    ;; that key is renamed for consistency
    (map #(clojure.set/rename-keys % {:table_catalog :table_cat }) schemas)))

(defn column->tap-stream-id [column]
  ;; This used to use format and hyphens but we changed it to underscore
  ;; and when the column is missing a key format would use "null"
  (string/join "_" [(:table_cat column "null") (:table_schem column "null") (:table_name column "null")]))

(defn column->catalog-entry
  [column]
  {"stream"        (:table_name column)
   "tap_stream_id" (column->tap-stream-id column)
   "table_name"    (:table_name column)
   "schema"        {"type" "object"}
   "metadata"      {"database-name"        (:table_cat column)
                    "schema-name"          (:table_schem column)
                    "table-key-properties" #{}
                    "is-view"              (:is-view? column)
                    "row-count"            (:approximate-row-count column)}})

(defn maybe-add-nullable-to-column-schema [column-schema column]
  (if (and column-schema
           (not (:primary-key? column)))
    (update column-schema "type" conj "null")
    column-schema))

(defn maybe-add-precision-to-numerics-column-schema [column-schema column]
  {:pre [(map? column)]}
  (let [sql-type (:type_name column)
        precision (:column_size column)
        scale (:decimal_digits column)]
    (if (contains? #{"numeric" "decimal"} sql-type)
      (-> column-schema
          (assoc "multipleOf" (* 1 (Math/pow 10 (- scale))))
          (assoc "minimum" (* -1 (Math/pow 10 (- precision scale))))
          (assoc "maximum" (Math/pow 10 (- precision scale)))
          (assoc "exclusiveMinimum" true)
          (assoc "exclusiveMaximum" true))
      column-schema)))

(defn type-name->type-name-lookup
  [type_name]
  (let [clean-identity (fn [t]
                           (if (string/ends-with? t "identity")
                             (string/trim (string/replace t #"identity$" ""))
                             t))
        ;; Numeric Identity columns come back as "<type_name>() identity"
        clean-ending-parens (fn [t]
                                (if (string/ends-with? t "()")
                                  (string/trim (string/replace t #"\(\)$" ""))
                                  t))]
    (-> type_name
        clean-identity
        clean-ending-parens)))

(defn column->schema
  [{:keys [type_name] :as column}]
  (let [type-name-lookup (type-name->type-name-lookup type_name)
        column-schema    ({"int"              {"type"    ["integer"]
                                               "minimum" -2147483648
                                               "maximum" 2147483647}
                           "bigint"           {"type"    ["integer"]
                                               "minimum" -9223372036854775808
                                               "maximum" 9223372036854775807}
                           "smallint"         {"type"    ["integer"]
                                               "minimum" -32768
                                               "maximum" 32767}
                           "tinyint"          {"type"    ["integer"]
                                               "minimum" 0
                                               "maximum" 255}
                           "float"            {"type" ["number"]}
                           "real"             {"type" ["number"]}
                           "bit"              {"type" ["boolean"]}
                           "decimal"          {"type" ["number"]}
                           "money"            {"type" ["number"]
                                               "minimum" -922337203685477.5808
                                               "maximum" 922337203685477.5807
                                               "multipleOf" 0.0001}
                           "smallmoney"       {"type" ["number"]
                                               "minimum" -214748.3648
                                               "maximum" 214748.3647
                                               "multipleOf" 0.0001}
                           "numeric"          {"type" ["number"]}
                           "date"             {"type"   ["string"]
                                               "format" "date-time"}
                           "time"             {"type" ["string"]}
                           "datetime"         {"type"   ["string"]
                                               "format" "date-time"}
                           "smalldatetime"    {"type"   ["string"]
                                               "format" "date-time"}
                           "datetimeoffset"   {"type"   ["string"]
                                               "format" "date-time"}
                           "datetime2"        {"type"   ["string"]
                                               "format" "date-time"}
                           "char"             {"type"      ["string"]
                                               "maxLength" (:column_size column)}
                           "nchar"            {"type"      ["string"]
                                               "maxLength" (:column_size column)}
                           "varchar"          {"type"      ["string"]
                                               "maxLength" (:column_size column)}
                           "nvarchar"         {"type"      ["string"]
                                               "maxLength" (:column_size column)}
                           "binary"           {"type"      ["string"]}
                           "varbinary"        {"type"      ["string"]}
                           "uniqueidentifier" {"type"    ["string"]
                                               ;; a string constant in the form
                                               ;; xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, in which
                                               ;; each x is a hexadecimal digit in the range 0-9
                                               ;; or a-f. For example,
                                               ;; 6F9619FF-8B86-D011-B42D-00C04FC964FF is a valid
                                               ;; uniqueidentifier value.
                                               ;;
                                               ;; https://docs.microsoft.com/en-us/sql/t-sql/data-types/uniqueidentifier-transact-sql?view=sql-server-2017
                                               "pattern" "[A-F0-9]{8}-([A-F0-9]{4}-){3}[A-F0-9]{12}"}
                           ;; timestamp is a synonym for rowversion, which is automatically
                           ;; generated and guaranteed to be unique. It is _not_ a datetime
                           ;; https://docs.microsoft.com/en-us/sql/t-sql/data-types/rowversion-transact-sql?view=sql-server-2017
                           "timestamp"        {"type" ["string"] }}
                          type-name-lookup)]
    (-> column-schema
        (maybe-add-nullable-to-column-schema column)
        (maybe-add-precision-to-numerics-column-schema column))))

(defn add-column-schema-to-catalog-stream-schema
  [catalog-stream-schema column]
  (update-in catalog-stream-schema ["properties" (:column_name column)]
             merge
             (column->schema column)))

(defn column->metadata
  [column]
  {"inclusion"           (if (:unsupported? column)
                           "unsupported"
                           (if (:primary-key? column)
                             "automatic"
                             "available"))
   "sql-datatype"        (:type_name column)
   "selected-by-default" (not (:unsupported? column))})

(defn add-column-schema-to-catalog-stream-metadata
  [catalog-stream-metadata column]
  (update-in catalog-stream-metadata ["properties" (:column_name column)]
             merge
             (column->metadata column)))

(defn add-column-to-primary-keys
  [catalog-stream column]
  (if (:primary-key? column)
    (update-in catalog-stream ["metadata" "table-key-properties"] conj (:column_name column))
    catalog-stream))

(defn add-column-to-stream
  [catalog-stream column]
  (-> (or catalog-stream (column->catalog-entry column))
      (add-column-to-primary-keys column)
      (update "schema" add-column-schema-to-catalog-stream-schema column)
      (update "metadata" add-column-schema-to-catalog-stream-metadata column)))

(defn add-column
  [catalog column]
  (update-in catalog ["streams" (column->tap-stream-id column)]
             add-column-to-stream
             column))

(defn get-table-names [conn-map]
  (map :table_name (jdbc/query conn-map ["SELECT table_name FROM INFORMATION_SCHEMA.TABLES"])))

(defn get-database-raw-columns
  [conn-map database]
  (log/infof "Discovering columns and tables for database: %s" (:table_cat database))
  (let [columns (jdbc/with-db-metadata [md conn-map]
                  (jdbc/metadata-result (.getColumns md (:table_cat database) (:table_schem database) nil nil)))
        table-names (set (get-table-names conn-map))]
    (filter (comp (partial contains? table-names)
                  :table_name)
            columns)))

(defn get-primary-keys
  [conn-map]
  (let [sql-query (str "SELECT kcu.table_name as table_name, kcu.column_name as primary_key, kcu.table_schema as table_schema, kcu.table_catalog as table_catalog "
                       "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc "
                       "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME")]
    (log/infof "Executing query: %s" sql-query)
    (jdbc/query conn-map [sql-query])))

(defn add-primary-key?-data
  [primary-key-data column]
  (let [key-format (format "%s.%s.%s" (:table_cat column) (:table_schem column) (:table_name column))
        pk-row (get primary-key-data key-format)]
    (assoc column :primary-key? (true? (some #(= (:column_name column) (:primary_key %)) pk-row)))))

(defn get-column-database-view-names*
  [conn-map table_cat table_schem]
  (jdbc/with-db-metadata [md conn-map]
    (->> (.getTables md table_cat table_schem nil (into-array ["VIEW"]))
         jdbc/metadata-result
         (map :table_name)
         (into #{}))))

;;; Not memoizing this proves to have prohibitively bad performance
;;; characteristics.
(def get-column-database-view-names (memoize get-column-database-view-names*))

(defn add-is-view?-data
  [conn-map column]
  (let [view-names (get-column-database-view-names conn-map (:table_cat column) (:table_schem column))]
    (assoc column :is-view? (if (view-names (:table_name column))
                              ;; Want to be explicit rather than punning
                              ;; here so that we're sure we serialize
                              ;; properly
                              true
                              false))))

(defn add-unsupported?-data
  [column]
  (if (nil? (column->schema column))
    (assoc column :unsupported? true)
    column))

(defn get-approximate-row-count
  [conn-map]
  (let [sql-query (str  "SELECT tbl.name as table_name, SCHEMA_NAME(tbl.schema_id) as schema_name, CAST(p.rows AS bigint) as row_count "
                    "FROM sys.tables AS tbl "
                    "INNER JOIN sys.indexes AS idx ON idx.object_id = tbl.object_id and idx.index_id < 2 "
                    "INNER JOIN sys.partitions AS p ON p.object_id=CAST(tbl.object_id AS int) "
                    "AND p.index_id=idx.index_id")]
    (log/infof "Executing query: %s" sql-query)
    (jdbc/query conn-map [sql-query])))

(defn add-row-count-data
  [row-count-data column]
  (if (:is-view? column)
    ;; a view's count can only be done via count(*) which causes a table scan so just return 0
    (assoc column :approximate-row-count 0)
    (let [row-count-key (format "%s.%s" (:table_schem column) (:table_name column))
          approximate-row-count (-> (get row-count-data row-count-key)
                                    first
                                    :row_count)]
      (assoc column :approximate-row-count approximate-row-count))))

(defn get-database-columns
  [config database]
  (let [conn-map (assoc (config/->conn-map config)
                        :dbname
                        (:table_cat database))
        raw-columns (get-database-raw-columns conn-map database)
        ;; group by "schema_name.table_name" so we can look it up
        row-count-data (->> (get-approximate-row-count conn-map)
                            (group-by #(format "%s.%s" (:schema_name %) (:table_name %))))
        ;; group by "database.schema.table" so we can look it up
        primary-key-data (->> (get-primary-keys conn-map)
                              (group-by #(format "%s.%s.%s" (:table_catalog %) (:table_schema %) (:table_name %))))]
    (->> raw-columns
         (map (partial add-primary-key?-data primary-key-data))
         (map (partial add-is-view?-data conn-map))
         (map (partial add-row-count-data row-count-data))
         (map add-unsupported?-data))))

(defn get-columns
  [config]
  (flatten (map (partial get-database-columns config) (get-databases-with-schemas config))))

(defn discover
  [config]
  (jdbc/with-db-metadata [metadata (config/->conn-map config)]
    (log/infof "Connecting to %s version %s"
               (.getDatabaseProductName metadata)
               (.getDatabaseProductVersion metadata)))
  ;; It's important to keep add-column pure and keep all database
  ;; interaction in get-columns for testability
  (let [the-catalog (reduce add-column empty-catalog (get-columns config))]
    (if (empty? (the-catalog "streams"))
      (throw (ex-info "Empty Catalog: did not discover any streams" {}))
      the-catalog)))

(defn serialize-stream-metadata-property
  [[stream-metadata-property-name stream-metadata-property-metadata :as stream-metadata-property]]
  {"metadata" stream-metadata-property-metadata
   "breadcrumb" ["properties" stream-metadata-property-name]})

(defn serialize-stream-metadata-properties
  [stream-metadata-properties]
  (let [properties (stream-metadata-properties "properties")]
    (concat [{"metadata" (dissoc stream-metadata-properties "properties")
              "breadcrumb" []}]
            (map serialize-stream-metadata-property properties))))

(defn serialize-stream-metadata
  [{:keys [metadata] :as stream}]
  (update stream "metadata" serialize-stream-metadata-properties))

(defn serialize-metadata
  [catalog]
  (update catalog "streams" (partial map serialize-stream-metadata)))

(defn serialize-stream-schema-property
  [[k v]]
  (if (nil? v)
    [k {}]
    [k v]))

(defn serialize-stream-schema-properties
  [stream-schema-properties]
  (into {} (map serialize-stream-schema-property
                stream-schema-properties)))

(defn serialize-stream-schema
  [stream-schema]
  (update stream-schema
          "properties"
          serialize-stream-schema-properties))

(defn serialize-stream
  [stream-catalog-entry]
  (update stream-catalog-entry "schema"
          serialize-stream-schema))

(defn serialize-streams
  [catalog]
  (update catalog
          "streams"
          (comp (partial map serialize-stream)
                vals)))

(defn ->serialized-catalog
  [catalog]
  (-> catalog
      serialize-streams
      serialize-metadata))
