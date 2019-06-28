(ns tap-mssql.core
  (:require [clojure.tools.logging :as log]
            [clojure.tools.nrepl.server :as nrepl-server]
            [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.set]
            [clojure.data.json :as json]
            [clojure.java.jdbc :as jdbc])
  (:import [com.microsoft.sqlserver.jdbc SQLServerException])
  (:gen-class))


;;; Note: This is different than the serialized form of the the catalog.
;;; The catalog serialized is "streams" → [stream1 … streamN]. This will be
;;; "streams" → :streamName → stream definition and will be serialized like
;;; {"streams" (vals (get catalog "streams"))}.
(def empty-catalog {"streams" {}})

(defn check-connection [conn-map]
  (do (jdbc/with-db-metadata [md conn-map]
        (jdbc/metadata-result (.getCatalogs md)))
      (log/info "Successfully connected to the instance")
      conn-map))

(defn config->conn-map*
  [config]
  (let [conn-map {:dbtype "sqlserver"
                  :dbname (config "database" "")
                  :host (config "host")
                  :port (config "port")
                  :password (config "password")
                  :user (config "user")}
        conn-map (if (= "true" (config "ssl"))
                   ;; TODO: The only way I can get a test failure is by
                   ;; changing the code to say ":trustServerCertificate
                   ;; false". In which case, truststores need to be
                   ;; specified. This is for the "correct" way of doing
                   ;; things, where we are validating SSL, but for now,
                   ;; leaving the certificate unverified should work.
                   (assoc conn-map
                          ;; Based on the [docs][1], we believe thet
                          ;; setting `authentication` to anything but
                          ;; `NotSpecified` (the default) activates SSL
                          ;; for the connection and have verified that by
                          ;; setting `trustServerCertificate` to `false`
                          ;; with `authentication` set to `SqlPassword`
                          ;; and observing SSL handshake errors. Because
                          ;; of this, we don't believe it's necessary to
                          ;; set `encrypt` to `true` as it used to be
                          ;; prior to Driver version 6.0.
                          ;;
                          ;; [1]: https://docs.microsoft.com/en-us/sql/connect/jdbc/setting-the-connection-properties?view=sql-server-2017
                          :authentication "SqlPassword"
                          :trustServerCertificate false)
                   conn-map)]
    ;; returns conn-map and logs on successful connection
    (check-connection conn-map)))

(def config->conn-map (memoize config->conn-map*))

(def system-database-names #{"master" "tempdb" "model" "msdb" "rdsadmin"})

(defn non-system-database?
  [database]
  (-> database
      :table_cat
      system-database-names
      not))

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
  (conj (filter #(not (nil? (:table_catalog %)))
                (jdbc/with-db-metadata [md (assoc (config->conn-map config)
                                                  :dbname
                                                  (:table_cat database))]
                  (jdbc/metadata-result (.getSchemas md))))
        ;; Calling getSchemas does not return dbo so that's added
        ;; for each database
        {:table_catalog (:table_cat database) :table_schem "dbo"}))

(defn get-databases
  [config]
  (log/info "Discovering  databases...")
  (let [conn-map (config->conn-map config)
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
  (format "%s-%s-%s"
          (:table_cat column)
          (:table_schem column)
          (:table_name column)))

(defn column->catalog-entry
  [column]
  {"stream"        (:table_name column)
   "tap_stream_id" (column->tap-stream-id column)
   "table_name"    (:table_name column)
   "schema"        {"type" "object"}
   "metadata"      {"database-name"        (:table_cat column)
                    "schema-name"          (:table_schem column)
                    "table-key-properties" #{}
                    "is-view"              (:is-view? column)}})

(defn maybe-add-nullable-to-column-schema [column-schema column]
  (if (and column-schema
           (= "YES" (:is_nullable column)))
    (update column-schema "type" conj "null")
    column-schema))

(defn maybe-add-precision-to-column-schema [column-schema column]
  {:pre [(map? column)]}
  (if (and column-schema
           (not (nil? (:decimal_digits column)))
           ;; Datetime columns have a precision, not a candidate for multipleOf
           (not (= "date-time" (column-schema "format")))
           (> (:decimal_digits column) 0))
    (assoc column-schema "multipleOf" (* 1 (Math/pow 10 (- (:decimal_digits column)))))
    column-schema))

(defn column->schema
  [{:keys [type_name] :as column}]
  (let [column-schema
        ({"int"              {"type"    ["integer"]
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
          "numeric"          {"type" ["number"]}
          "date"             {"type"   ["string"]
                              "format" "date-time"}
          "time"             {"type"   ["string"]
                              "format" "date-time"}
          "datetime"         {"type"   ["string"]
                              "format" "date-time"}
          "char"             {"type"      ["string"]
                              "minLength" (:column_size column)
                              "maxLength" (:column_size column)}
          "nchar"            {"type"      ["string"]
                              "minLength" (:column_size column)
                              "maxLength" (:column_size column)}
          "varchar"          {"type"      ["string"]
                              "minLength" 0
                              "maxLength" (:column_size column)}
          "nvarchar"         {"type"      ["string"]
                              "minLength" 0
                              "maxLength" (:column_size column)}
          "binary"           {"type"      ["string"]
                              "minLength" (:column_size column)
                              "maxLength" (:column_size column)}
          "varbinary"        {"type"      ["string"]
                              "maxLength" (:column_size column)}
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
          "timestamp"         {"type" ["string"] }}
         type_name)]
    (-> column-schema
        (maybe-add-nullable-to-column-schema column)
        (maybe-add-precision-to-column-schema column))))

(defn add-column-schema-to-catalog-stream-schema
  [catalog-stream-schema column]
  (update-in catalog-stream-schema ["properties" (:column_name column)]
             merge
             (column->schema column)))

(defn column->table-primary-keys*
  [conn-map table_cat table_schem table_name]
  (jdbc/with-db-metadata [md conn-map]
    (->> (.getPrimaryKeys md table_cat table_schem table_name)
         jdbc/metadata-result
         (map :column_name)
         (into #{}))))

;;; Not memoizing this proves to have prohibitively bad performance
;;; characteristics.
(def column->table-primary-keys (memoize column->table-primary-keys*))

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
  (if (and (not (:unsupported? column)) (:primary-key? column))
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

(defn get-database-raw-columns
  [conn-map database]
  (log/infof "Discovering columns and tables for database: %s" (:table_cat database))
  (jdbc/with-db-metadata [md conn-map]
    (jdbc/metadata-result (.getColumns md (:table_cat database) (:table_schem database) nil nil))))

(defn add-primary-key?-data
  [conn-map column]
  (let [primary-keys (column->table-primary-keys conn-map
                                                 (:table_cat column)
                                                 (:table_schem column)
                                                 (:table_name column))]
    (assoc column :primary-key? (primary-keys (:column_name column)))))

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

(defn get-database-columns
  [config database]
  (let [conn-map (assoc (config->conn-map config)
                        :dbname
                        (:table_cat database))
        raw-columns (get-database-raw-columns conn-map database)]
    (->> raw-columns
         (map (partial add-primary-key?-data conn-map))
         (map (partial add-is-view?-data conn-map))
         (map add-unsupported?-data))))

(defn get-columns
  [config]
  (flatten (map (partial get-database-columns config) (get-databases-with-schemas config))))

(defn discover-catalog
  [config]
  (jdbc/with-db-metadata [metadata (config->conn-map config)]
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

(defn catalog->serialized-catalog
  [catalog]
  (-> catalog
      serialize-streams
      serialize-metadata))

(defn do-discovery [{:as config}]
  (log/info "Starting discovery mode")
  (-> (discover-catalog config)
      catalog->serialized-catalog
      json/write-str
      println))

(defn message-valid?
  [message]
  (and (#{"SCHEMA" "STATE" "RECORD" "ACTIVATE_VERSION"} (message "type"))
       (case (message "type")
         "SCHEMA"
         (message "schema")

         "STATE"
         (message "value")

         "RECORD"
         (message "record")

         "ACTIVATE_VERSION"
         (message "version"))))


(defn serialize-datetime [k v]
  (if (instance? java.sql.Timestamp v)
    (.. v toInstant toString)
    v)
  )

(defn write-message!
  [message]
  {:pre [(message-valid? message)]}
  (-> message
      (json/write-str :value-fn serialize-datetime)
      println))

(defn maybe-add-bookmark-properties-to-schema [schema-message catalog stream-name]
  ;; Add or don't and return message
  (let [replication-key (get-in catalog ["streams"
                                         stream-name
                                         "metadata"
                                         "replication-key"])]
    (if replication-key
      (assoc schema-message "bookmark_properties" [replication-key])
      schema-message)))


(defn maybe-add-deleted-at-to-schema [schema-message catalog stream-name]
  (if (= "LOG_BASED"(get-in catalog ["streams" stream-name "metadata" "replication-method"]))
    (assoc-in schema-message ["schema" "properties" "_sdc_deleted_at"] {"type" "string"
                                                                        "format" "date-time"})
    schema-message))

(comment
  ;; TODO: Make this into a test and make a change so that `rowversion`
  ;; shows up as `{}` insteald of `null`
  (def catalog (serialized-catalog->catalog
    (catalog->serialized-catalog
     (reduce add-column nil [{:table_name "catalog_test"
                              :column_name "id"
                              :type_name "int"
                              :primary-key? false
                              :is-view? false}
                             {:table_name "unsupported_data_types"
                              :column_name "rowversion"
                              :type_name "rowversion"
                              :is_nullable "YES"
                              :unsupported? true}]))))
  (def stream-name "null-null-unsupported_data_types")
  (with-out-str
    (write-schema! catalog stream-name))
  )

(defn make-unsupported-schemas-empty [schema-message catalog stream-name]
  (let [schema-keys (get-in catalog ["streams" stream-name "metadata" "properties"])
        unsupported-keys (map first (filter #(= "unsupported" ((second %) "inclusion"))
                                            (seq schema-keys)))] (reduce (fn [msg x] (assoc-in msg ["schema" "properties" x] {})) schema-message unsupported-keys)))

(defn write-schema! [catalog stream-name]
  ;; TODO: Make sure that unsupported values are written with an empty schema
  (-> {"type" "SCHEMA"
       "stream" stream-name
       "key_properties" (get-in catalog ["streams"
                                         stream-name
                                         "metadata"
                                         "table-key-properties"])
       "schema" (get-in catalog ["streams" stream-name "schema"])}
      (maybe-add-bookmark-properties-to-schema catalog stream-name)
      (maybe-add-deleted-at-to-schema catalog stream-name)
      (make-unsupported-schemas-empty catalog stream-name)
      write-message!))

(defn write-state!
  [stream-name state]
  (write-message! {"type" "STATE"
                   "stream" stream-name
                   "value" state})
  ;; This is very important. This function needs to return state so that
  ;; the outer reduce can pass it in to the next iteration.
  state)

(defn write-record!
  [stream-name state record]
  (let [record-message {"type" "RECORD"
                        "stream" stream-name
                        "record" record}
        version (get-in state ["bookmarks" stream-name "version"])]
    (if (nil? version)
      (write-message! record-message)
      (write-message! (assoc record-message "version" version)))))

(defn write-activate-version!
  [stream-name state]
  (write-message! {"type" "ACTIVATE_VERSION"
                   "stream" stream-name
                   "version" (get-in state
                                     ["bookmarks" stream-name "version"])})
  ;; This must return state, as it appears in the pipeline of a sync
  state)

(defn selected-field?
  [[field-name field-metadata]]
  (or (field-metadata "selected")
      (and (field-metadata "selected-by-default")
           (not (contains? field-metadata "selected")))))

(defn get-selected-fields
  [catalog stream-name]
  (let [metadata-properties
        (get-in catalog ["streams" stream-name "metadata" "properties"])
        selected-fields (filter selected-field? metadata-properties)
        selected-field-names (map (comp name first) selected-fields)]
    selected-field-names))

(defn now []
  ;; To redef in tests
  (System/currentTimeMillis))

(defn maybe-write-activate-version!
  "Writes activate version message if not in state"
  [stream-name state]
  ;; TODO: This assumes that uninterruptible full-table is the only mode,
  ;; this will need modified for incremental, CDC, and interruptible full
  ;; table to not change the table version in those modes unless needed
  ;; For now, always generate and return a new version
  (let [version-bookmark (get-in state ["bookmarks" stream-name "version"])
        new-state        (assoc-in state
                                   ["bookmarks" stream-name "version"]
                                   (now))]
    ;; Write activate version on first sync to get records flowing, and
    ;; never again so that the table only truncates at the end of the load
    ;; TODO: This will need changed (?), assumes that a full-table sync runs
    ;; 100% in a single tap run. It will need to be smarter than `nil?`
    ;; for these cases (?)
    (when (nil? version-bookmark)
      (write-activate-version! stream-name new-state))
    new-state))

(defn transform-rowversion [rowversion]
  (apply str "0x" (map (comp string/upper-case
                             (partial format "%02x"))
                       rowversion)))

(defn transform-field [catalog stream-name [k v]]
  (condp = (get-in catalog ["streams" stream-name "metadata" "properties" k "sql-datatype"])
    "timestamp"
    [k (transform-rowversion v)]
    ;; Other cases?
    [k v]))

(defn transform [catalog stream-name record]
  (into {} (map (partial transform-field catalog stream-name) record)))

(defn get-bookmark-keys
  "Gets the possible bookmark keys to use for sorting, falling back to
  `nil`.

  Priority is in this order:
  `replicationkey` > `timestamp` field > `table-key-properties`"
  [catalog stream-name]
  (let [replication-key (get-in catalog ["streams"
                                         stream-name
                                         "metadata"
                                         "replication-key"])
        timestamp-column (first
                          (first
                           (filter (fn [[k v]] (= "timestamp"
                                                  (v "sql-datatype")))
                                   (get-in catalog ["streams"
                                                    stream-name
                                                    "metadata"
                                                    "properties"]))))
        table-key-properties (get-in catalog ["streams"
                                              stream-name
                                              "metadata"
                                              "table-key-properties"])]
    (if (not (nil? replication-key))
      [replication-key]
      (if (not (nil? timestamp-column))
        [timestamp-column]
        (when (not (empty? table-key-properties))
          table-key-properties)))))

(defn valid-full-table-state? [state table-name]
  ;; The state MUST contain max_pk_values if there is a bookmark
  (if (contains? (get-in state ["bookmarks" table-name]) "last_pk_fetched")
    (contains? (get-in state ["bookmarks" table-name]) "max_pk_values")
    true))

(defn build-sync-query [stream-name schema-name table-name record-keys state]
  {:pre [(not (empty? record-keys))
         (valid-full-table-state? state stream-name)]}
  ;; TODO: Fully qualify and quote all database structures, maybe just schema
  (let [last-pk-fetched   (get-in state ["bookmarks" stream-name "last_pk_fetched"])
        bookmark-keys     (map #(format "%s >= ?" %)
                               (keys last-pk-fetched))
        max-pk-values     (get-in state ["bookmarks" stream-name "max_pk_values"])
        limiting-keys     (map #(format "%s <= ?" %)
                               (keys max-pk-values))
        add-where-clause? (or (not (empty? bookmark-keys))
                              (not (empty? limiting-keys)))
        where-clause      (when add-where-clause?
                            (str " WHERE " (string/join " AND "
                                                        (concat bookmark-keys
                                                                limiting-keys))))
        order-by           (when (not (empty? limiting-keys))
                            (str " ORDER BY " (string/join ", "
                                                           (map #(format "%s" %)
                                                                (keys max-pk-values)))))
        sql-params        [(str (format "SELECT %s FROM %s.%s"
                                        (string/join ", " record-keys)
                                        schema-name
                                        table-name)
                                where-clause
                                order-by)]]
    (if add-where-clause?
      (concat sql-params
              (vals last-pk-fetched)
              ;; TODO: String PKs? They may need quoted, they may also
              ;; need N'' for nvarchar/nchar/ntext, etc., conditionally
              ;; :facepalm:
              ;; Likely an edge case, but might be pretty rough.
              (vals max-pk-values))
      sql-params)))

(defn update-state [stream-name bookmark-keys state record]
  (reduce (fn [acc bookmark-key]
            (assoc-in acc ["bookmarks" stream-name bookmark-key] (get record bookmark-key)))
          state
          bookmark-keys))

(defn update-last-pk-fetched [stream-name bookmark-keys state record]
  (assoc-in state
            ["bookmarks" stream-name "last_pk_fetched"]
            (zipmap bookmark-keys (map (partial get record) bookmark-keys))))

(def records-since-last-state (atom 0))
(defn write-state-buffered! [stream-name state]
  (swap! records-since-last-state inc)
  (if (> @records-since-last-state 100)
    (do
      (reset! records-since-last-state 0)
      (write-state! stream-name state))
    state))

(defn get-max-pk-values [config catalog stream-name state]
  (let [dbname (get-in catalog ["streams" stream-name "metadata" "database-name"])
        bookmark-keys (get-bookmark-keys catalog stream-name)
        table-name (get-in catalog ["streams" stream-name "table_name"])
        sql-query [(format "SELECT %s FROM %s" (string/join " ," (map (fn [bookmark-key] (format "MAX(%1$s) AS %1$s" bookmark-key)) bookmark-keys)) table-name)]]
    (if (not (nil? bookmark-keys))
      (->> (jdbc/query (assoc (config->conn-map config) :dbname dbname) sql-query {:keywordize? false})
           first
           (assoc-in state ["bookmarks" stream-name "max_pk_values"]))
       state))
  )

(defn write-records-and-states!
  "Syncs all records, states, returns the latest state. Ensures that the
  bookmark we have for this stream matches our understanding of the fields
  defined in the catalog that are bookmark-able."
  [config catalog stream-name state]
  (let [dbname (get-in catalog ["streams" stream-name "metadata" "database-name"])
        record-keys (get-selected-fields catalog stream-name)
        bookmark-keys (get-bookmark-keys catalog stream-name)
        table-name (get-in catalog ["streams" stream-name "table_name"])
        schema-name (get-in catalog ["streams" stream-name "metadata" "schema-name"])
        sql-params (build-sync-query stream-name schema-name table-name record-keys state)]
    (-> (reduce (fn [acc result]
               (let [record (->> (select-keys result record-keys)
                                 (transform catalog stream-name))]
                 (write-record! stream-name state record)
                 (->> (update-last-pk-fetched stream-name bookmark-keys acc record)
                      (write-state-buffered! stream-name))))
             state
             (jdbc/reducible-query (assoc (config->conn-map config)
                                          :dbname dbname)
                                   sql-params
                                   {:raw? true}))
        (update-in ["bookmarks" stream-name] dissoc "last_pk_fetched" "max_pk_values"))))

(defn valid-state?
  [state]
  (map? state))

(defn sync-full-table
  [config catalog stream-name state]
  {:post [(valid-state? %)]}
  (->> state
       (get-max-pk-values config catalog stream-name)
       (write-state! stream-name)
       (write-records-and-states! config catalog stream-name)
       (write-activate-version! stream-name)))

(defn get-current-log-version [config catalog stream-name]
  (let [dbname (get-in catalog ["streams" stream-name "metadata" "database-name"])]
    (:current_version (first
                       (jdbc/query (assoc (config->conn-map config)
                                          :dbname dbname)
                                   ["SELECT current_version = CHANGE_TRACKING_CURRENT_VERSION()"]))))
  )

(defn build-log-based-sql-query [catalog stream-name state]
  (let [schema-name           (get-in catalog ["streams" stream-name "metadata" "schema-name"])
        table-name            (get-in catalog ["streams" stream-name "table_name"])
        primary-keys          (set (get-in catalog ["streams"
                                                    stream-name
                                                    "metadata"
                                                    "table-key-properties"]))
        primary-key-bookmarks (get-in state ["bookmarks" stream-name "last_pk_fetched"])
        current-log-version   (get-in state ["bookmarks" stream-name "current_log_version"])
        record-keys           (clojure.set/difference (set (get-selected-fields catalog stream-name))
                                                      primary-keys)]
    ;; Assert state of the world
    (assert (or (not-empty primary-keys)
                (not-empty record-keys))
            "No selected keys found, you must have a primary key and/or select columns to replicate.")
    (assert (not (nil? current-log-version))
            "Invalid log-based state, need a value for `current-log-version`.")
    (as-> (format
           (str "SELECT c.SYS_CHANGE_VERSION, c.SYS_CHANGE_OPERATION, tc.commit_time"
                (when (not-empty primary-keys)
                  (str ", " (clojure.string/join ", "
                                                 (map #(format "c.%s" %)
                                                      primary-keys))))
                (when (not-empty record-keys)
                  (str ", " (clojure.string/join ", "
                                                 (map #(format "%s.%s.%s" schema-name table-name %)
                                                      record-keys))))
                " FROM CHANGETABLE (CHANGES %s.%s, %s) as c "
                "LEFT JOIN %s.%s ON %s LEFT JOIN %s on %s"
                ;; Existence of PK Bookmark indicates that a single change
                ;; was interrupted. Only get changes for this version to
                ;; finish it out.
                (when (not-empty primary-key-bookmarks)
                  (str (format " WHERE c.SYS_CHANGE_VERSION = %s AND "
                               current-log-version)
                       (clojure.string/join " AND "
                                            (map #(format "c.%s >= ?" %)
                                                 (-> (keys primary-key-bookmarks)
                                                     sort
                                                     vec)))))
                " ORDER BY c.SYS_CHANGE_VERSION"
                (when (not-empty primary-keys)
                  (str ", " (clojure.string/join ", "
                                                 (map #(format "c.%s" %)
                                                      (sort (vec primary-keys)))))))
           schema-name
           table-name
           ;; CHANGETABLE is strictly greater than, so we decrement here
           ;; to keep the state showing the "current version"
           (if (> current-log-version 0)
             (dec current-log-version)
             0)
           schema-name
           table-name
           (clojure.string/join " AND "(map #(format "c.%s=%s.%s.%s" % schema-name table-name %) primary-keys))
           "sys.dm_tran_commit_table tc"
           "c.sys_change_version = tc.commit_ts")
        query-string
      (if (not-empty primary-key-bookmarks)
        (into [query-string] (-> (sort-by key primary-key-bookmarks)
                                 vals))
        [query-string]))
    )
  )

(defn update-current-log-version [stream-name version state]
  (let [previous-log-version (get-in state ["bookmarks" stream-name "current_log_version"])]
    (as-> (assoc-in state
                    ["bookmarks" stream-name "current_log_version"]
                    version)
        new-state
        (if (not= previous-log-version version)
          (update-in new-state ["bookmarks" stream-name] dissoc "last_pk_fetched")
          new-state))))

(defn log-based-init
  [config catalog stream-name state]
  (if (nil? (get-in state ["bookmarks" stream-name "initial_full_table_complete"]))
    (-> state
        (assoc-in ["bookmarks" stream-name "current_log_version"]
                  (get-current-log-version config catalog stream-name))
        (assoc-in ["bookmarks" stream-name "initial_full_table_complete"] false)
        ((partial write-state! stream-name)))
    state))

(defn log-based-initial-full-table
  [config catalog stream-name state]
  (if (= false (get-in state ["bookmarks" stream-name "initial_full_table_complete"]))
      (-> state
          ((partial sync-full-table config catalog stream-name))
          (assoc-in ["bookmarks" stream-name "initial_full_table_complete"] true)
          ((partial write-state! stream-name)))
      state))

(defn get-change-tracking-databases* [config]
  (set (map #(:db_name %)
            (jdbc/query (config->conn-map config)
                        [(str "SELECT DB_NAME(database_id) AS db_name "
                              "FROM sys.change_tracking_databases")]))))

(def get-change-tracking-databases (memoize get-change-tracking-databases*))

(defn get-change-tracking-tables* [config db-name]
  ;; TODO: What if it's the same named table in different schemas?
  (set (map #(:table_name %)
            (jdbc/query (assoc (config->conn-map config)
                               :dbname db-name)
                        [(str "SELECT OBJECT_NAME(object_id) AS table_name "
                              "FROM sys.change_tracking_tables")]))))

(def get-change-tracking-tables (memoize get-change-tracking-tables*))

(defn log-based-sync
  [config catalog stream-name state]
  {:pre  [(= true (get-in state ["bookmarks" stream-name "initial_full_table_complete"]))]
   :post [(valid-state? %)]}
  (let [record-keys   (get-selected-fields catalog stream-name)
        bookmark-keys (get-bookmark-keys catalog stream-name)
        dbname        (get-in catalog ["streams" stream-name "metadata" "database-name"])]
    (-> (reduce (fn [st result]
                  (let [record (as-> (select-keys result record-keys) rec
                                 (if (= "D" (get result "sys_change_operation"))
                                   (assoc rec "_sdc_deleted_at" (get result "commit_time"))
                                   rec)
                                 (transform catalog stream-name rec))]
                    (write-record! stream-name state record)
                    (->> (update-last-pk-fetched stream-name bookmark-keys st record)
                         (update-current-log-version stream-name
                                                     (get result "sys_change_version"))
                         (write-state-buffered! stream-name))))
                state
                (jdbc/reducible-query (assoc (config->conn-map config)
                                             :dbname dbname)
                                      (build-log-based-sql-query catalog stream-name state)
                                      {:raw? true}))
        ;; last_pk_fetched indicates an interruption, and should be gone
        ;; after a successful log sync
        (update-in ["bookmarks" stream-name] dissoc "last_pk_fetched"))
    )
  )

(defn assert-log-based-is-enabled [config catalog stream-name state]
  (let [table-name (get-in catalog ["streams" stream-name "table_name"])
        dbname (get-in catalog ["streams" stream-name "metadata" "database-name"])]
    (when (not (contains? (get-change-tracking-databases config) dbname))
      (throw (UnsupportedOperationException.
              (format (str "Cannot sync stream: %s using log-based replication. "
                           "Change Tracking is not enabled for database: %s")
                      stream-name
                      dbname))))
    (when (not (contains? (get-change-tracking-tables config dbname) table-name))
      (throw (UnsupportedOperationException.
              (format (str "Cannot sync stream: %s using log-based replication. "
                           "Change Tracking is not enabled for table: %s")
                      stream-name
                      table-name))))
    state))

(defn choose-sync-strategy [config catalog stream-name state]
  {:post [(valid-state? %)]}
  (condp = (get-in catalog ["streams" stream-name "metadata" "replication-method"])
    "FULL_TABLE"
    (sync-full-table config catalog stream-name state)

    "LOG_BASED"
    (->> state
         (assert-log-based-is-enabled config catalog stream-name)
         (log-based-init config catalog stream-name)
         (log-based-initial-full-table config catalog stream-name)
         (write-state! stream-name)
         (log-based-sync config catalog stream-name))

    "INCREMENTAL"
    (throw (UnsupportedOperationException.
            (format "Cannot sync stream %s. Incremental replication is not yet implemented."
                    stream-name)))

    ;; Default
    (throw (IllegalArgumentException. (format "Replication Method for stream %s is invalid: %s"
                                              stream-name
                                              (get-in catalog ["streams" stream-name "metadata" "replication-method"]))))))

(defn sync-stream!
  [config catalog state stream-name]
  (log/infof "Syncing stream %s" stream-name)
  (write-schema! catalog stream-name)
  (->> (maybe-write-activate-version! stream-name state)
       (choose-sync-strategy config catalog stream-name)
       (write-state! stream-name)))

(defn selected? [catalog stream-name]
  (get-in catalog ["streams" stream-name "metadata" "selected"]))

(defn maybe-sync-stream! [config catalog state stream-name]
  {:post [(valid-state? %)]}
  (if (selected? catalog stream-name)
    ;; returns state
    (sync-stream! config catalog state stream-name)
    (do (log/infof "Skipping stream %s"
                   stream-name)
        ;; returns original state
        state)))

(defn do-sync [config catalog state]
  {:pre [(valid-state? state)]}
  (log/info "Starting sync mode")
  ;; Sync streams, no selection (e.g., maybe-sync-stream)
  (reduce (partial maybe-sync-stream! config catalog)
          state
          (->> (catalog "streams")
               vals
               (map #(get % "tap_stream_id")))))

(defn repl-arg-passed?
  [args]
  (some (partial = "--repl") args))

(defn start-nrepl-server
  [args]
  (require 'cider.nrepl)
  (let [the-nrepl-server
        (nrepl-server/start-server :bind "0.0.0.0"
                                   :handler (ns-resolve 'cider.nrepl 'cider-nrepl-handler))]
    (spit ".nrepl-port" (:port the-nrepl-server))
    (log/infof "Started nrepl server at %s"
               (.getLocalSocketAddress (:server-socket the-nrepl-server)))
    the-nrepl-server))

(defn maybe-stop-nrepl-server
  [args the-nrepl-server]
  (if (repl-arg-passed? args)
    (do
      (log/infof "Leaving repl open for inspection because --repl was passed")
      (log/infof "nrepl server at %s"
                 (.getLocalSocketAddress (:server-socket the-nrepl-server))))
    (do
      (log/infof "Shutting down the nrepl server")
      (nrepl-server/stop-server the-nrepl-server))))

(defn non-system-database-name?
  [config]
  (if (config "database")
    (non-system-database? {:table_cat (config "database")})
    config))

(defn deserialize-stream-metadata
  [serialized-stream-metadata]
  (reduce (fn [metadata serialized-metadata-entry]
            (reduce (fn [entry-metadata [k v]]
                      (assoc-in
                       entry-metadata
                       (conj (serialized-metadata-entry "breadcrumb") k)
                       v))
                    metadata
                    (serialized-metadata-entry "metadata")))
          {}
          serialized-stream-metadata))

(defn get-unsupported-breadcrumbs
  [stream-schema-metadata]
  (->> (stream-schema-metadata "properties")
       (filter (fn [[k v]]
                 (= "unsupported" (v "inclusion"))))
       (map (fn [[k _]]
              ["properties" k]))))

(defn deserialize-stream-schema
  [serialized-stream-schema stream-schema-metadata]
  (let [unsupported-breadcrumbs (get-unsupported-breadcrumbs stream-schema-metadata)]
    (reduce (fn [acc unsupported-breadcrumb]
              (assoc-in acc unsupported-breadcrumb nil))
            serialized-stream-schema
            unsupported-breadcrumbs)))

(defn deserialize-stream
  [serialized-stream]
  {:pre [(map? serialized-stream)]}
  (as-> serialized-stream ss
    (update ss "metadata" deserialize-stream-metadata)
    (update ss "schema" deserialize-stream-schema (ss "metadata"))))

(defn deserialize-streams
  [serialized-streams]
  (reduce (fn [streams deserialized-stream]
            (assoc streams (deserialized-stream "tap_stream_id") deserialized-stream))
          {}
          (map deserialize-stream serialized-streams)))

(defn serialized-catalog->catalog
  [serialized-catalog]
  (update serialized-catalog "streams" deserialize-streams))

(defn slurp-json
  [f]
  (-> f
      io/reader
      json/read))

(defn parse-config
  "This function exists as a test seam"
  [config-file]
  (slurp-json config-file))

(defn parse-state
  "This function exists as a test seam and for the post condition"
  [state-file]
  {:post [(valid-state? %)]}
  (slurp-json state-file))

(defn parse-catalog
  "This function exists as a test seam"
  [catalog-file]
  (slurp-json catalog-file))

(def cli-options
  [["-d" "--discover" "Discovery Mode"]
   [nil "--repl" "REPL Mode"]
   [nil "--config CONFIG" "Config File"
    :parse-fn #'parse-config
    :validate [non-system-database-name?
               (format "System databases (%s) may not be synced"
                       (string/join ", " system-database-names))]]
   [nil "--catalog CATALOG" "Singer Catalog File"
    :parse-fn (comp serialized-catalog->catalog
                    #'parse-catalog)]
   [nil "--state STATE" "Singer State File"
    :default {}
    :parse-fn #'parse-state]
   ["-h" "--help"]])

(defn get-interesting-errors
  [opts]
  (filter (fn [error]
            (not (string/starts-with? error "Unknown option: ")))
          (:errors opts)))

(defn parse-opts
  [args]
  (let [opts (cli/parse-opts args cli-options)
        _ (def opts opts)
        interesting-errors (get-interesting-errors opts)]
    (def config (get-in opts [:options :config]))
    (def catalog (get-in opts [:options :catalog]))
    (def state (get-in opts [:options :state]))
    (when (not (empty? interesting-errors))
      (throw (IllegalArgumentException. (string/join "\n" interesting-errors))))
    opts))

(defn -main [& args]
  (let [the-nrepl-server (start-nrepl-server args)]
    ;; This and the other defs here are not accidental. These are required
    ;; to be able to easily debug a running process that you didn't already
    ;; intend to repl into.
    (def args args)
    (try
      (let [{{:keys [discover repl config catalog state]} :options}
            (parse-opts args)]
        (cond
          discover
          (do-discovery config)

          catalog
          (do-sync config catalog state)

          :else
          ;; FIXME: (show-help)?
          nil)
        (log/info "Tap Finished")
        (maybe-stop-nrepl-server args the-nrepl-server)
        (when (not (repl-arg-passed? args))
          (System/exit 0)))
      (catch Exception ex
        (def ex ex)
        (maybe-stop-nrepl-server args the-nrepl-server)
        (dorun (map #(log/fatal %) (string/split (or (.getMessage ex)
                                                     (str ex)) #"\n")))
        (when (not (repl-arg-passed? args))
          (System/exit 1))))))
