(ns tap-mssql.core
  (:require [clojure.tools.logging :as log]
            [clojure.tools.nrepl.server :as nrepl-server]
            [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.data.json :as json]
            [clojure.java.jdbc :as jdbc])
  (:import [com.microsoft.sqlserver.jdbc SQLServerException])
  (:gen-class))


;;; Note: This is different than the serialized form of the the catalog.
;;; The catalog serialized is :streams → [stream1 … streamN]. This will be
;;; :streams → :streamName → stream definition and will be serialized like
;;; {:streams (vals (:streams catalog))}.
(def empty-catalog {:streams {}})

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
                          :trustServerCertificate true)
                   conn-map)]
    (do (jdbc/with-db-metadata [md conn-map]
          (jdbc/metadata-result (.getCatalogs md)))
        (log/info "Successfully connected to the instance")
        conn-map)))

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

(defn get-databases
  [config]
  (let [conn-map (config->conn-map config)]
    (filter (every-pred non-system-database?
                        (partial config-specific-database? config))
            (jdbc/with-db-metadata [md conn-map]
              (jdbc/metadata-result (.getCatalogs md))))))

(defn column->catalog-entry
  [column]
  {:stream (:table_name column)
   :tap_stream_id (format "%s-%s-%s"
                          (:table_cat column)
                          (:table_schem column)
                          (:table_name column))
   :table_name (:table_name column)
   :schema {:type "object"}
   :metadata {:database-name (:table_cat column)
              :schema-name (:table_schem column)
              :table-key-properties #{}
              :is-view (:is-view? column)}})

(defn column->schema
  [{:keys [type_name] :as column}]
  ({"int"              {:type    "integer"
                        :minimum -2147483648
                        :maximum 2147483647}
    "bigint"           {:type    "integer"
                        :minimum -9223372036854775808
                        :maximum 9223372036854775807}
    "smallint"         {:type    "integer"
                        :minimum -32768
                        :maximum 32767}
    "tinyint"          {:type    "integer"
                        :minimum 0
                        :maximum 255}
    "float"            {:type "number"}
    "real"             {:type "number"}
    "bit"              {:type "boolean"}
    "decimal"          {:type "number"}
    "numeric"          {:type "number"}
    "date"             {:type   "string"
                        :format "date-time"}
    "time"             {:type   "string"
                        :format "date-time"}
    "datetime"         {:type   "string"
                        :format "date-time"}
    "char"             {:type      "string"
                        :minLength (:column_size column)
                        :maxLength (:column_size column)}
    "nchar"            {:type      "string"
                        :minLength (:column_size column)
                        :maxLength (:column_size column)}
    "varchar"          {:type      "string"
                        :minLength 0
                        :maxLength (:column_size column)}
    "nvarchar"         {:type      "string"
                        :minLength 0
                        :maxLength (:column_size column)}
    "binary"           {:type      "string"
                        :minLength (:column_size column)
                        :maxLength (:column_size column)}
    "varbinary"        {:type      "string"
                        :maxLength (:column_size column)}
    "uniqueidentifier" {:type    "string"
                        ;; a string constant in the form
                        ;; xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, in which
                        ;; each x is a hexadecimal digit in the range 0-9
                        ;; or a-f. For example,
                        ;; 6F9619FF-8B86-D011-B42D-00C04FC964FF is a valid
                        ;; uniqueidentifier value.
                        ;;
                        ;; https://docs.microsoft.com/en-us/sql/t-sql/data-types/uniqueidentifier-transact-sql?view=sql-server-2017
                        :pattern "[A-F0-9]{8}-([A-F0-9]{4}){3}-[A-F0-9]{12}"}}
   type_name))

(defn add-column-schema-to-catalog-stream-schema
  [catalog-stream-schema column]
  (update-in catalog-stream-schema [:properties (:column_name column)]
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
  {:inclusion           (if (:unsupported? column)
                          "unsupported"
                          (if (:primary-key? column)
                            "automatic"
                            "available"))
   :sql-datatype        (:type_name column)
   :selected-by-default (not (:unsupported? column))})

(defn add-column-schema-to-catalog-stream-metadata
  [catalog-stream-metadata column]
  (update-in catalog-stream-metadata [:properties (:column_name column)]
             merge
             (column->metadata column)))

(defn add-column-to-primary-keys
  [catalog-stream column]
  (if (:primary-key? column)
    (update-in catalog-stream [:metadata :table-key-properties] conj (:column_name column))
    catalog-stream))

(defn add-column-to-stream
  [catalog-stream column]
  (-> (or catalog-stream (column->catalog-entry column))
      (add-column-to-primary-keys column)
      (update :schema add-column-schema-to-catalog-stream-schema column)
      (update :metadata add-column-schema-to-catalog-stream-metadata column)))

(defn add-column
  [catalog column]
  (update-in catalog [:streams (:table_name column)]
             add-column-to-stream
             column))

(defn get-database-raw-columns
  [conn-map database]
  (jdbc/with-db-metadata [md conn-map]
    (jdbc/metadata-result (.getColumns md (:table_cat database) "dbo" nil nil))))

(defn add-primary-key?-data
  [conn-map column]
  (let [primary-keys (column->table-primary-keys conn-map
                                                 (:table_cat column)
                                                 (:table_schem column)
                                                 (:table_name column))]
    (assoc column :primary-key? (primary-keys (:column_name column)))))

(defn get-column-database-view-names*
  [conn-map table_cat]
  (jdbc/with-db-metadata [md conn-map]
    (->> (.getTables md table_cat "dbo" nil (into-array ["VIEW"]))
         jdbc/metadata-result
         (map :table_name)
         (into #{}))))

;;; Not memoizing this proves to have prohibitively bad performance
;;; characteristics.
(def get-column-database-view-names (memoize get-column-database-view-names*))

(defn add-is-view?-data
  [conn-map column]
  (let [view-names (get-column-database-view-names conn-map (:table_cat column))]
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
  (flatten (map (partial get-database-columns config) (get-databases config))))

(defn discover-catalog
  [config]
  (jdbc/with-db-metadata [metadata (config->conn-map config)]
    (log/infof "Connecting to %s version %s"
               (.getDatabaseProductName metadata)
               (.getDatabaseProductVersion metadata)))
  ;; It's important to keep add-column pure and keep all database
  ;; interaction in get-columns for testability
  (reduce add-column empty-catalog (get-columns config)))

(defn serialize-stream-metadata-property
  [[stream-metadata-property-name stream-metadata-property-metadata :as stream-metadata-property]]
  {:metadata stream-metadata-property-metadata
   :breadcrumb [:properties stream-metadata-property-name]})

(defn serialize-stream-metadata-properties
  [stream-metadata-properties]
  (let [properties (:properties stream-metadata-properties)]
    (concat [{:metadata (dissoc stream-metadata-properties :properties)
              :breadcrumb []}]
            (map serialize-stream-metadata-property properties))))

(defn serialize-stream-metadata
  [{:keys [metadata] :as stream}]
  (update stream :metadata serialize-stream-metadata-properties))

(defn serialize-metadata
  [catalog]
  (update catalog :streams (partial map serialize-stream-metadata)))

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
          :properties
          serialize-stream-schema-properties))

(defn serialize-stream
  [stream-catalog-entry]
  (update stream-catalog-entry :schema
          serialize-stream-schema))

(defn serialize-streams
  [catalog]
  (update catalog
          :streams
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

(defn do-sync [config catalog state]
  (log/info "Starting sync mode")
  (throw (UnsupportedOperationException. "Sync mode not yet implemented.")))

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

(defn parse-config
  [config-file]
  (-> config-file
      io/reader
      json/read))

(def cli-options
  [["-d" "--discover" "Discovery Mode"]
   [nil "--repl" "REPL Mode"]
   [nil "--config CONFIG" "Config File"
    :parse-fn #'parse-config
    :validate [non-system-database-name?
               (format "System databases (%s) may not be synced"
                       (string/join ", " system-database-names))]]
   [nil "--catalog CATALOG" "Singer Catalog File"
    :parse-fn (comp json/read io/reader)]
   [nil "--state STATE" "Singer State File"
    :parse-fn (comp json/read io/reader)]
   ["-h" "--help"]])

(defn parse-opts
  [args]
  (let [opts (cli/parse-opts args cli-options)
        _ (def opts opts)]
    (def config (get-in opts [:options :config]))
    (when (:errors opts)
      (throw (IllegalArgumentException. (string/join "\n" (:errors opts)))))
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
