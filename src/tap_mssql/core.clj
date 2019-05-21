(ns tap-mssql.core
  (:require [clojure.tools.logging :as logger]
            [clojure.tools.nrepl.server :as nrepl-server]
            [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.java.jdbc :as jdbc])
  (:gen-class))


(def sql-server-2017-version 14)

;;; Note: This is different than the serialized form of the the catalog.
;;; The catalog serialized is :streams → [stream1 … streamN]. This will be
;;; :streams → :streamName → stream definition and will be serialized like
;;; {:streams (vals (:streams catalog))}.
(def empty-catalog {:streams {}})

(def cli-options
  [["-d" "--discover" "Discovery Mode"]
   [nil "--repl" "REPL Mode"]
   [nil "--config CONFIG" "Config File"
    :parse-fn (comp json/read io/reader)]
   [nil "--catalog CATALOG" "Singer Catalog File"
    :parse-fn (comp json/read io/reader)]
   [nil "--state STATE" "Singer State File"
    :parse-fn (comp json/read io/reader)]
   ["-h" "--help"]])

(defn nrepl-handler []
  (require 'cider.nrepl)
  (ns-resolve 'cider.nrepl 'cider-nrepl-handler))

(defn log-infof
  [message-format & args]
  (binding [*out* *err*]
    (println (apply format
                    (str "INFO " message-format)
                    args))))

(defn config->conn-map
  [config]
  {:dbtype "sqlserver"
   :dbname ""
   :host (config "host")
   :password (config "password")
   :user (config "user")})

(defn non-system-database?
  [database]
  (-> database
      :table_cat
      #{"master" "tempdb" "model" "msdb" "rdsadmin"}
      not))

(defn get-databases
  [config]
  (let [conn-map (config->conn-map config)]
    (filter non-system-database?
            (jdbc/with-db-metadata [md conn-map]
              (jdbc/metadata-result (.getCatalogs md))))))

(defn column->catalog-entry
  [column]
  {:stream (:table_name column)
   :tap_stream_id (:table_name column)
   :table_name (:table_name column)
   :schema {:type "object"}
   :metadata {:database-name (:table_cat column)
              :schema-name (:table_name column)
              :table-key-properties #{}
              :is-view (:is-view? column)}})

(defn column->schema
  [{:keys [type_name] :as column}]
  ({"int"       {:type    "integer"
                 :minimum -2147483648
                 :maximum 2147483647}
    "bigint"    {:type    "integer"
                 :minimum -9223372036854775808
                 :maximum 9223372036854775807}
    "smallint"  {:type    "integer"
                 :minimum -32768
                 :maximum 32767}
    "tinyint"   {:type    "integer"
                 :minimum 0
                 :maximum 255}
    "float"     {:type "number"}
    "real"      {:type "number"}
    "bit"       {:type "boolean"}
    "decimal"   {:type "number"}
    "numeric"   {:type "number"}
    "date"      {:type   "string"
                 :format "date-time"}
    "time"      {:type   "string"
                 :format "date-time"}
    "char"      {:type      "string"
                 :minLength (:column_size column)
                 :maxLength (:column_size column)}
    "nchar"     {:type      "string"
                 :minLength (:column_size column)
                 :maxLength (:column_size column)}
    "varchar"   {:type      "string"
                 :minLength 0
                 :maxLength (:column_size column)}
    "nvarchar"  {:type      "string"
                 :minLength 0
                 :maxLength (:column_size column)}
    "binary"    {:type      "string"
                 :minLength (:column_size column)
                 :maxLength (:column_size column)}
    "varbinary" {:type      "string"
                 :maxLength (:column_size column)}}
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
  {:inclusion           (if (:primary-key? column)
                          "automatic"
                          "available")
   :sql-datatype        (:type_name column)
   :selected-by-default true})

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

(defn get-database-columns
  [config database]
  (let [conn-map (assoc (config->conn-map config)
                        :dbname
                        (:table_cat database))
        raw-columns (get-database-raw-columns conn-map database)]
    (->> raw-columns
         (map (partial add-primary-key?-data conn-map))
         (map (partial add-is-view?-data conn-map)))))

(defn get-columns
  [config]
  (flatten (map (partial get-database-columns config) (get-databases config))))

(defn discover-catalog
  [config]
  (jdbc/with-db-metadata [metadata (config->conn-map config)]
    (when (not= sql-server-2017-version (.getDatabaseMajorVersion metadata))
      (throw (IllegalStateException. "SQL Server database is not SQL Server 2017"))))
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

(defn serialize-streams
  [catalog]
  (update catalog :streams vals))

(defn catalog->serialized-catalog
  [catalog]
  (-> catalog
      serialize-streams
      serialize-metadata))

(defn do-discovery [{:as config}]
  (log-infof "Starting discovery mode")
  (-> (discover-catalog config)
      catalog->serialized-catalog
      json/write-str
      println))

(defn do-sync [config catalog state]
  (log-infof "Starting sync mode")
  (throw (UnsupportedOperationException. "Sync mode not yet implemented.")))

(defn -main [& args]
  (try
    (let [opts (cli/parse-opts args cli-options)
          {{:keys [discover repl config catalog state]} :options} opts]
      (when repl
        ;; We do this here to avoid starting the nrepl server during `lein
        ;; test` executions
        (defonce the-nrepl-server
          (nrepl-server/start-server :bind "0.0.0.0"
                                     :handler (nrepl-handler)))
        (.start (Thread. #((loop []
                             (Thread/sleep 1000)
                             (recur)))))
        (log-infof "Started nrepl server at %s"
                   (.getLocalSocketAddress (:server-socket the-nrepl-server)))
        (spit ".nrepl-port" (:port the-nrepl-server)))

      (cond
        discover
        (do-discovery config)

        catalog
        (do-sync config catalog state)

        :else
        ;; FIXME: (show-help)?
        nil))
    (catch Exception ex
      (dorun (map #(logger/fatal %)
                  (clojure.string/split (str ex) #"\n")))
      (throw ex))))
