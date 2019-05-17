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
  [{:keys [host user password]}]
  {:dbtype "sqlserver"
   :dbname ""
   :host host
   :password password
   :user user})

(defn non-system-database?
  [database]
  ((complement (comp #{"master" "tempdb" "model" "msdb" "rdsadmin"} :table_cat))
   database))

(defn get-databases
  [config]
  (let [conn-map (config->conn-map config)]
    (filter non-system-database?
            (jdbc/with-db-metadata [md conn-map]
              (jdbc/metadata-result (.getCatalogs md))))))

(defn column->catalog-entry
  [column]
  {:stream (:table_name column)
   :tap-stream-id (:table_name column)
   :table-name (:table_name column)
   :schema {:type "object"}
   :metadata {}})

(defn column->schema
  [{:keys [type_name] :as column}]
  ({"int"      {:type    "integer"
                :minimum -2147483648
                :maximum 2147483647}
    "bigint"   {:type    "integer"
                :minimum -9223372036854775808
                :maximum 9223372036854775807}
    "smallint" {:type    "integer"
                :minimum -32768
                :maximum 32767}
    "tinyint"  {:type    "integer"
                :minimum 0
                :maximum 255}
    "bit"      {:type "boolean"}
    "char"     {:type      "string"
                :minLength (:column_size column)
                :maxLength (:column_size column)}}
   type_name))

(defn add-column-schema-to-catalog-stream-schema
  [catalog-stream-schema column]
  (update-in catalog-stream-schema [:properties (:column_name column)]
             merge
             (column->schema column)))

(defn add-column-to-stream
  [catalog-stream column]
  (update (or catalog-stream (column->catalog-entry column))
          :schema add-column-schema-to-catalog-stream-schema column))

(defn add-column
  [catalog column]
  (update-in catalog [:streams (:table_name column)]
             add-column-to-stream
             column))

(defn get-database-columns
  [config database]
  (let [conn-map (assoc (config->conn-map config)
                        :dbname
                        (:table_cat database))]
    (jdbc/with-db-metadata [md conn-map]
      (jdbc/metadata-result (.getColumns md (:table_cat database) "dbo" nil nil)))))

(defn get-columns
  [config]
  (flatten (map (partial get-database-columns config) (get-databases config))))

(defn discover-catalog
  [config]
  (jdbc/with-db-metadata [metadata (config->conn-map config)]
    (when (not= sql-server-2017-version (.getDatabaseMajorVersion metadata))
      (throw (IllegalStateException. "SQL Server database is not SQL Server 2017"))))
  (reduce add-column empty-catalog (get-columns config)))

(defn do-discovery [{:as config}]
  (log-infof "Starting discovery mode")
  (println (json/write-str (discover-catalog config))))

(defn do-sync [config catalog state]
  (log-infof "Starting sync mode")
  (throw (UnsupportedOperationException. "Sync mode not yet implemented.")))

(defn -main [& args]
  (try
    (let [opts (cli/parse-opts args cli-options)
          {{:keys [:discover :repl :config :catalog :state]} :options} opts]
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
        (do-discovery)

        catalog
        (do-sync config catalog state)

        :else
        ;; FIXME: (show-help)?
        nil))
    (catch Exception ex
      (dorun (map #(logger/fatal %)
                  (clojure.string/split (str ex) #"\n")))
      (throw ex))))
