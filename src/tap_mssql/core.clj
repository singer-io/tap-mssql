(ns tap-mssql.core
  (:require [clojure.tools.logging :as logger]
            [clojure.tools.nrepl.server :as nrepl-server]
            [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.java.jdbc :as jdbc])
  (:gen-class))


(def sql-server-2017-version 14)

(def empty-catalog {:streams []})

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
  ((complement (comp #{"master" "tempdb" "model" "msdb" "rdsadmin"} :name))
   database))

(defn get-databases
  [config]
  (let [conn-map (config->conn-map config)]
    (filter non-system-database?
            (jdbc/query conn-map
                        [(str "select name "
                              "from sys.databases "
                              "where has_dbaccess(name) = 1")]))))

(defn table->catalog-entry
  [table]
  {:stream (:name table)
   :tap-stream-id (:name table)
   :table-name (:name table)
   :schema {}
   :metadata {}})

(defn add-table
  [catalog table]
  (update (or catalog empty-catalog) :streams conj (table->catalog-entry table)))

(defn get-database-tables
  [config database]
  (let [conn-map (config->conn-map config)]
    (jdbc/query (assoc conn-map :dbname (:name database))
                ["select name from sys.tables"])))

(defn get-tables
  [config]
  (flatten (map (partial get-database-tables config) (get-databases config))))

(defn get-database-views
  [config database]
  (let [conn-map (config->conn-map config)]
    (jdbc/query (assoc conn-map :dbname (:name database))
                ["select name from sys.views"])))

(defn get-views
  [config]
  (flatten (map (partial get-database-views config) (get-databases config))))

(defn view->catalog-entry
  [view]
  {:stream (:name view)
   :tap-stream-id (:name view)
   :table-name (:name view)
   :schema {}
   :metadata {}})

(defn add-view
  [catalog view]
  (update (or catalog empty-catalog) :streams conj (view->catalog-entry view)))

(defn discover-catalog
  [config]
  (jdbc/with-db-metadata [metadata (config->conn-map config)]
    (when (not= sql-server-2017-version (.getDatabaseMajorVersion metadata))
      (throw (IllegalStateException. "SQL Server database is not SQL Server 2017"))))
  (as-> empty-catalog catalog
      (reduce add-table catalog (get-tables config))
      (reduce add-view catalog (get-views config))))

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
