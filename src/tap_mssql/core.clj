(ns tap-mssql.core
  (:require [clojure.tools.logging :as logger]
            [clojure.tools.nrepl.server :as nrepl-server]
            [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.java.jdbc :as jdbc])
  (:gen-class))


(def SQL-SERVER-2017-VERSION 14)

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

(defonce the-nrepl-server
  (nrepl-server/start-server :bind "0.0.0.0"
                             :handler (nrepl-handler)))

(defn config->conn-map
  [{:keys [host user password]}]
  {:dbtype "sqlserver"
   :dbname ""
   :host host
   :password password
   :user user})

(defn do-discovery [{:as config}]
  (log-infof "Starting discovery mode")
  (with-open [conn (jdbc/get-connection (config->conn-map config))]
    (let [metadata (.getMetaData conn)]
      (= SQL-SERVER-2017-VERSION (.getDatabaseMajorVersion metadata)))
    (println (json/write-str {}))))

(defn do-sync [config catalog state]
  (log-infof "Starting sync mode")
  (throw (UnsupportedOperationException. "Sync mode not yet implemented.")))

(defn -main [& args]
  (try
    (let [opts (cli/parse-opts args cli-options)
          {{:keys [:discover :repl :config :catalog :state]} :options} opts]
      (when repl (log-infof "Started nrepl server at %s"
                            (.getLocalSocketAddress (:server-socket the-nrepl-server))))
      (spit ".nrepl-port" (:port the-nrepl-server))

      (cond
        discover
        (do-discovery)

        catalog
        (do-sync config catalog state)

        :else
        ;; FIXME: (show-help)?
        nil)

      (if repl
        (.start (Thread. #((loop []
                             (Thread/sleep 1000)
                             (recur)))))
        (nrepl-server/stop-server the-nrepl-server)))
    (catch Exception ex
      (dorun (map #(logger/fatal %)
                  (clojure.string/split (str ex) #"\n")))
      (throw ex))))
