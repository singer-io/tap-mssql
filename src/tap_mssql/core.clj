(ns tap-mssql.core
  (:require [clojure.tools.logging :as logger]
            [clojure.tools.nrepl.server :as nrepl-server]
            [clojure.tools.cli :as cli]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            )
  (:gen-class))


(def cli-options
  [["-d" "--discover" "Discovery Mode"]
   [nil "--repl" "REPL Mode"]
   [nil "--config CONFIG" "Config File"
    :parse-fn #(json/parse-stream (io/reader %) true)]
   [nil "--catalog CATALOG" "Singer Catalog File"
    :parse-fn #(json/parse-stream (io/reader %) true)]
   [nil "--state STATE" "Singer State File"
    :parse-fn #(json/parse-stream (io/reader %) true)]
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

(defn do-discovery []
  (log-infof "Starting discovery mode"))

(defn do-sync [config catalog state]
  (log-infof "Starting sync mode")
  (throw (ex-info "Sync mode not yet implemented." {})))

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
