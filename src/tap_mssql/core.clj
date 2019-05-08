(ns tap-mssql.core
  (:require [clojure.tools.logging :as logger]
            [clojure.tools.nrepl.server :as nrepl-server]
            [clojure.tools.cli :as cli]
            )
  (:gen-class))


(def cli-options
  [[nil "--repl" "REPL Mode"]
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

(defn -main [& args]
  (try
    (let [opts (cli/parse-opts args cli-options)
          {{:keys [:discover :repl :config :catalog :state]} :options} opts]
      (when repl (log-infof "Started nrepl server at %s"
                            (.getLocalSocketAddress (:server-socket the-nrepl-server))))
      (spit ".nrepl-port" (:port the-nrepl-server))


      (if repl
        (.start (Thread. #((loop []
                       (Thread/sleep 1000)
                       (recur)))))
        (nrepl-server/stop-server the-nrepl-server)))
   (catch Exception ex
      (dorun (map #(logger/fatal %)
                  (clojure.string/split (str ex) #"\n")))
      (throw ex))))
