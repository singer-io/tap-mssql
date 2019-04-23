(ns tap-const.core
  (:require [clojure.tools.nrepl.server :as nrepl-server])
  (:gen-class))

(defn nrepl-handler
  []
  (require 'cider.nrepl)
  (ns-resolve 'cider.nrepl 'cider-nrepl-handler))

(defonce the-nrepl-server
  (nrepl-server/start-server :bind "0.0.0.0"
                             :handler (nrepl-handler)))

(defn log-infof
  [message-format & args]
  (binding [*out* *err*]
    (println (apply format
                    (str "INFO " message-format)
                    args))))

(defn -main
  [& args]
  (log-infof "INFO Started nrepl server at %s"
             (.getLocalSocketAddress (:server-socket the-nrepl-server)))
  (spit ".nrepl-port" (:port the-nrepl-server))
  (.start (Thread. #((loop []
                       (Thread/sleep 1000)
                       (recur))))))
