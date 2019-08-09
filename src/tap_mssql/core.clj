(ns tap-mssql.core
  (:require [tap-mssql.catalog :as catalog]
            [tap-mssql.serialized-catalog :as serialized-catalog]
            [tap-mssql.sync-strategies.full :as full]
            [tap-mssql.sync-strategies.logical :as logical]
            [tap-mssql.sync-strategies.incremental :as incremental]
            [tap-mssql.singer.parse :as singer-parse]
            [tap-mssql.singer.messages :as singer-messages]
            [clojure.tools.logging :as log]
            [clojure.tools.nrepl.server :as nrepl-server]
            [clojure.tools.cli :as cli]
            [clojure.string :as string]
            [clojure.data.json :as json])
  (:gen-class))

(defn valid-state?
  [state]
  (map? state))

(def cli-options
  [["-d" "--discover" "Discovery Mode"]
   [nil "--repl" "REPL Mode"]
   [nil "--config CONFIG" "Config File"
    :parse-fn #'singer-parse/config
    :validate [catalog/non-system-database-name?
               (format "System databases (%s) may not be synced"
                       (string/join ", " catalog/system-database-names))]]
   [nil "--catalog CATALOG" "Singer Catalog File"
    :parse-fn (comp serialized-catalog/->catalog
                    #'singer-parse/catalog)]
   [nil "--state STATE" "Singer State File"
    :default {}
    :parse-fn #'singer-parse/state]
   ["-h" "--help"]])

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

(defn do-discovery [config]
  (log/info "Starting discovery mode")
  (-> (catalog/discover config)
      (catalog/->serialized-catalog)
      json/write-str
      println))

(defn valid-primary-keys? [catalog stream-name]
  (let [stream-metadata          (get-in catalog ["streams" stream-name "metadata"])
        primary-keys             (get-in stream-metadata ["table-key-properties"])
        unsupported-primary-keys (filter #(= "unsupported"
                                             (get-in stream-metadata ["properties" % "inclusion"]))
                                         primary-keys)]
    (if (not-empty unsupported-primary-keys)
      (throw (ex-info (format "Stream %s has unsupported primary key(s): %s"
                              stream-name
                              (string/join ", " unsupported-primary-keys)) {}))
      true)))

(defn dispatch-sync-by-strategy [config catalog stream-name state]
  {:post [(map? %)]}
  (condp = (get-in catalog ["streams" stream-name "metadata" "replication-method"])
    "FULL_TABLE"
    (full/sync! config catalog stream-name state)

    "LOG_BASED"
    (logical/sync! config catalog stream-name state)

    "INCREMENTAL"
    (incremental/sync! config catalog stream-name state)

    ;; Default
    (throw (IllegalArgumentException. (format "Replication Method for stream %s is invalid: %s"
                                              stream-name
                                              (get-in catalog ["streams" stream-name "metadata" "replication-method"]))))))

(defn sync-stream!
  [config catalog state stream-name]
  {:pre [(valid-primary-keys? catalog stream-name)]}
  (let [replication-method (get-in catalog ["streams" stream-name "metadata" "replication-method"])]
    (log/infof "Syncing stream %s using replication method %s" stream-name replication-method)
    (singer-messages/write-schema! catalog stream-name)
    (->> (singer-messages/maybe-write-activate-version! stream-name replication-method catalog state)
         (dispatch-sync-by-strategy config catalog stream-name)
         (singer-messages/write-state! stream-name))))

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

(defn set-include-db-and-schema-names-in-messages!
  [config]
  (reset! singer-messages/include-db-and-schema-names-in-messages? (= "true"
                                                                      (get config "include_schemas_in_destination_stream_name"))))

(defn -main [& args]
  (let [the-nrepl-server (start-nrepl-server args)]
    ;; This and the other defs here are not accidental. These are required
    ;; to be able to easily debug a running process that you didn't already
    ;; intend to repl into.
    (def args args)
    (try
      (let [{{:keys [discover repl config catalog state]} :options}
            (parse-opts args)]
        (set-include-db-and-schema-names-in-messages! config)
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

      (catch Throwable ex
        (def ex ex)
        (.printStackTrace ex)
        (dorun (map #(log/fatal %) (string/split (or (.getMessage ex)
                                                     (str ex)) #"\n"))))
      (finally
        ;; If we somehow skip the catch block, we need to always at least exit if not --repl
        (maybe-stop-nrepl-server args the-nrepl-server)
        (when (not (repl-arg-passed? args))
          (System/exit 1))))))
