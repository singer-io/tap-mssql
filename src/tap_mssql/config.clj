(ns tap-mssql.config
  (:require [tap-mssql.utils :refer [try-read-only]]
            [clojure.tools.logging :as log]
            [clojure.java.jdbc :as jdbc]))

(defn check-connection [conn-map]
  (do (jdbc/with-db-metadata [md conn-map]
        (jdbc/metadata-result (.getCatalogs md)))
      (log/info "Successfully connected to the instance")
      conn-map))

(defn ->conn-map*
  ([config]
   (->conn-map* config false))
  ([config is-readonly?]
   (let [conn-map (cond-> {:dbtype "sqlserver"
                           :dbname (or (config "database") "") ;; database is optional - if omitted it is set to an empty string
                           :host (config "host")
                           :port (or (config "port") 0) ;; port is optional - if omitted it is set to 0 for a dynamic port
                           :password (config "password")
                           :user (config "user")}

                    (= "true" (config "ssl"))
                    ;; TODO: The only way I can get a test failure is by
                    ;; changing the code to say ":trustServerCertificate
                    ;; false". In which case, truststores need to be
                    ;; specified. This is for the "correct" way of doing
                    ;; things, where we are validating SSL, but for now,
                    ;; leaving the certificate unverified should work.
                    (assoc ;; Based on the [docs][1], we believe thet
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
                     :trustServerCertificate false))]
     ;; returns conn-map and logs on successful connection
     (if is-readonly?
       (try-read-only [test-conn conn-map]
         (check-connection test-conn))
       (check-connection conn-map)))))

(def ->conn-map (memoize ->conn-map*))
