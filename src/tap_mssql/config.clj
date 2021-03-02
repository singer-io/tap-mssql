(ns tap-mssql.config
  (:require [clojure.tools.logging :as log]
            [clojure.java.jdbc :as jdbc]))

(defn check-connection [conn-map]
  (do (jdbc/with-db-metadata [md conn-map]
        (jdbc/metadata-result (.getCatalogs md)))
      (log/info "Successfully connected to the instance")
      conn-map))

(defn ->conn-map*
  [config]
  (let [conn-map {:dbtype "sqlserver"
                  :dbname (or (config "database") "") ;; database is optional - if omitted it is set to an empty string
                  :host (config "host")
                  :port (or (config "port") 0) ;; port is optional - if omitted it is set to 0 for a dynamic port
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
                          :trustServerCertificate false)
                   conn-map)]
    ;; returns conn-map and logs on successful connection
    (check-connection conn-map)))

(def ->conn-map (memoize ->conn-map*))
