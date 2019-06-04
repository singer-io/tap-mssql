(ns tap-mssql.test-utils
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.logging :as log]))

(defmacro with-out-and-err-to-dev-null
  [& body]
  `(let [null-out# (io/writer
                    (proxy [java.io.OutputStream] []
                      (write [& args#])))]
     (binding [*err* null-out#
               *out* null-out#]
       (let [no-op# (constantly nil)]
         (with-redefs [log/log* no-op#]
           ~@body)))))

(defn get-test-hostname
  []
  (let [hostname (.getHostName (java.net.InetAddress/getLocalHost))]
    (cond (string/starts-with? hostname "taps-")
          hostname

          :default
          ;; This allows circleci run locally but in a container to talk
          ;; to the dev's test infrastructure.
          (or (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_HOST")
              "circleci"))))

(def test-db-config
  {"host"     (format "%s-test-mssql-2017.db.test.stitchdata.com"
                      (get-test-hostname))
   "user"     (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_USER")
   "password" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD")
   "port"     "1433"})
