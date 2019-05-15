(ns tap-mssql.discover-empty-catalog-test
  (:require [clojure.test :refer [is deftest]]
            [clojure.java.io :as io]
            [tap-mssql.core :refer :all]))

(def test-db-config
  {:host (System/getenv "STITCH_TAP_MSSQL_TEST_DB_HOST")
   :user (System/getenv "STITCH_TAP_MSSQL_TEST_DB_USER")
   :password (System/getenv "STITCH_TAP_MSSQL_TEST_DB_PASSWORD")})

(defmacro with-out-and-err-to-dev-null
  [& body]
  `(let [null-out# (io/writer
                    (proxy [java.io.OutputStream] []
                      (write [& args#])))]
     (binding [*err* null-out#
               *out* null-out#]
       ~@body)))

(deftest ^:integration verify-mssql-version
  (is (nil?
       (with-out-and-err-to-dev-null
         (do-discovery test-db-config)))
      "Discovery ran succesfully and did not throw an exception"))

(deftest ^:integration verify-empty-catalog
  (is (= "{}\n"
         (with-out-str
           (do-discovery test-db-config)))))
