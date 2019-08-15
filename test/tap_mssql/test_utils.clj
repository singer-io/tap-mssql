(ns tap-mssql.test-utils
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [clojure.data.json :as json]
            [clojure.test :refer [deftest]]))

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

(def test-db-config
  "Default to local docker instance."
  {"host"     "localhost"
   "user"     (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_USER")
   "password" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD")
   "port"     (or (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_PORT") "1433")})

(def test-db-configs
  "Maps over `bin/testing-resources.json` and creates a list of tap config
  objects based on its contents."
  ;; TODO: Recover support for RDS instances from git history if needed.
  [test-db-config])

(def ^:dynamic *test-db-config*)

;; Def multiple assertions for the same test
(defmacro with-matrix-assertions
  "Expands to a copy of the passed in `~@body` forms for each config in
  `test-db-configs-form` that is wrapped with an anonymous function that is
  passed into `fixture-fn`.

  Within `fixture-fn` and the wrapped forms, the
  symbol `test-db-config` is bound to the current config.

  NOTE: At execution time, this shadows any other `test-db-config` in the
  namespace."
  [test-db-configs-form fixture-fn & body]
  (let [test-configs (eval test-db-configs-form)]
    (assert (every? (fn [test-db-config]
                      (map? test-db-config))
                    test-configs)
            "test-db-configs must eval to a sequence of config objects.")
    (assert (fn? (eval fixture-fn))
            "fixture-fn must be a valid clojure.test fixture-fn")
    `(do
       ~@(map (fn [test-db-config]
                ;; `let` here provides ease of use without dynamic binding
                ;; at the cost of shadowing test-db-config if imported.
                ;; The benefit is that no code needs to change to add or
                ;; remove the matrix.
                `(let [~(symbol "test-db-config") ~test-db-config]
                   (~fixture-fn (fn [] ~@body) ~(symbol "test-db-config"))))
              test-configs))))
