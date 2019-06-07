(ns tap-mssql.test-utils
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
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

;; Need some kind of definition like 2017-test-db-config and
;; 2014-test-db-config which are dynamically created from the descriptins.
;;
;; That's actually nice because to create a test that runs against all of
;; them can be done with a simple mapping over a vector of configs and a
;; test that should run against a specific one can just reference it by
;; name.
;;
;; Or maybe the answer is to just read in the configs and patch them with
;; the needed creds and such so that they're in a a map exactly as they
;; already are in the testing-resources.json file.

(def test-db-config
  {"host"     (format "%s-test-mssql-2017.db.test.stitchdata.com"
                      (get-test-hostname))
   "user"     (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_USER")
   "password" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD")
   "port"     "1433"})

(comment
  (:testing-resource-name (meta (first test-db-configs)))
  (intern *ns* (symbol (str "test-db-config-" "name")))
  )

(def test-db-configs
  (let [testing-resources
        (-> "bin/testing-resources.json"
            io/reader
            json/read)]
    (map (fn [testing-resource]
           (let [config  (with-meta
                           {"user" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_USER")
                            "password" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD")
                            "port" "1433"
                            "host" (format "%s-test-mssql-%s.db.test.stitchdata.com"
                                           (get-test-hostname)
                                           (testing-resource "name"))}
                           {:testing-resource-name (testing-resource "name")})]
             ;; TODO: Proper way to define the one-off configs?
             (intern *ns*
                     (symbol (str "test-db-config-" (testing-resource "name")))
                     config)
             config))
         testing-resources)))

;;; This and the following needs to move to test-utils
(def ^:dynamic *test-db-config*)

;; Def Multiple Tests With the same assertions
(defmacro def-matrix-tests [test-name test-db-configs-form fixture-fn & body]
  (let [test-configs (eval test-db-configs-form)]
    (assert (symbol? test-name))
    (assert (every? (fn [test-db-config]
                      (-> test-db-config
                          meta
                          :testing-resource-name))
                    test-configs)
            "test-db-configs must eval to a sequence of objects with :testing-resource-name metadata or a map with a :db-configs key of this type.")
    (assert (fn? (eval fixture-fn))
            "fixture-fn must be a valid clojure.test fixture-fn")
    `(do
       ~@(map (fn [test-db-config]
                (let [testing-resource-name
                      (-> test-db-config meta :testing-resource-name)

                      test-symbol
                      (symbol (str test-name "-END-" testing-resource-name))]
                  `(deftest ~(vary-meta test-symbol
                                        assoc
                                        (keyword testing-resource-name)
                                        true
                                        :integration
                                        true)
                     (binding [*test-db-config* ~test-db-config]
                       (~fixture-fn (fn [] ~@body))))))
              test-configs))))

;; Def multiple assertions for the same test
(defmacro with-matrix-assertions [test-db-configs-form fixture-fn & body]
  (let [test-configs (eval test-db-configs-form)]
    (assert (every? (fn [test-db-config]
                      (map? test-db-config))
                    test-configs)
            "test-db-configs must eval to a sequence of config objects.")
    (assert (fn? (eval fixture-fn))
            "fixture-fn must be a valid clojure.test fixture-fn")
    `(do
       ~@(map (fn [test-db-config]
                `(binding [*test-db-config* ~test-db-config]
                   (~fixture-fn (fn [] ~@body))))
              test-configs))))
