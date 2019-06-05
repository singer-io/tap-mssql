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

(comment
  (require '[clojure.data.json :as json])

  (def m ^:hi [1 2 3])

  (meta m)
  )

(def test-db-config
  {"host"     (format "%s-test-mssql-2017.db.test.stitchdata.com"
                      (get-test-hostname))
   "user"     (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_USER")
   "password" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD")
   "port"     "1433"})

(comment
  (:testing-resource-name (meta (first test-db-configs)))
  )

(def test-db-configs
  (let [testing-resources
        (-> "bin/testing-resources.json"
            io/reader
            json/read)]
    ;; TODO: Create the special defs during this too? e.g., test-db-config-2017, etc.
    (map (fn [testing-resource]
           (with-meta
             {"user" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_USER")
              "password" (System/getenv "STITCH_TAP_MSSQL_TEST_DATABASE_PASSWORD")
              "port" "1433"
              "host" (format "%s-test-mssql-%s.db.test.stitchdata.com"
                             (get-test-hostname)
                             (testing-resource "name"))}
             {:testing-resource-name (testing-resource "name")}))
         testing-resources)))

;;; This and the following needs to move to test-utils
(def ^:dynamic *test-db-config*)

(defmacro def-matrix-tests [test-name test-db-configs & body]
  (let [configs (eval test-db-configs)]
    (assert (symbol? test-name))
    (assert (every? (fn [test-db-config]
                      (-> test-db-config
                          meta
                          :testing-resource-name))
                    configs)
            "test-db-configs must eval to a sequence of objects with :testing-resource-name metadata")
    `(do
       ~@(map (fn [test-db-config]
                (let [testing-resource-name
                      (-> test-db-config
                          meta
                          :testing-resource-name)

                      test-symbol
                      (symbol
                       (str test-name
                            "-"
                            testing-resource-name))]
                  `(do
                     (deftest ~test-symbol
                       (binding [*test-db-config* ~test-db-config]
                         ~@body))
                     (alter-meta! (var ~test-symbol)
                                  assoc
                                  ~(keyword testing-resource-name)
                                  true
                                  :integration true)
                     (var ~test-symbol))))
              configs))))

(comment
  ;; TODO: Can this be turned into a utility function for unmapping a
  ;; namesapce?
  (map (comp (partial ns-unmap *ns*) #(.sym %)) (filter (comp :test meta) (vals (ns-publics *ns*))))
  )
