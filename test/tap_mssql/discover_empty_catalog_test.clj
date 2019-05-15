(ns tap-mssql.discover-empty-catalog-test
  (:require [clojure.test :refer [is deftest]]
            [tap-mssql.core :refer :all]))

(def test-db-config
  {:host (System/getenv "STITCH_TAP_MSSQL_TEST_DB_HOST")
   :user (System/getenv "STITCH_TAP_MSSQL_TEST_DB_USER")
   :password (System/getenv "STITCH_TAP_MSSQL_TEST_DB_PASSWORD")})

(deftest ^:integration verify-mssql-version
  (is (do-discovery test-db-config)))
