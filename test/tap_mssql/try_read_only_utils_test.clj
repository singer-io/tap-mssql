(ns tap-mssql.try-read-only-utils-test
  (:require [tap-mssql.utils :refer [try-read-only]]
            [clojure.test :refer :all]
            [tap-mssql.test-utils :refer [sql-server-exception]])
  (:import [java.sql Date]))


(deftest ^:integration verify-application-intent-only-set-if-body-succeeds
  (is (= "ReadOnly"
         (:ApplicationIntent (try-read-only [db-spec {}]
                               db-spec)))))

(deftest ^:integration verify-application-intent-only-unset-if-body-fails-first-time
  (let [times (atom 0)]
    (is (= nil
           (:ApplicationIntent
            (try-read-only [db-spec {:ApplicationIntent "ReadOnly"}]
                           (when (= 0 @times)
                             (swap! times inc)
                             (throw (sql-server-exception)))
                           db-spec))))))

(deftest ^:integration verify-application-intent-only-unset-if-body-fails-continuously
  (is (thrown-with-msg?
       com.microsoft.sqlserver.jdbc.SQLServerException
       #"__TEST_BOOM__"
       (try-read-only [db-spec {:ApplicationIntent "ReadOnly"}]
                      (throw (sql-server-exception))))))
