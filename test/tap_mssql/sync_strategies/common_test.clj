(ns tap-mssql.sync-strategies.common-test
  (:require [tap-mssql.sync-strategies.common :refer :all]
            [clojure.test :refer [is deftest]]))

(deftest names-are-sanitized []
  (is (= "[chicken]" (sanitize-names "chicken"))
      "Normal strings should be surrounded by []")
  (is (= "[chicken]]potpie]" (sanitize-names "chicken]potpie"))
      "Right square brackets should be closed and sanitized"))
