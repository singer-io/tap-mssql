(ns tap-mssql.core-test
  (:require [clojure.test :refer [is deftest]]
            [tap-mssql.core :refer :all]))

(deftest add-int-column-to-catalog
  (is (= {:streams
          {"theologians"
           {:stream        "theologians"
            :tap-stream-id "theologians"
            :table-name    "theologians"
            :schema        {:type       "object"
                            :properties {"name"          {:type    "integer"
                                                          :minimum -2147483648
                                                          :maximum 2147483647}
                                         "year_of_death" {:type    "integer"
                                                          :minimum -2147483648
                                                          :maximum 2147483647}}}
            :metadata      {}}
           "revivalists"
           {:stream        "revivalists"
            :tap-stream-id "revivalists"
            :table-name    "revivalists"
            :schema        {:type       "object"
                            :properties {"name"          {:type    "integer"
                                                          :minimum -2147483648
                                                          :maximum 2147483647}
                                         "year_of_death" {:type    "integer"
                                                          :minimum -2147483648
                                                          :maximum 2147483647}}}
            :metadata      {}}}}
         (reduce add-column empty-catalog [{:table_name  "theologians"
                                            :table_cat   "test"
                                            :column_name "name"
                                            :type_name "int"}
                                           {:table_name  "theologians"
                                            :table_cat   "test"
                                            :column_name "year_of_death"
                                            :type_name "int"}
                                           {:table_name  "revivalists"
                                            :table_cat   "test"
                                            :column_name "name"
                                            :type_name "int"}
                                           {:table_name  "revivalists"
                                            :table_cat   "test"
                                            :column_name "year_of_death"
                                            :type_name "int"}]))))

(comment
  ;; Run all loaded tests
  (do
    (require '[clojure.string :as string])
    (apply clojure.test/run-tests (->> (all-ns)
                                       (map ns-name)
                                       (filter #(string/starts-with? % "tap-mssql."))
                                       (filter #(string/ends-with? % "-test")))))

  (clojure.test/run-tests *ns*)
  )
