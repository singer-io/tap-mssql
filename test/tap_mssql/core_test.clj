(ns tap-mssql.core-test
  (:require [clojure.test :refer [is deftest]]
            [tap-mssql.core :refer :all]))

(deftest add-int-column-to-catalog
  (is (= {:streams
          {"theologians"
           {:stream        "theologians"
            :tap_stream_id "theologians"
            :table_name    "theologians"
            :schema        {:type       "object"
                            :properties {"name"          {:type    "integer"
                                                          :minimum -2147483648
                                                          :maximum 2147483647}
                                         "year_of_death" {:type    "integer"
                                                          :minimum -2147483648
                                                          :maximum 2147483647}}}
            :metadata      {:properties
                            {"name"          {:inclusion "available"}
                             "year_of_death" {:inclusion "available"}}}}
           "revivalists"
           {:stream        "revivalists"
            :tap_stream_id "revivalists"
            :table_name    "revivalists"
            :schema        {:type       "object"
                            :properties {"name"          {:type    "integer"
                                                          :minimum -2147483648
                                                          :maximum 2147483647}
                                         "year_of_death" {:type    "integer"
                                                          :minimum -2147483648
                                                          :maximum 2147483647}}}
            :metadata      {:properties
                            {"name"          {:inclusion "available"}
                             "year_of_death" {:inclusion "available"}}}}}}
         (reduce add-column empty-catalog [{:table_name  "theologians"
                                            :table_cat   "test"
                                            :column_name "name"
                                            :type_name   "int"}
                                           {:table_name  "theologians"
                                            :table_cat   "test"
                                            :column_name "year_of_death"
                                            :type_name   "int"}
                                           {:table_name  "revivalists"
                                            :table_cat   "test"
                                            :column_name "name"
                                            :type_name   "int"}
                                           {:table_name  "revivalists"
                                            :table_cat   "test"
                                            :column_name "year_of_death"
                                            :type_name   "int"}]))))

(deftest catalog->serialized-catalog-test
  (let [expected-catalog
        {:streams [{:stream        "theologians"
                    :tap_stream_id "theologians"
                    :table_name    "theologians"
                    :schema        {:type       "object"
                                    :properties {"name"          {:type    "integer"
                                                                  :minimum -2147483648
                                                                  :maximum 2147483647}
                                                 "year_of_death" {:type    "integer"
                                                                  :minimum -2147483648
                                                                  :maximum 2147483647}}}
                    :metadata      [{:metadata   {:inclusion "available"}
                                     :breadcrumb [:properties "name"]}
                                    {:metadata   {:inclusion "available"}
                                     :breadcrumb [:properties "year_of_death"]}]}
                   {:stream        "revivalists"
                    :tap_stream_id "revivalists"
                    :table_name    "revivalists"
                    :schema        {:type       "object"
                                    :properties {"name"          {:type    "integer"
                                                                  :minimum -2147483648
                                                                  :maximum 2147483647}
                                                 "year_of_death" {:type    "integer"
                                                                  :minimum -2147483648
                                                                  :maximum 2147483647}}}
                    :metadata      [{:metadata   {:inclusion "available"}
                                     :breadcrumb [:properties "name"]}
                                    {:metadata   {:inclusion "available"}
                                     :breadcrumb [:properties "year_of_death"]}]}]}]
    (is (= expected-catalog
           (let [catalog
                 {:streams
                  {"theologians"
                   {:stream        "theologians"
                    :tap_stream_id "theologians"
                    :table_name    "theologians"
                    :schema        {:type       "object"
                                    :properties {"name"          {:type    "integer"
                                                                  :minimum -2147483648
                                                                  :maximum 2147483647}
                                                 "year_of_death" {:type    "integer"
                                                                  :minimum -2147483648
                                                                  :maximum 2147483647}}}
                    :metadata      {:properties
                                    {"name"          {:inclusion "available"}
                                     "year_of_death" {:inclusion "available"}}}}
                   "revivalists"
                   {:stream        "revivalists"
                    :tap_stream_id "revivalists"
                    :table_name    "revivalists"
                    :schema        {:type       "object"
                                    :properties {"name"          {:type    "integer"
                                                                  :minimum -2147483648
                                                                  :maximum 2147483647}
                                                 "year_of_death" {:type    "integer"
                                                                  :minimum -2147483648
                                                                  :maximum 2147483647}}}
                    :metadata      {:properties
                                    {"name"          {:inclusion "available"}
                                     "year_of_death" {:inclusion "available"}}}}}}]
             (catalog->serialized-catalog catalog))))))

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
