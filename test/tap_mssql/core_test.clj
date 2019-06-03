(ns tap-mssql.core-test
  (:require [clojure.test :refer [is deftest]]
            [tap-mssql.core :refer :all]))

(deftest add-int-column-to-catalog
  (is (= {:streams
          {"theologians"
           {:stream        "theologians"
            :tap_stream_id "test-bar-theologians"
            :table_name    "theologians"
            :schema        {:type       "object"
                            :properties {"name"          {:type    "integer"
                                                          :minimum -2147483648
                                                          :maximum 2147483647}
                                         "year_of_death" {:type    "integer"
                                                          :minimum -2147483648
                                                          :maximum 2147483647}}}
            :metadata      {:database-name        "test",
                            :schema-name          "bar",
                            :table-key-properties #{},
                            :is-view              false,
                            :properties
                            {"name"
                             {:inclusion           "available",
                              :sql-datatype        "int",
                              :selected-by-default true},
                             "year_of_death"
                             {:inclusion           "available",
                              :sql-datatype        "int",
                              :selected-by-default true}}}}
           "revivalists"
           {:stream        "revivalists"
            :tap_stream_id "test-bar-revivalists"
            :table_name    "revivalists"
            :schema        {:type       "object"
                            :properties {"name"          {:type    "integer"
                                                          :minimum -2147483648
                                                          :maximum 2147483647}
                                         "year_of_death" {:type    "integer"
                                                          :minimum -2147483648
                                                          :maximum 2147483647}}}
            :metadata      {:database-name        "test",
                            :schema-name          "bar",
                            :table-key-properties #{},
                            :is-view              false,
                            :properties
                            {"name"
                             {:inclusion           "available",
                              :sql-datatype        "int",
                              :selected-by-default true},
                             "year_of_death"
                             {:inclusion           "available",
                              :sql-datatype        "int",
                              :selected-by-default true}}}}}}
         (reduce add-column nil [{:table_name   "theologians"
                                  :table_cat    "test"
                                  :table_schem  "bar"
                                  :column_name  "name"
                                  :type_name    "int"
                                  :primary-key? false
                                  :is-view?     false}
                                 {:table_name   "theologians"
                                  :table_cat    "test"
                                  :table_schem  "bar"
                                  :column_name  "year_of_death"
                                  :type_name    "int"
                                  :primary-key? false
                                  :is-view?     false}
                                 {:table_name   "revivalists"
                                  :table_cat    "test"
                                  :table_schem  "bar"
                                  :column_name  "name"
                                  :type_name    "int"
                                  :primary-key? false
                                  :is-view?     false}
                                 {:table_name   "revivalists"
                                  :table_cat    "test"
                                  :table_schem  "bar"
                                  :column_name  "year_of_death"
                                  :type_name    "int"
                                  :primary-key? false
                                  :is-view?     false}]))))

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
                    :metadata      [{:metadata   {:database-name        "foo"
                                                  :schema-name          "bar"
                                                  :table-key-properties #{}
                                                  :is-view              false}
                                     :breadcrumb []}
                                    {:metadata   {:inclusion           "available"
                                                  :selected-by-default true
                                                  :sql-datatype        "int"}
                                     :breadcrumb [:properties "name"]}
                                    {:metadata   {:inclusion           "available"
                                                  :selected-by-default true
                                                  :sql-datatype        "int"}
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
                    :metadata      [{:metadata   {:database-name        "foo"
                                                  :schema-name          "bar"
                                                  :table-key-properties #{}
                                                  :is-view              false}
                                     :breadcrumb []}
                                    {:metadata   {:inclusion           "available"
                                                  :selected-by-default true
                                                  :sql-datatype        "int"}
                                     :breadcrumb [:properties "name"]}
                                    {:metadata   {:inclusion           "available"
                                                  :selected-by-default true
                                                  :sql-datatype        "int"}
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
                    :metadata      {:database-name        "foo"
                                    :schema-name          "bar"
                                    :table-key-properties #{}
                                    :is-view              false
                                    :properties
                                    {"name"          {:inclusion           "available"
                                                      :selected-by-default true
                                                      :sql-datatype        "int"}
                                     "year_of_death" {:inclusion           "available"
                                                      :selected-by-default true
                                                      :sql-datatype        "int"}}}}
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
                    :metadata      {:database-name        "foo"
                                    :schema-name          "bar"
                                    :table-key-properties #{}
                                    :is-view              false
                                    :properties
                                    {"name"          {:inclusion           "available"
                                                      :selected-by-default true
                                                      :sql-datatype        "int"}
                                     "year_of_death" {:inclusion           "available"
                                                      :selected-by-default true
                                                      :sql-datatype        "int"}}}}}}]
             (catalog->serialized-catalog catalog))))))

(deftest verify-extra-arguments-does-not-throw
  (is (parse-opts ["--properties" "foo"])))

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
