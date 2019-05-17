(ns tap-mssql.core-test
  (:require [clojure.test :refer [is deftest]]
            [tap-mssql.core :refer :all]))

(deftest add-int-column-to-catalog
  (is (= {:streams
          {"theologians"
           {:stream "theologians",
            :tap-stream-id "theologians",
            :table-name "theologians",
            :schema {},
            :metadata {}},
           "revivalists"
           {:stream "revivalists",
            :tap-stream-id "revivalists",
            :table-name "revivalists",
            :schema {},
            :metadata {}}}}
         (reduce add-column empty-catalog [{:table_name "theologians"
                                            :table_cat "test"
                                            :column_name "name"}
                                           {:table_name "theologians"
                                            :table_cat "test"
                                            :column_name "year_of_death"}
                                           {:table_name "revivalists"
                                            :table_cat "test"
                                            :column_name "name"}
                                           {:table_name "revivalists"
                                            :table_cat "test"
                                            :column_name "year_of_death"}]))))
