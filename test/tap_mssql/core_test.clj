(ns tap-mssql.core-test
  (:require [clojure.test :refer [is deftest]]
            [tap-mssql.core :refer :all]))

(defn get-serialized-catalog-entry [serialized-catalog stream-name]
  (first (filter (comp (partial = stream-name)
                       #(get % "stream"))
                 (serialized-catalog "streams"))))

(defn get-serialized-catalog-metadata-for-breadcrumb
  [serialized-catalog-entry breadcrumb]
  ((first
    (filter (comp (partial = breadcrumb)
                  #(get % "breadcrumb"))
            (serialized-catalog-entry "metadata")))
   "metadata"))

(deftest add-int-column-to-catalog
  (is (= ["integer"]
         (let [catalog (add-column nil {:table_name   "theologians"
                                        :table_cat    "test"
                                        :table_schem  "bar"
                                        :column_name  "name"
                                        :type_name    "int"
                                        :primary-key? false
                                        :is-view?     false})]
           (get-in catalog
                   ["streams" "theologians" "schema" "properties" "name" "type"])))))

(deftest catalog->serialized-catalog-test
  (let [catalog (reduce add-column nil [{:table_name "catalog_test"
                                         :column_name "id"
                                         :type_name "int"
                                         :primary-key? false
                                         :is-view? false}
                                        {:table_name "unsupported_data_types"
                                         :column_name "rowversion"
                                         :type_name "rowversion"
                                         :is_nullable "YES"
                                         :unsupported? true}])
        serialized-catalog (catalog->serialized-catalog catalog)]
    (is (= catalog (serialized-catalog->catalog serialized-catalog)))
    ;; Specific Structure
    (is (map? (catalog "streams")))
    (is (every? (comp map? #(get % "metadata")) (vals (catalog "streams"))))
    (is (sequential? (serialized-catalog "streams")))
    (is (every? (comp sequential? #(get % "metadata")) (serialized-catalog "streams")))
    ;; Unsupported Type Replacement
    (is (nil? (get-in (serialized-catalog->catalog serialized-catalog)
                      ["streams" "unsupported_data_types" "schema" "properties" "rowversion"])))
    (is (= {} (get-in (get-serialized-catalog-entry serialized-catalog "unsupported_data_types")
                      ["schema" "properties" "rowversion"])))
    (is (= {"inclusion" "unsupported",
            "sql-datatype" "rowversion",
            "selected-by-default" false}
           (get-serialized-catalog-metadata-for-breadcrumb
            (get-serialized-catalog-entry serialized-catalog "unsupported_data_types")
            ["properties" "rowversion"]))))
  )

(deftest catalog->serialized-catalog-invalid-characters-test
  (let [catalog (reduce add-column nil [{:table_name "invalid_characters"
                                         :column_name "invalid_characters_ !#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"
                                         :type_name "int"
                                         :primary-key? false
                                         :is-view? false}
                                        {:table_name "invalid_characters"
                                         :column_name "invalid_characters_ !\"#$%&'()*+,-./:;<=>?@\\^_`{|}~"
                                         :type_name "int"}])
        serialized-catalog (catalog->serialized-catalog catalog)]
    (is (= catalog (serialized-catalog->catalog serialized-catalog)))
    ;; Property Validation
    (is (= {"type" ["integer"], "minimum" -2147483648, "maximum" 2147483647}
           (get-in (serialized-catalog->catalog serialized-catalog)
                   ["streams" "invalid_characters" "schema" "properties" "invalid_characters_ !#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"])))
    (is (= {"type" ["integer"], "minimum" -2147483648, "maximum" 2147483647}
           (get-in (serialized-catalog->catalog serialized-catalog)
                   ["streams" "invalid_characters" "schema" "properties" "invalid_characters_ !\"#$%&'()*+,-./:;<=>?@\\^_`{|}~"])))
    (is (= {"type" ["integer"], "minimum" -2147483648, "maximum" 2147483647}
           (get-in (get-serialized-catalog-entry serialized-catalog "invalid_characters")
                   ["schema" "properties" "invalid_characters_ !#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"])))
    (is (= {"type" ["integer"], "minimum" -2147483648, "maximum" 2147483647}
           (get-in (get-serialized-catalog-entry serialized-catalog "invalid_characters")
                   ["schema" "properties" "invalid_characters_ !\"#$%&'()*+,-./:;<=>?@\\^_`{|}~"])))
    ;; Metadata Validation
    (is (= {"inclusion" "available", "sql-datatype" "int", "selected-by-default" true}
           (get-in (serialized-catalog->catalog serialized-catalog)
                   ["streams" "invalid_characters" "metadata" "properties" "invalid_characters_ !#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"])))
    (is (= {"inclusion" "available", "sql-datatype" "int", "selected-by-default" true}
           (get-in (serialized-catalog->catalog serialized-catalog)
                   ["streams" "invalid_characters" "metadata" "properties" "invalid_characters_ !\"#$%&'()*+,-./:;<=>?@\\^_`{|}~"])))
    (is (= {"inclusion" "available", "sql-datatype" "int", "selected-by-default" true}
           (get-serialized-catalog-metadata-for-breadcrumb
            (get-serialized-catalog-entry serialized-catalog "invalid_characters")
            ["properties" "invalid_characters_ !#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"])))
    (is (= {"inclusion" "available", "sql-datatype" "int", "selected-by-default" true}
           (get-serialized-catalog-metadata-for-breadcrumb
            (get-serialized-catalog-entry serialized-catalog "invalid_characters")
            ["properties" "invalid_characters_ !\"#$%&'()*+,-./:;<=>?@\\^_`{|}~"])))))

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
