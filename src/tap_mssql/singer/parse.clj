(ns tap-mssql.singer.parse
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]))

(defn slurp-json
  [f]
  (-> f
      io/reader
      json/read))

(defn config
  "This function exists as a test seam"
  [config-file]
  (slurp-json config-file))


(defn state
  "This function exists as a test seam and for the post condition"
  [state-file]
  {:post [(map? %)]}
  (slurp-json state-file))

(defn catalog
  "This function exists as a test seam"
  [catalog-file]
  (slurp-json catalog-file))
