(ns tap-mssql.singer.transform
  (:require [clojure.string :as string]))

(defn transform-binary [binary]
  (when binary
    (apply str "0x" (map (comp string/upper-case
                               (partial format "%02x"))
                         binary))))

(defn transform-date [^java.sql.Date date]
  (when date
    (str date "T00:00:00+00:00")))

(defn transform-field [catalog stream-name [k v]]
  (condp contains? (get-in catalog ["streams" stream-name "metadata" "properties" k "sql-datatype"])
    #{"timestamp" "varbinary" "binary"}
    [k (transform-binary v)]

    #{"date"}
    [k (transform-date v)]

    [k v]))

(defn transform [catalog stream-name record]
  (into {} (map (partial transform-field catalog stream-name) record)))
