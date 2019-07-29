(ns tap-mssql.singer.transform
  (:require [clojure.string :as string]))

(defn transform-rowversion [rowversion]
  (when rowversion
    (apply str "0x" (map (comp string/upper-case
                               (partial format "%02x"))
                         rowversion))))

(defn transform-field [catalog stream-name [k v]]
  (condp = (get-in catalog ["streams" stream-name "metadata" "properties" k "sql-datatype"])
    "timestamp"
    [k (transform-rowversion v)]
    ;; Other cases?
    [k v]))

(defn transform [catalog stream-name record]
  (into {} (map (partial transform-field catalog stream-name) record)))
