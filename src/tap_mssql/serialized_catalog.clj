(ns tap-mssql.serialized-catalog)

(defn deserialize-stream-metadata
  [serialized-stream-metadata]
  (reduce (fn [metadata serialized-metadata-entry]
            (reduce (fn [entry-metadata [k v]]
                      (assoc-in
                       entry-metadata
                       (conj (serialized-metadata-entry "breadcrumb") k)
                       v))
                    metadata
                    (serialized-metadata-entry "metadata")))
          {}
          serialized-stream-metadata))

(defn get-unsupported-breadcrumbs
  [stream-schema-metadata]
  (->> (stream-schema-metadata "properties")
       (filter (fn [[k v]]
                 (= "unsupported" (v "inclusion"))))
       (map (fn [[k _]]
              ["properties" k]))))

(defn deserialize-stream-schema
  [serialized-stream-schema stream-schema-metadata]
  (let [unsupported-breadcrumbs (get-unsupported-breadcrumbs stream-schema-metadata)]
    (reduce (fn [acc unsupported-breadcrumb]
              (assoc-in acc unsupported-breadcrumb nil))
            serialized-stream-schema
            unsupported-breadcrumbs)))

(defn deserialize-stream
  [serialized-stream]
  {:pre [(map? serialized-stream)]}
  (as-> serialized-stream ss
    (update ss "metadata" deserialize-stream-metadata)
    (update ss "schema" deserialize-stream-schema (ss "metadata"))))

(defn deserialize-streams
  [serialized-streams]
  (reduce (fn [streams deserialized-stream]
            (assoc streams (deserialized-stream "tap_stream_id") deserialized-stream))
          {}
          (map deserialize-stream serialized-streams)))

(defn ->catalog
  [serialized-catalog]
  (update serialized-catalog "streams" deserialize-streams))
