(ns tap-mssql.singer.schema)

(defn make-unsupported-schemas-empty [schema-message catalog stream-name]
  (let [schema-keys (get-in catalog ["streams" stream-name "metadata" "properties"])
        unsupported-keys (map first (filter #(= "unsupported" ((second %) "inclusion"))
                                            (seq schema-keys)))]
    (reduce (fn [msg x] (assoc-in msg ["schema" "properties" x] {}))
            schema-message
            unsupported-keys)))

(defn maybe-add-deleted-at-to-schema [schema-message catalog stream-name]
  (if (= "LOG_BASED"(get-in catalog ["streams" stream-name "metadata" "replication-method"]))
    (assoc-in schema-message ["schema" "properties" "_sdc_deleted_at"] {"type" ["string" "null"]
                                                                        "format" "date-time"})
    schema-message))

(defn maybe-add-bookmark-properties-to-schema [schema-message catalog stream-name]
  ;; Add or don't and return message
  (let [replication-key (get-in catalog ["streams"
                                         stream-name
                                         "metadata"
                                         "replication-key"])]
    (if replication-key
      (assoc schema-message "bookmark_properties" [replication-key])
      schema-message)))
