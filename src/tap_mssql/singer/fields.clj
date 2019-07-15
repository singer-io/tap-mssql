(ns tap-mssql.singer.fields)

(defn selected-field?
  [[field-name field-metadata]]
  (or (field-metadata "selected")
      (= (field-metadata "inclusion") "automatic")
      (and (field-metadata "selected-by-default")
           (not (contains? field-metadata "selected")))))

(defn get-selected-fields
  [catalog stream-name]
  (let [metadata-properties
        (get-in catalog ["streams" stream-name "metadata" "properties"])
        selected-fields (filter selected-field? metadata-properties)
        selected-field-names (map (comp name first) selected-fields)]
    selected-field-names))
