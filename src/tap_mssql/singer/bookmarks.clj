(ns tap-mssql.singer.bookmarks)

(defn get-full-bookmark-keys
  "Ensures the use of a stream's `table-key-properties` > `rowversion` as an intermediary bookmark for
  interrupted full syncs, else returns nil"
  [catalog stream-name]
  (let [is-view? (get-in catalog ["streams" stream-name "metadata" "is-view"])
        table-key-properties (if is-view?
                               (get-in catalog ["streams"
                                                stream-name
                                                "metadata"
                                                "view-key-properties"])
                               (get-in catalog ["streams"
                                                stream-name
                                                "metadata"
                                                "table-key-properties"]))
        timestamp-column (first
                          (first
                           (filter (fn [[k v]] (= "timestamp"
                                                  (v "sql-datatype")))
                                   (get-in catalog ["streams"
                                                    stream-name
                                                    "metadata"
                                                    "properties"]))))]

    (if (seq table-key-properties)
      table-key-properties
      (when (some? timestamp-column)
        [timestamp-column]
       ))))

(defn get-logical-bookmark-keys
  "Ensures the use of a stream's `table-key-properties` as an intermediary bookmark for
  interrupted logical syncs."
  [catalog stream-name]
  (get-in catalog ["streams"
                   stream-name
                   "metadata"
                   "table-key-properties"]))

(defn update-state [stream-name replication-key record state]
  (-> state
      (assoc-in ["bookmarks" stream-name "replication_key_value"]
                (get record replication-key))
      (assoc-in ["bookmarks" stream-name "replication_key_name"]
                replication-key)))

(defn update-last-pk-fetched [stream-name bookmark-keys state record]
  ;; bookmark-keys can be nil under certain conditions:
  ;; ex: if a view is missing view-key-properties
  (if bookmark-keys
    (assoc-in state
              ["bookmarks" stream-name "last_pk_fetched"]
              (zipmap bookmark-keys (map (partial get record) bookmark-keys)))
    state))
