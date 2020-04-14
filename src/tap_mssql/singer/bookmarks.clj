(ns tap-mssql.singer.bookmarks)

(defn get-bookmark-keys
  "Gets the possible bookmark keys to use for sorting, falling back to
  `nil`.

  Priority is in this order:
  `replicationkey` > `timestamp` field > `table-key-properties`"
  [catalog stream-name]
  (let [replication-key (get-in catalog ["streams"
                                         stream-name
                                         "metadata"
                                         "replication-key"])
        timestamp-column (first
                          (first
                           (filter (fn [[k v]] (= "timestamp"
                                                  (v "sql-datatype")))
                                   (get-in catalog ["streams"
                                                    stream-name
                                                    "metadata"
                                                    "properties"]))))
        is-view? (get-in catalog ["streams" stream-name "metadata" "is-view"])
        table-key-properties (if is-view?
                               (get-in catalog ["streams"
                                                stream-name
                                                "metadata"
                                                "view-key-properties"])
                               (get-in catalog ["streams"
                                                stream-name
                                                "metadata"
                                                "table-key-properties"]))]
    (if (not (nil? replication-key))
      [replication-key]
      (if (not (nil? timestamp-column))
        [timestamp-column]
        (when (not (empty? table-key-properties))
          table-key-properties)))))

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
