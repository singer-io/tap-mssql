(ns tap-mssql.singer.messages
  (:require [tap-mssql.singer.schema :as singer-schema]
            [clojure.data.json :as json]))

(defn now []
  ;; To redef in tests
  (System/currentTimeMillis))

(defn valid?
  [message]
  (and (#{"SCHEMA" "STATE" "RECORD" "ACTIVATE_VERSION"} (message "type"))
       (case (message "type")
         "SCHEMA"
         (message "schema")

         "STATE"
         (message "value")

         "RECORD"
         (message "record")

         "ACTIVATE_VERSION"
         (message "version"))))

;; Passed to the json serializer as value-converter fn
;; Needed to convert java.sql.Date types to json strings
(defn serialize-datetimes [k v]
  (condp contains? (type v)
    #{java.sql.Timestamp}
    (.. v toInstant toString)

    #{java.sql.Time java.sql.Date}
    (.toString v)

    v))

(def include-db-and-schema-names-in-messages? (atom false))

(defn calculate-destination-stream-name
  [stream-name catalog]
  (if @include-db-and-schema-names-in-messages?
    (get-in catalog ["streams" stream-name "tap_stream_id"])
    (get-in catalog ["streams" stream-name "table_name"])))

(defn write!
  [message]
  {:pre [(valid? message)]}
  (-> message
      (json/write-str :value-fn serialize-datetimes)
      println))

(defn write-schema! [catalog stream-name]
  ;; TODO: Make sure that unsupported values are written with an empty schema
  (-> {"type" "SCHEMA"
       "stream" (calculate-destination-stream-name stream-name catalog)
       "key_properties" (get-in catalog ["streams" stream-name "metadata" "table-key-properties"])
       "schema" (get-in catalog ["streams" stream-name "schema"])}
      (singer-schema/maybe-add-bookmark-properties-to-schema catalog stream-name)
      (singer-schema/maybe-add-deleted-at-to-schema catalog stream-name)
      (singer-schema/make-unsupported-schemas-empty catalog stream-name)
      write!))

(defn write-state!
  [stream-name state]
  (write! {"type" "STATE"
           "stream" stream-name
           "value" state})
  ;; This is very important. This function needs to return state so that
  ;; the outer reduce can pass it in to the next iteration.
  state)

(defn write-record!
  [stream-name state record catalog]
  (let [record-message {"type"   "RECORD"
                        "stream" (calculate-destination-stream-name stream-name catalog)
                        "record" record}
        version        (get-in state ["bookmarks" stream-name "version"])]
    (if (nil? version)
      (write! record-message)
      (write! (assoc record-message "version" version)))))

(defn write-activate-version!
  [stream-name catalog state]
  (write! {"type"    "ACTIVATE_VERSION"
           "stream"  (calculate-destination-stream-name stream-name catalog)
           "version" (get-in state
                             ["bookmarks" stream-name "version"])})
  ;; This must return state, as it appears in the pipeline of a sync
  state)

(defn maybe-write-activate-version!
  "Writes activate version message if not in state"
  [stream-name replication-method catalog state]
  (let [version-bookmark (get-in state ["bookmarks" stream-name "version"])
        resuming?        (get-in state ["bookmarks"
                                        stream-name "last_pk_fetched"] nil)
        new-state        (condp contains? replication-method
                           #{"FULL_TABLE"}
                           (if resuming?
                             state
                             (assoc-in state
                                       ["bookmarks" stream-name "version"]
                                       (now)))

                           #{"INCREMENTAL" "LOG_BASED"}
                           (if version-bookmark
                             state
                             (assoc-in state
                                       ["bookmarks" stream-name "version"]
                                       (now)))

                           (throw (IllegalArgumentException. (format "Replication Method for stream %s is invalid: %s"
                                                                     stream-name
                                                                     replication-method))))]
    ;; Write an activate_version message when we havent and its full table
    (when (and (nil? version-bookmark)
               (contains? #{"FULL_TABLE" "LOG_BASED"} replication-method))
      (write-activate-version! stream-name catalog new-state))
    new-state))

(def records-since-last-state (atom 0))

(defn write-state-buffered! [stream-name state]
  (swap! records-since-last-state inc)
  (if (> @records-since-last-state 100)
    (do
      (reset! records-since-last-state 0)
      (write-state! stream-name state))
    state))
