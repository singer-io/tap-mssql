(ns tap-mssql.singer.messages
  (:require [tap-mssql.singer.schema :as singer-schema]
            [tap-mssql.singer.transform :as singer-transform]
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

(def df (-> (java.time.format.DateTimeFormatterBuilder.)
            (.appendPattern "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX")
            (.toFormatter)))

(defn- parse-timestamp-to-string [ts]
  (-> ts
      (.toLocalDateTime)
      (.atOffset java.time.ZoneOffset/UTC)
      (.format df)
      (.replace "000Z" "Z") ;; replacing microseconds because dates are saved as bookmarks and mssql does not support string datetimes being more precise than the column type
      (.replace ".000Z" "Z") ;; same with milliseconds
      ))

;; date - 0001-01-01 through 9999-12-31
;; datetime - 1753-01-01 through 9999-12-31 and 00:00:00 through 23:59:59.997 and no TZ
;; datetime2 - 0001-01-01 through 9999-12-31 and 00:00:00 through 23:59:59.9999999 and no TZ
;; datetimeoffset - 0001-01-01 through 9999-12-31 and 00:00:00 through 23:59:59.9999999 and -14:00 through +14:00
;; smalldatetime - 1900-01-01 through 2079-06-06 and 00:00:00 through 23:59:59 and no TZ
;; time - 00:00:00.0000000 through 23:59:59.9999999

;; 1) The tap should always write ISO8601 dates
;; 2) In the absence of time, we should add 00:00:00
;; 3) In the absence of a TZ, we should add Z (assume UTC)
;; 4) In the presence of a TZ, we should emit with the appropriate +/- 00:00
;; 5) In the absense of a date, we should emit the time in the format HH:mm:ss.ffffffff
(defn serialize-datetimes [k v]
  (condp contains? (type v)
    #{java.sql.Timestamp} ;; Java type for datetime, datetime2, and smalldatetime column types
    (parse-timestamp-to-string v)

    #{microsoft.sql.DateTimeOffset} ;; Java type for datetimeoffset columns
    (-> v
        (.getTimestamp)
        (parse-timestamp-to-string))

    #{java.sql.Time java.sql.Date} ;; Java type for Time/Date columns respectively
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
       "key_properties" (if (get-in catalog ["streams" stream-name "metadata" "is-view"])
                          (get-in catalog ["streams" stream-name "metadata" "view-key-properties"] [])
                          (get-in catalog ["streams" stream-name "metadata" "table-key-properties"]))
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
  (let [transformed-record (singer-transform/transform catalog stream-name record)
        record-message {"type"   "RECORD"
                        "stream" (calculate-destination-stream-name stream-name catalog)
                        "record" transformed-record}
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

(def record-buffer-size 100)

(defn write-state-buffered! [stream-name state]
  (swap! records-since-last-state inc)
  (if (> @records-since-last-state record-buffer-size)
    (do
      (reset! records-since-last-state 0)
      (write-state! stream-name state))
    state))
