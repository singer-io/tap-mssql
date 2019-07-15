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

(defn write!
  [message]
  {:pre [(valid? message)]}
  (-> message
      (json/write-str :value-fn serialize-datetimes)
      println))

(defn write-schema! [catalog stream-name]
  ;; TODO: Make sure that unsupported values are written with an empty schema
  (-> {"type" "SCHEMA"
       "stream" stream-name
       "key_properties" (get-in catalog ["streams"
                                         stream-name
                                         "metadata"
                                         "table-key-properties"])
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
  [stream-name state record]
  (let [record-message {"type" "RECORD"
                        "stream" stream-name
                        "record" record}
        version (get-in state ["bookmarks" stream-name "version"])]
    (if (nil? version)
      (write! record-message)
      (write! (assoc record-message "version" version)))))

(defn write-activate-version!
  [stream-name state]
  (write! {"type" "ACTIVATE_VERSION"
                   "stream" stream-name
                   "version" (get-in state
                                     ["bookmarks" stream-name "version"])})
  ;; This must return state, as it appears in the pipeline of a sync
  state)

(defn maybe-write-activate-version!
  "Writes activate version message if not in state"
  [stream-name state]
  ;; TODO: This assumes that uninterruptible full-table is the only mode,
  ;; this will need modified for incremental, CDC, and interruptible full
  ;; table to not change the table version in those modes unless needed
  ;; For now, always generate and return a new version
  (let [version-bookmark (get-in state ["bookmarks" stream-name "version"])
        new-state        (assoc-in state
                                   ["bookmarks" stream-name "version"]
                                   (now))]
    ;; Write activate version on first sync to get records flowing, and
    ;; never again so that the table only truncates at the end of the load
    ;; TODO: This will need changed (?), assumes that a full-table sync runs
    ;; 100% in a single tap run. It will need to be smarter than `nil?`
    ;; for these cases (?)
    (when (nil? version-bookmark)
      (write-activate-version! stream-name new-state))
    new-state))

(def records-since-last-state (atom 0))

(defn write-state-buffered! [stream-name state]
  (swap! records-since-last-state inc)
  (if (> @records-since-last-state 100)
    (do
      (reset! records-since-last-state 0)
      (write-state! stream-name state))
    state))

