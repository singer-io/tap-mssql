(ns tap-const.in-situ-println-with-state-managed-by-paginator
  "tap-const.in-situ-println-with-state-managed-by-paginator is for
  exploring the idea of in-situ println statements being the primary way
  to write singer messages in a clojure tap. The primary difference
  between `record` and `paginator` is to show what can be done to liberate
  state messages from record messages.

  The idea is to make sync-record operate on a `new-state` arg and a
  `record` arg, returning the new-state on a successful write. Then, the
  thing processing that record can write state messages at whatever
  interval it feels comfortable with."
  (:require [clojure.data.json :as json]
            [clojure.string :as string]
            [tap-const.core :refer [log-infof]]
            [clojure.tools.nrepl.server :as nrepl-server])
  (:gen-class))

(def source (let [contents (partition-all
                            2
                            [{:id 1
                              :date "2019-04-19T09:40:50Z"}
                             {:id 2
                              :date "2019-04-19T09:41:40Z"}
                             {:id 3
                              :date "2019-04-19T09:41:50Z"}
                             {:id 4
                              :date "2019-04-19T09:41:54Z"}
                             {:id 5
                              :date "2019-04-19T09:41:58Z"}
                             {:id 6
                              :date "2019-04-19T09:42:06Z"}
                             {:id 7
                              :date "2019-04-19T09:42:12Z"}
                             {:id 8
                              :date "2019-04-19T09:42:15Z"}
                             {:id 9
                              :date "2019-04-19T09:42:19Z"}
                             {:id 10
                              :date "2019-04-19T09:42:23Z"}
                             {:id 11
                              :date "2019-04-19T15:10:10"}])]
              {:stream1 contents
               :skipped-stream2 contents
               :stream3 contents}))

(defn make-record
  [stream rec]
  {:stream (name stream)
   :record rec})

(defn make-message
  [type-kw value]
  (let [singer-type (string/upper-case (name type-kw))]
    (assoc value :type singer-type)))

(defn write-message
  [type-kw value]
  (println
   (json/write-str
    (make-message type-kw value))))

(defn sync-record
  "Function of current state, rec → new state based on record"
  [stream-name new-state rec]
  ;; `new-bookmark` being passed here is a bit awkward but I think it's
  ;; what makes the most sense. Really the record _may_ be useful for
  ;; deriving the new state but it's the thing that's calling sync-record
  ;; that actually is likely to be able to make that decision. If you need
  ;; to derive the new state from the record then you'd do it in the
  ;; calling function based on the rec before you passed it in here. The
  ;; reason you can't just do that here is because there's lots of
  ;; bookmarks that aren't derived from the record itself.
  (write-message :record (make-record stream-name rec))
  new-state)

(defn record-seq
  [data]
  (mapcat identity data))

(defn make-state
  [state stream-name bookmark]
  (assoc-in state [:bookmarks stream-name :position] bookmark))

(defn sync-stream
  "catalog, stream-name, current state → new state"
  [catalog stream-name state]
  (log-infof "Syncing stream %s" stream-name)
  (write-message
   :schema
   (get-in catalog [:streams stream-name]))
  ;; This is the logic that would be replaced with real sync logic. Most
  ;; notably the thing responsible for writing messages is the thing that
  ;; must be responsible for writing state messages.

  ;; data here is 'page's of records
  (let [data (source stream-name)
        current-bookmark (get-in state
                                 [:bookmarks
                                  stream-name
                                  :position]
                                 0)
        state-emission-rate 10
        bookmarks (drop (+ 1 current-bookmark) (range))]
    ;; The idea of sync-stream is still catalog, stream-name, current
    ;; state → new state but the design differs from the record emission
    ;; strategy. There, you essentially have to emit state every time you
    ;; emit a record because every time you emit a record you don't have
    ;; any context about what you had emitted before.
    ;;
    ;; The primary change here is that now instead of a reduction over
    ;; state we're simply mapping over state-to-be and records which
    ;; become the state-to-be once they're written. This lets us have very
    ;; granular states but only emit them every so often and could be
    ;; configured in whatever way we saw fit. If bookmarks needed to be
    ;; generated from the record rather than outside of the record then
    ;; they could be generated via mapping over the records rather than
    ;; generated ex nihilo as we're doing it here.

    ;; Take either the last state emitted or (if nothing was emitted) the
    ;; state as it was entered.
    (or (last
         (map (fn [state-page]
                (let [latest-state (last state-page)]
                  (write-message
                   :state {:value latest-state})
                  latest-state))
              ;; partition into state pages for processing.
              (partition-all
               state-emission-rate
               (map (partial sync-record stream-name)
                    ;; make new states over all the bookmarks. The new
                    ;; state must be the state as it should be _after_ the
                    ;; record is written.
                    (map (partial make-state state stream-name) bookmarks)
                    ;; Move to the current bookmark in the data, this
                    ;; allows us to ignore the pagination because we can
                    ;; express it as a lazy-seq.
                    (drop current-bookmark (record-seq data))))))
        state)))

(defn selected?
  [stream-name]
  ((complement #{:skipped-stream2}) stream-name))

(defn maybe-sync-stream
  [catalog state stream-name]
  (if (selected? stream-name)
    (sync-stream catalog stream-name state)
    (do (log-infof "Skipping stream %s"
                   stream-name)
        state)))

(defn do-sync
  [catalog state]
  ;; To make parallel you need to move state to an atom.
  (reduce (partial maybe-sync-stream catalog)
          state
          (keys source)))
