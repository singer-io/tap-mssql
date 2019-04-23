(ns tap-const.in-situ-println-with-state-managed-by-record
  "tap-const.core is for exploring the idea of in-situ println
  statements being the primary way to write singer messages in a clojure
  tap"
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
  [stream-name state new-bookmark rec]
  ;; `new-bookmark` being passed here is a bit awkward but I think it's
  ;; what makes the most sense. Really the record _may_ be useful for
  ;; deriving the new state but it's the thing that's calling sync-record
  ;; that actually is likely to be able to make that decision. If you need
  ;; to derive the new state from the record then you'd do it in the
  ;; calling function based on the rec before you passed it in here. The
  ;; reason you can't just do that here is because there's lots of
  ;; bookmarks that aren't derived from the record itself.
  (write-message :record (make-record stream-name rec))
  (assoc-in state [:bookmarks stream-name :position] new-bookmark))

(defn record-seq
  [data]
  (mapcat identity data))

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
  (let [data (source stream-name)
        current-bookmark (get-in state
                                 [:bookmarks
                                  stream-name
                                  :position]
                                 0)]
    (let [new-state
          (reduce (partial apply sync-record stream-name)
                  state
                  (map vector
                       (drop current-bookmark
                             (range))
                       (drop current-bookmark
                             (record-seq data))))]
      ;; FIXME not good enough. One state message at the end of syncing
      ;; the entire stream. Need to emit state message at end of every
      ;; 'page' at least.
      (write-message :state {:value new-state})
      new-state)))

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
