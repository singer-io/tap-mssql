(ns tap-const.core
  (:require [clojure.tools.nrepl.server :as nrepl-server]
            [clojure.java.jdbc :as jdbc]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [with-test is run-tests test-vars]]
            [clojure.string :as string])
  (:gen-class))

;;; Note source is now infinite.
;;;
;;; Note that transformation logic is pushed down into this function
;;; rather than moved centrally. Reuse would be accomplished via library
;;; functions.
(def source (let [contents (map (fn [id]
                                  (when (= 0 (mod id 100))
                                    ;; This is so evaluating record
                                    ;; emission in the REPL is easily
                                    ;; interruptible and to simulate
                                    ;; stateful i/o wait.
                                    (Thread/sleep 1000))
                                  {:id id})
                                (range))]
              {:stream1 contents
               :skipped-stream2 contents
               :stream3 contents}))

(with-test
  (defn make-log-info
    [message-format & args]
    {:type :log
     :value {:level :info
             :message (apply format message-format args)}})

  (is (= {:type :log
          :value {:level :info
                  :message "This is a message with args foo, bar, and bat"}}
         (make-log-info "This is a message with args %s, %s, and %s"
                        "foo"
                        "bar"
                        "bat"))))

;;; with-test macro likely not used for production style code
(with-test
  (defn message-envelope?
    [message-envelope]
    (if (and (#{:schema :state :records} (:type message-envelope))
             (:value message-envelope))
      message-envelope))

  ;; tests
  (let [records-message-envelope
        {:type :records
         :value [{:id 1} {:id 2}]}]
    (is (= records-message-envelope
           (message-envelope? records-message-envelope))))
  (let [schema-message-envelope {:type :schema
                                 :value {:foo :bar}}]
    (is (= schema-message-envelope
           (message-envelope? schema-message-envelope))))
  (let [state-message-envelope {:type :state
                                :value {:foo :bar}}]
    (is (= state-message-envelope
           (message-envelope? state-message-envelope))))
  (is (not (message-envelope? {:type :state})))
  (is (not (message-envelope? {:type :records})))
  (is (not (message-envelope? {:foo :bar}))))

(defn get-singer-type
  [message-envelope]
  {:pre [(message-envelope? message-envelope)]
   :post [#{:schema :state :records}]}
  (:type message-envelope))

(with-test
  (defn log
    [singer-message]
    {:pre [(= :log (:type singer-message))
           (get-in singer-message [:value :level])
           (get-in singer-message [:value :message])]}
    (let [level-name (string/upper-case (name (get-in singer-message [:value :level])))]
      (binding [*out* *err*]
        (println (format "%s: %s"
                         level-name
                         (get-in singer-message [:value :message]))))))

  (is (= "INFO: Foo bar\n"
         (with-out-str (binding [*err* *out*]
                         (log (make-log-info "Foo bar"))))))
  (is (= "INFO: Foo bar bat\n"
         (with-out-str (binding [*err* *out*]
                         (log (make-log-info "Foo bar %s"
                                             "bat")))))))

;;; Purely for testing purposes
(defmacro with-out-and-err-to-dev-null
  [& body]
  `(let [null-out# (io/writer
                    (proxy [java.io.OutputStream] []
                      (write [& args#])))]
     (binding [*err* null-out#
               *out* null-out#]
       ~@body)))

(defmacro with-err-to-dev-null
  [& body]
  `(let [null-out# (io/writer
                    (proxy [java.io.OutputStream] []
                      (write [& args#])))]
     (binding [*err* null-out#]
       ~@body)))

(with-test
  (defn make-messages
    [stream-name message-envelope]
    {:pre [(message-envelope? message-envelope)]}
    (let [singer-type (get-singer-type message-envelope)
          singer-type-name (string/upper-case (name singer-type))
          value (:value message-envelope)]
      (case singer-type
        :schema [(update message-envelope
                         :value
                         assoc
                         :type singer-type-name)]
        :state [(assoc message-envelope
                       :value
                       {:type singer-type-name
                        :stream stream-name
                        :value value})]
        :records (map (fn [v]
                        {:type :record
                         :value {:stream stream-name
                                 :type "RECORD"
                                 :record v}})
                      value))))

  ;; tests
  (is (= [{:type :record
           :value {:stream :stream1
                   :type "RECORD"
                   :record {:id 0}}}
          {:type :record
           :value {:stream :stream1
                   :type "RECORD"
                   :record {:id 1}}}]
         (make-messages :stream1 {:type :records :value (take 2 (:stream1 source))})))
  (is (= [{:type :schema
           :value {:stream :stream1,
                   :key_properties ["id"],
                   :schema
                   {:type "object",
                    :properties
                    {:id {:type "integer"}, :date {:type "string", :format "date-time"}}},
                   :type "SCHEMA"}}]
         (make-messages :stream1
                        {:type :schema
                         :value {:stream :stream1,
                                 :key_properties ["id"],
                                 :schema
                                 {:type "object",
                                  :properties
                                  {:id {:type "integer"},
                                   :date {:type "string", :format "date-time"}}}}})))
  (is (= [{:type :state
           :value {:type "STATE" :stream :stream1 :value {:bookmarks {:stream1 {:id 11}}}}}]
         (make-messages :stream1 {:type :state
                                  :value {:bookmarks {:stream1 {:id 11}}}}))))

(defn write-message!
  [stream-name singer-message]
  ((comp println json/write-str) (:value singer-message))
  singer-message)

(with-test
  (defn write-messages!
    [stream-name message-envelope]
    {:pre [message-envelope? message-envelope]
     :post [message-envelope?]}
    (dorun
     (map (partial write-message! stream-name)
          (make-messages stream-name message-envelope)))
    message-envelope)

  ;; tests
  ;; records
  (let [records-message-envelope
        {:type :records
         :value (take 2 (:stream1 source))}]
    (is (= records-message-envelope
           (with-out-and-err-to-dev-null
             (write-messages! :stream1 records-message-envelope))))
    (is (= "{\"stream\":\"stream1\",\"type\":\"RECORD\",\"record\":{\"id\":0}}\n{\"stream\":\"stream1\",\"type\":\"RECORD\",\"record\":{\"id\":1}}\n"
           (with-out-str (write-messages! :stream1 records-message-envelope)))))
  ;; schema
  (let [schema-message-envelope
        {:type :schema
         :value {:stream         :stream1,
                 :key_properties ["id"],
                 :schema
                 {:type "object",
                  :properties
                  {:id   {:type "integer"},
                   :date {:type "string", :format "date-time"}}}}}]
    (is (= schema-message-envelope
           (with-out-and-err-to-dev-null
             (write-messages! :stream1 schema-message-envelope))))
    (is (= "{\"stream\":\"stream1\",\"key_properties\":[\"id\"],\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"date\":{\"type\":\"string\",\"format\":\"date-time\"}}},\"type\":\"SCHEMA\"}\n"
           (with-out-str (write-messages! :stream1 schema-message-envelope)))))
  ;; state
  (let [state-message-envelope
        {:type :state
         :value {:bookmarks {:stream1 {:id 11}}}}]
    (is (= state-message-envelope
           (with-out-and-err-to-dev-null
             (write-messages! :stream1 state-message-envelope))))
    (is (= "{\"type\":\"STATE\",\"stream\":\"stream1\",\"value\":{\"bookmarks\":{\"stream1\":{\"id\":11}}}}\n"
           (with-out-str (write-messages! :stream1 state-message-envelope))))))

(comment
  (let [records-message-envelope {:type :records
                                  :value (take 2 (:stream1 source))}
        messages (make-messages :stream1 records-message-envelope)]
    (with-out-and-err-to-dev-null
      (write-messages! :stream1 records-message-envelope))
    )
  )


(defn make-state
  [state stream-name bookmark]
  (assoc-in state [:bookmarks stream-name :id] bookmark))

(defn get-new-state
  [state stream-name data-page]
  (make-state state stream-name (:id (last data-page))))

;;; A stream-seq is a lazy sequence of schema followed by interleaved data
;;; pages and bookmarks. It _must_ end in a bookmark, not a data page.
(with-test
  (defn make-stream-seq
    "config, catalog, stream-name, current state → stream-seq"
    [config catalog stream-name state]
;;; This code derives the latest bookmark from the current page of data.
;;; Could just as easily have an infinite stream of bookmarks and the
;;; pages that would produce them.
;;;
;;; It also assumes a true lazy sequence, not an exhaustible iterator of
;;; some kind. We would need an iterator seq or something if that's the
;;; case. Will be solved by the memory management spike.
    (lazy-cat
     [{:type :schema
       :value (get-in catalog [:streams stream-name])}]
     (let [data-pages (partition-all (:page-size config 20)
                                     (let [data (source stream-name)
                                           data (drop (get-in state [:bookmarks stream-name :id] 0)
                                                      data)
                                           {:keys [max-records]} config]
                                       (if max-records
                                         (take max-records data)
                                         data)))]
       (interleave (map (fn [data-page]
                          {:type :records
                           :value data-page})
                        data-pages)
                   (map (comp (fn [state]
                           {:type :state
                            :value state})
                              (partial get-new-state state stream-name))
                        data-pages))))
    ;; It's important that this is the tail here since it needs to return the value
    )

  ;; tests
  (let [config {:max-records 10}
        catalog {:streams
                 {:stream1
                  {:stream         :stream1,
                   :key_properties ["id"],
                   :schema
                   {:type "object",
                    :properties
                    {:id   {:type "integer"},
                     :date {:type "string", :format "date-time"}}}}}}
        stream-name :stream1
        state {:bookmarks {:stream1 {:id 5}}}]
    (is (= [{:type :schema
             :value {:stream :stream1
                     :key_properties ["id"]
                     :schema {:type "object"
                              :properties {:id {:type "integer"}
                                           :date {:type "string", :format "date-time"}}}}}
            {:type :records
             :value [{:id 5} {:id 6} {:id 7} {:id 8} {:id 9}  {:id 10} {:id 11} {:id 12} {:id 13} {:id 14}]}
            {:type :state
             :value {:bookmarks {:stream1 {:id 14}}}}]
           (make-stream-seq config catalog stream-name state)))))

;;; Incidental.
(defn selected?
  [stream-name]
  ((complement #{:skipped-stream2}) stream-name))

(defn state-message?
  [message-envelope]
  {:pre [(message-envelope? message-envelope)]}
  (and (= :state (get-singer-type message-envelope))
       (:value message-envelope)))

(with-test
  (defn write-stream!
    [stream-name stream-seq]
    {:post [state-message?]}
    (log (make-log-info "Writing stream %s" stream-name))
    (let [last-state (last (map (partial write-messages! stream-name)
                                stream-seq))]
      (log (make-log-info "Finished writing stream %s" stream-name))
      last-state))

  ;; tests
  (let [config {:max-records 10}
        catalog {:streams
                 {:stream1
                  {:stream         :stream1,
                   :key_properties ["id"],
                   :schema
                   {:type "object",
                    :properties
                    {:id   {:type "integer"},
                     :date {:type "string", :format "date-time"}}}}}}
        stream-name :stream1
        state {:bookmarks {:stream1 {:id 5}}}
        stream-seq (make-stream-seq config catalog stream-name state)]
    (is (= {:type :state, :value {:bookmarks {:stream1 {:id 14}}}}
           (with-out-and-err-to-dev-null
             (write-stream! :stream1 stream-seq))))
    (is (= (format "%s\n"
                   (string/join "\n"
                                ["{\"stream\":\"stream1\",\"key_properties\":[\"id\"],\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"date\":{\"type\":\"string\",\"format\":\"date-time\"}}},\"type\":\"SCHEMA\"}"
                                 "{\"stream\":\"stream1\",\"type\":\"RECORD\",\"record\":{\"id\":5}}"
                                 "{\"stream\":\"stream1\",\"type\":\"RECORD\",\"record\":{\"id\":6}}"
                                 "{\"stream\":\"stream1\",\"type\":\"RECORD\",\"record\":{\"id\":7}}"
                                 "{\"stream\":\"stream1\",\"type\":\"RECORD\",\"record\":{\"id\":8}}"
                                 "{\"stream\":\"stream1\",\"type\":\"RECORD\",\"record\":{\"id\":9}}"
                                 "{\"stream\":\"stream1\",\"type\":\"RECORD\",\"record\":{\"id\":10}}"
                                 "{\"stream\":\"stream1\",\"type\":\"RECORD\",\"record\":{\"id\":11}}"
                                 "{\"stream\":\"stream1\",\"type\":\"RECORD\",\"record\":{\"id\":12}}"
                                 "{\"stream\":\"stream1\",\"type\":\"RECORD\",\"record\":{\"id\":13}}"
                                 "{\"stream\":\"stream1\",\"type\":\"RECORD\",\"record\":{\"id\":14}}"
                                 "{\"type\":\"STATE\",\"stream\":\"stream1\",\"value\":{\"bookmarks\":{\"stream1\":{\"id\":14}}}}"]))
           ;; Should discard STDERR by default
           (with-out-str
             (write-stream! :stream1 stream-seq))))))

(defn maybe-sync-stream!
  "catalog, state, stream-name → new state"
  [config catalog state stream-name]
  (if (selected? stream-name)
    (let [stream (make-stream-seq config catalog stream-name state)]
      ;; returns state
      (write-stream! stream-name stream))
    (do (log (make-log-info "Skipping stream %s" stream-name))
        state)))

(with-test
  (defn do-sync
    [config catalog state]
    ;; To make parallel you need to move state to an atom.
    (reduce (partial maybe-sync-stream! config catalog)
            state
            (keys source)))

  (is {:type :state
       :value {:bookmarks {:stream1 {:max_pk {:id 10} :max_fetched_pk {:id 10}
                                        :max_lsn 5 :max_fetched_lsn 5}}}}
      (with-out-and-err-to-dev-null
        (let [stream-name :stream1
              config {:max-records 10}
              catalog {:streams
                       {stream-name
                        {:stream         stream-name,
                         :key_properties ["id"],
                         :schema
                         {:type "object",
                          :properties
                          {:id   {:type "integer"},
                           :date {:type "string", :format "date-time"}}}}

                        :stream3
                        {:stream         :stream3
                         :key_properties ["id"],
                         :schema
                         {:type "object",
                          :properties
                          {:id   {:type "integer"},
                           :date {:type "string", :format "date-time"}}}}}}
               state nil]
          (do-sync config catalog state)))))

(comment
  ;; We can use nil as the initial state.
  (assoc-in nil [:foo :bar :bat] 1)
  ;; => {:foo {:bar {:bat 1}}}

  (with-test
    (defn full-table-sync-complete?
      [stream-name state]
      (let [[max_pk max_fetched_pk]
            (map :id ((juxt :max_pk :max_fetched_pk)
                      (get-in state [:bookmarks stream-name])))]
        (when (< max_pk max_fetched_pk)
          (throw (ex-info "max pk is less than max fetched pk"
                          {:stream-name stream-name
                           :state state
                           :max_pk max_pk
                           :max_fetched_pk max_fetched_pk})))
        (= max_pk max_fetched_pk)))

    (is
     (let [stream-name :stream1
           incomplete-state
           {:bookmarks {stream-name {:max_pk {:id 10} :max_fetched_pk {:id 9}}}}]
       (not (full-table-sync-complete? stream-name incomplete-state))))
    (is
     (let [stream-name :stream1
           complete-state
           {:bookmarks {stream-name {:max_pk {:id 10} :max_fetched_pk {:id 10}}}}]
       (full-table-sync-complete? stream-name complete-state)))
    (is
     (thrown? clojure.lang.ExceptionInfo
              (let [stream-name :stream1
                    complete-state
                    {:bookmarks {stream-name {:max_pk {:id 10}
                                              :max_fetched_pk {:id 11}}}}]
                (full-table-sync-complete? stream-name complete-state)))))

  ;; An example run
  (let [stream-name :stream1]
    ;; Start with no state
    [nil
     ;; Emit some state from the full table sync
     {:bookmarks {stream-name {:max_pk {:id 10} :max_fetched_pk {:id 9}}}}
     ;; Emit final state from the full table sync
     {:bookmarks {stream-name {:max_pk {:id 10} :max_fetched_pk {:id 10}}}}
     ;; Keep the final state from the full table sync.
     {:bookmarks {stream-name {:max_pk {:id 10} :max_fetched_pk {:id 10}
                               :max_lsn 5 :max_fetched_lsn 4}}}
     {:bookmarks {stream-name {:max_pk {:id 10} :max_fetched_pk {:id 10}
                               :max_lsn 5 :max_fetched_lsn 5}}}])
  )



(defn nrepl-handler
  []
  (require 'cider.nrepl)
  (ns-resolve 'cider.nrepl 'cider-nrepl-handler))

(defonce the-nrepl-server
  (nrepl-server/start-server :bind "0.0.0.0"
                             :handler (nrepl-handler)))

(defn log-infof
  [message-format & args]
  (binding [*out* *err*]
    (println (apply format
                    (str "INFO " message-format)
                    args))))

(defn -main
  [& args]
  (log-infof "Started nrepl server at %s"
             (.getLocalSocketAddress (:server-socket the-nrepl-server)))
  (spit ".nrepl-port" (:port the-nrepl-server))
  (.start (Thread. #((loop []
                       (Thread/sleep 1000)
                       (recur))))))
