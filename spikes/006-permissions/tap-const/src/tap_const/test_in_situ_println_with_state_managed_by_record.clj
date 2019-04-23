(ns tap-const.test-in-situ-println-with-state-managed-by-paginator
  "Code for development runs of do-sync etc."
  (:require [tap-const.in-situ-println-with-state-managed-by-record :refer :all]))

(def catalog (let [schema {:type "object"
                           :properties {:id {:type "integer"}
                                        :date {:type "string"
                                               :format "date-time"}}}]
               (reduce (fn [catalog stream-name]
                         (assoc-in catalog [:streams stream-name]
                                   {:stream stream-name
                                    :key_properties ["id"]
                                    :schema schema}))
                       {}
                       (keys source))))

(require '[clojure.java.io :as io])

(def initial-state {:bookmarks {:stream1 {:position 3}
                                :stream3 {:position 7}}})

(defmacro with-out-and-err-to-dev-null
  [& body]
  `(let [null-out# (io/writer
                    (proxy [java.io.OutputStream] []
                      (write [& args#])))]
     (binding [*err* null-out#
               *out* null-out#]
       ~@body)))

(comment
  (with-out-and-err-to-dev-null
    (do-sync catalog initial-state))
  )

;; This is a convient way to run the sync through target-stitch in
;; dry-run mode.
(require '[clojure.java.shell :refer [sh]]
         '[clojure.string :as string]
         '[clojure.data.json :as json])
(defn run-test
  [state]
  (let [results (sh
                 "/usr/local/share/virtualenvs/target-stitch/bin/target-stitch"
                 "--dry-run"
                 :in (with-out-str (do-sync catalog state)))]
    (-> results
        (update :err string/split #"\n")
        (update :out (comp (partial map (fn [s]
                                          (try (json/read-str s)
                                               (catch Exception _ s))))
                           #(string/split % #"\n"))))))

(comment
  (string/split "" #"\n")
  (with-out-and-err-to-dev-null
    (run-test initial-state))
  (run-test initial-state)
  ;; => {:exit 0,
  ;;     :out
  ;;     ({"bookmarks" {"stream3" {"position" 5}, "stream1" {"position" 10}}}
  ;;      {"bookmarks" {"stream3" {"position" 10}, "stream1" {"position" 10}}}),
  ;;     :err
  ;;     ["INFO stream1 (7): Batch is valid"
  ;;      "INFO stream3 (5): Batch is valid"
  ;;      "INFO Exiting normally"]}
  )
