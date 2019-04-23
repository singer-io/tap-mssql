(ns tap-const.test-lazy-seq
  (:require [tap-const.lazy-seq :refer :all])
  (:gen-class))

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

(def initial-state {:bookmarks {:stream1 {:id 37}
                                :stream3 {:id 775}}})

(def config {})

(require '[clojure.java.io :as io])

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
                 :in (with-out-str (do-sync (assoc config :max-records 1000)
                                            catalog
                                            state)))]
    (-> results
        (update :err string/split #"\n")
        (update :out (comp (partial map (fn [s]
                                          (try (json/read-str s)
                                               (catch Exception _ s))))
                           #(string/split % #"\n"))))))

(comment
  (do-sync (assoc config
                  :max-records 1000
                  :page-size 100)
           catalog
           initial-state)

  (with-out-and-err-to-dev-null
    (run-test initial-state))
  ;; => {:exit 0,
  ;;     :out
  ;;     ({"bookmarks" {"stream3" {"id" 775}, "stream1" {"id" 1036}}}
  ;;      {"value" {"bookmarks" {"stream3" {"id" 775}, "stream1" {"id" 1036}}},
  ;;       "type" "state",
  ;;       "bookmarks" {"stream3" {"id" 999}}}),
  ;;     :err
  ;;     ["INFO stream1 (1000): Batch is valid"
  ;;      "INFO stream3 (1000): Batch is valid"
  ;;      "INFO Exiting normally"]}
  (run-test initial-state)
  )

(defn -main [& args]
  (do-sync catalog initial-state))
