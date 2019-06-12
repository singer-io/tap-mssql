(ns tap-mssql.sync-interruptible-full-table-test
  (:require [clojure.test :refer [is deftest]]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.data.json :as json]
            [clojure.set :as set]
            [clojure.string :as string]
            [tap-mssql.core :refer :all]
            [tap-mssql.test-utils :refer [with-out-and-err-to-dev-null
                                          test-db-config
                                          test-db-configs
                                          *test-db-config*
                                          with-matrix-assertions]]))

(defn get-destroy-database-command
  [database]
  (format "DROP DATABASE %s" (:table_cat database)))

(defn maybe-destroy-test-db
  []
  (let [destroy-database-commands (->> (get-databases *test-db-config*)
                                       (filter non-system-database?)
                                       (map get-destroy-database-command))]
    (let [db-spec (config->conn-map *test-db-config*)]
      (jdbc/db-do-commands db-spec destroy-database-commands))))

(defn create-test-db
  []
  (let [db-spec (config->conn-map *test-db-config*)]
    (jdbc/db-do-commands db-spec ["CREATE DATABASE full_table_interruptible_sync_test"])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_interruptible_sync_test")
                         [(jdbc/create-table-ddl
                           "data_table"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]
                            [:deselected_value "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_interruptible_sync_test")
                         [(jdbc/create-table-ddl
                           "data_table_rowversion"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "int"]
                            [:rowversion "rowversion"]])])))

(defn populate-data
  []
  (jdbc/insert-multi! (-> (config->conn-map *test-db-config*)
                          (assoc :dbname "full_table_interruptible_sync_test"))
                      "data_table"
                      (take 1000 (map (partial hash-map :deselected_value nil :value) (range))))
  (jdbc/insert-multi! (-> (config->conn-map *test-db-config*)
                          (assoc :dbname "full_table_interruptible_sync_test"))
                      "data_table_rowversion"
                      (take 1000 (map (partial hash-map :value) (range)))))

(defn test-db-fixture [f]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db)
    (create-test-db)
    (populate-data)
    (f)))

(defn get-messages-from-output
  ([]
   (get-messages-from-output nil))
  ([table]
   (get-messages-from-output
    table
    (discover-catalog test-db-config)))
  ([table catalog]
   (get-messages-from-output table {} catalog))
  ([table state catalog]
   (as-> (with-out-str
           (try
             (do-sync test-db-config catalog state)
             (catch Exception e (when (not (:ignore (ex-data e)))
                                  (throw e )))))
       output
       (string/split output #"\n")
       (filter (complement empty?) output)
       (map json/read-str
            output)
       (if table
         (filter (comp (partial = (name table)) #(% "stream"))
                 output)
         output)
       (vec output))))

(defn select-stream
  [stream-name catalog]
  (assoc-in catalog ["streams" stream-name "metadata" "selected"] true))

(defn deselect-field
  [stream-name field-name catalog]
  (assoc-in catalog ["streams" stream-name "metadata" "properties" field-name "selected"] false))

;; How is this supposed to work?
;; - Check if sync is marked as FULL_TABLE
;; - Check if rowversion (e.g., timestamp) column exists
;; - If so, use that, otherwise, use PK (get-full-table-replication-key catalog stream-name) => ["rowversion"] or ["name", "age", "ssn"] (if rowversion not present)
;;    - Should `max_pk_values` be stored in state like mysql? Needed for CDC
;; - If no sortable PK exists, do uninterruptible full table (with log)

;; State Should Look like: {"bookmarks":{"my_table": {"version": 1235677879, "max_pk_values": {"field": "value", ...}, "last_pk_fetched": {"field: "value", ...}}}}}

(def record-count (atom 0))

(comment
  ;; Check if sequence is strictly increasing
  (apply < [1 2 3 4])

  (let [sample '("0x0000000000000A2E"
                "0x0000000000000884"
                "0x0000000000000AA5"
                "0x0000000000000AAE"
                "0x0000000000000A29"
                "0x00000000000008CA"
                "0x0000000000000A2D"
                "0x00000000000008C6"
                "0x0000000000000B7A"
                "0x00000000000009F1"
                "0x00000000000009A8"
                "0x000000000000082B"
                "0x000000000000099D"
                "0x000000000000093D"
                "0x0000000000000A3E")]
    (every? (comp (partial > 0) (partial apply compare)) (partition 2 (sort sample)))
      (sort sample))
  )

(deftest ^:integration verify-full-table-sync-with-rowversion-resumes-on-interruption
  (with-matrix-assertions test-db-configs test-db-fixture
    ;; Steps:
    ;; Sync partially, a table with row version, interrupted at some point
    ;;     -- e.g., (with-redefs [valid-message? (fn [msg] (if (some-atom-thing-changes-after x calls) (throw...) (valid-message? msg)))] ... )
    (let [old-write-record write-record!]
      (with-redefs [write-record! (fn [stream-name record]
                                    ;; Call inc N times (using atom to track), then throw
                                    (swap! record-count inc)
                                    (if (> @record-count 600)
                                      (do
                                        (reset! record-count 0)
                                        (throw (ex-info "Interrupting!" {:ignore true})))
                                      (old-write-record stream-name record)))]
        (let [first-messages (->> (discover-catalog test-db-config)
                                  (select-stream "data_table_rowversion")
                                  (get-messages-from-output "data_table_rowversion"))
              first-state (last first-messages)]
          (is (valid-state? first-state))
          (is (= "full_table_interruptible_sync_test-dbo-data_table_rowversion"
                 ((-> first-messages
                      first)
                  "tap_stream_id")))
          ;; Strictly increasing rowversion
          (is (every? (comp (partial > 0)
                            (partial apply compare))
                      (->> first-messages
                           (filter #(= "RECORD" (% "type")))
                           (map #(get-in % ["record" "rowversion"]))
                           (partition 2 1))))
          ;; Format of transformed rowversion field
          (is (every? (partial re-matches #"0x[0-9A-F]{16}")
                      (->> first-messages
                           (filter #(= "RECORD" (% "type")))
                           (map #(get-in % ["record" "rowversion"])))))
          ;; Last state emitted has the rowversion of the last record emitted before that state
          (is (let [[activate-version initial-state] (->> first-messages
                                                          (drop 3) ;; Drop initial schema, state, and activate_version
                                                          (partition 2 1)
                                                          (drop-while (fn [[a b]] (not= "STATE" (b "type"))))
                                                          first)]
                (= (activate-version "version") (get-in initial-state ["value" "bookmarks" "data_table_rowversion" "version"]))))
          ;; Activate Version on first sync
          (is (= "ACTIVATE_VERSION"
                 ((->> first-messages
                       (second)) "type")))
          ;;   - Record count (if we use a consistent method of blowing up)
          (is (= 603 ;; 600 records, schema, state, activate_version
                 (count first-messages))))
        ))
    ))
