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
                                          with-matrix-assertions]]))

(defn get-destroy-database-command
  [database]
  (format "DROP DATABASE %s" (:table_cat database)))

(defn maybe-destroy-test-db
  [config]
  (let [destroy-database-commands (->> (get-databases config)
                                       (filter non-system-database?)
                                       (map get-destroy-database-command))]
    (let [db-spec (config->conn-map config)]
      (jdbc/db-do-commands db-spec destroy-database-commands))))

(defn create-test-db
  [config]
  (let [db-spec (config->conn-map config)]
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
                            [:rowversion "rowversion"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_interruptible_sync_test")
                         [(jdbc/create-table-ddl
                           "table_with_unsupported_pk"
                           [[:id "smallmoney NOT NULL PRIMARY KEY"]
                            [:value "int"]])])
    (jdbc/db-do-commands (assoc db-spec :dbname "full_table_interruptible_sync_test")
                         [(jdbc/create-table-ddl
                           "table_with_unsupported_column"
                           [[:id "uniqueidentifier NOT NULL PRIMARY KEY DEFAULT NEWID()"]
                            [:value "smallmoney"]])])))

(defn populate-data
  [config]
  (jdbc/insert-multi! (-> (config->conn-map config)
                          (assoc :dbname "full_table_interruptible_sync_test"))
                      "data_table"
                      (take 200 (map (partial hash-map :deselected_value nil :value) (range))))
  (jdbc/insert-multi! (-> (config->conn-map config)
                          (assoc :dbname "full_table_interruptible_sync_test"))
                      "data_table_rowversion"
                      (take 200 (map (partial hash-map :value) (range))))
  (jdbc/insert-multi! (-> (config->conn-map config)
                          (assoc :dbname "full_table_interruptible_sync_test"))
                      "table_with_unsupported_pk"
                      (take 200 (map #(hash-map :id (+ % 0.05) :value %) (range))))
  (jdbc/insert-multi! (-> (config->conn-map config)
                          (assoc :dbname "full_table_interruptible_sync_test"))
                      "table_with_unsupported_column"
                      (take 200 (map #(hash-map :value (+ % 0.05)) (range)))))

(defn test-db-fixture [f config]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db config)
    (create-test-db config)
    (populate-data config)
    (f)))

(defn get-messages-from-output
  ([config]
   (get-messages-from-output config nil))
  ([config table]
   (get-messages-from-output
    config
    table
    (discover-catalog config)))
  ([config table catalog]
   (get-messages-from-output config table {} catalog))
  ([config table state catalog]
   (as-> (with-out-str
           (try
             (do-sync config catalog state)
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
  (-> (assoc-in catalog ["streams" stream-name "metadata" "selected"] true)
      (assoc-in ["streams" stream-name "metadata" "replication-method"] "FULL_TABLE")))

(defn deselect-field
  [stream-name field-name catalog]
  (assoc-in catalog ["streams" stream-name "metadata" "properties" field-name "selected"] false))

(def record-count (atom 0))


(deftest ^:integration verify-unsupported-column-has-empty-schema
  (with-matrix-assertions test-db-configs test-db-fixture
    (is (= {}
           (get-in (first (filter #(= "SCHEMA" (% "type"))
                                  (->> (discover-catalog test-db-config)
                                       (select-stream "full_table_interruptible_sync_test-dbo-table_with_unsupported_column")
                                       (get-messages-from-output test-db-config
                                                                 "full_table_interruptible_sync_test-dbo-table_with_unsupported_column"))))
                    ["schema" "properties" "value"])))))

(deftest ^:integration verify-unsupported-primary-key-throws
  (with-matrix-assertions test-db-configs test-db-fixture
    (is (thrown-with-msg? java.lang.Exception
                          #"has unsupported primary key"
                          (->> test-db-config
                              (discover-catalog)
                              (select-stream "full_table_interruptible_sync_test-dbo-table_with_unsupported_pk")
                              (get-messages-from-output test-db-config "full_table_interruptible_sync_test-dbo-table_with_unsupported_pk"))))))

(deftest ^:integration verify-full-table-sync-with-rowversion-resumes-on-interruption
  (with-matrix-assertions test-db-configs test-db-fixture
    ;; Steps:
    ;; Sync partially, a table with row version, interrupted at some point
    ;;     -- e.g., (with-redefs [valid-message? (fn [msg] (if (some-atom-thing-changes-after x calls) (throw...) (valid-message? msg)))] ... )
    (let [old-write-record write-record!]
      (with-redefs [write-record! (fn [stream-name state record]
                                    (swap! record-count inc)
                                    (if (> @record-count 120)
                                      (do
                                        (reset! record-count 0)
                                        (throw (ex-info "Interrupting!" {:ignore true})))
                                      (old-write-record stream-name state record)))]
        (let [first-messages (->> (discover-catalog test-db-config)
                                  (select-stream "full_table_interruptible_sync_test-dbo-data_table_rowversion")
                                  (get-messages-from-output test-db-config
                                                            "full_table_interruptible_sync_test-dbo-data_table_rowversion"))
              first-state (last first-messages)]
          (def first-messages first-messages) ;; Convenience def for debugging
          (is (valid-state? first-state))
          (is (= "SCHEMA"
                 ((-> first-messages
                      first)
                  "type")))
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
          ;; Next state emitted has the pk of the last record emitted before that state
          (is (let [[last-record last-state] (->> first-messages
                                                  (drop 5) ;; Ignore first state
                                                  (partition 2 1)
                                                  (drop-while (fn [[a b]] (not= "STATE" (b "type"))))
                                                  first)]
                (= (get-in last-record ["record" "rowversion"]) (get-in last-state ["value" "bookmarks" "full_table_interruptible_sync_test-dbo-data_table_rowversion" "last_pk_fetched" "rowversion"])))
              "Either no state emitted, or state does not match previous record")
          ;; Next state emitted has the version of the last record emitted before that state
          (is (let [[last-record last-state] (->> first-messages
                                                  (drop 5) ;; Ignore first state
                                                  (partition 2 1)
                                                  (drop-while (fn [[a b]] (not= "STATE" (b "type"))))
                                                  first)]
                (= (last-record "version") (get-in last-state ["value" "bookmarks" "full_table_interruptible_sync_test-dbo-data_table_rowversion" "version"]))))
          ;; Activate Version on first sync
          (is (= "ACTIVATE_VERSION"
                 ((->> first-messages
                       (second)) "type")))
          ;;   - Record count (Equal to the counter of the atom before exception)
          (is (= 120
                 (count (filter #(= "RECORD" (% "type")) first-messages)))))
        ))
    ))
