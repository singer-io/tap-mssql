(ns tap-mssql.discover-sync-precision-test
  (:require [tap-mssql.catalog :as catalog]
            [tap-mssql.config :as config]
            [clojure.test :refer [is deftest use-fixtures]]
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
  (let [destroy-database-commands (->> (catalog/get-databases config)
                                       (filter catalog/non-system-database?)
                                       (map get-destroy-database-command))]
    (let [db-spec (config/->conn-map config)]
      (jdbc/db-do-commands db-spec destroy-database-commands))))

(defn create-test-db
  [config]
  (let [db-spec (config/->conn-map config)]
    (jdbc/db-do-commands db-spec ["CREATE DATABASE precision"])
    (jdbc/db-do-commands (assoc db-spec :dbname "precision")
                         [(jdbc/create-table-ddl :numeric_precisions
                                                 [[:pk "int"]
                                                  [:numeric_9_3 "numeric(9,3)"]
                                                  [:numeric_19_8 "numeric(19,8)"]
                                                  [:numeric_28_1 "numeric(28,1)"]
                                                  [:numeric_38_22 "numeric(38,22)"]])
                          (jdbc/create-table-ddl :float_precisions
                                                 [[:pk "int"]
                                                  [:float_53 "float(53)"]
                                                  [:float_24 "float(24)"]])
                          (jdbc/create-table-ddl :money_precisions
                                                 [[:pk "int"]
                                                  [:smallmoney "smallmoney"]
                                                  [:money "money"]])])))

(def test-data-numerics [[0
                          (bigdec "-999999.999")
                          (bigdec "-99999999999.99999999")
                          (bigdec "-999999999999999999999999999.9")
                          (bigdec "-9999999999999999.9999999999999999999999")]
                         [1 0 0 0 0]
                         [2 nil nil nil nil]
                         [3
                          (bigdec "999999.999")
                          (bigdec "99999999999.99999999")
                          (bigdec "999999999999999999999999999.9")
                          (bigdec "9999999999999999.9999999999999999999999")]
                         [4
                          (bigdec "-5.667")
                          (bigdec "99847407548.36066732")
                          (bigdec "-331310880255879828202956905.1")
                          (bigdec "-7187498962233394.3739812942138415666763")]
                         [5
                          (bigdec "-634772.214")
                          (bigdec "-74662665258.71477591")
                          (bigdec "95964925502400625335154028.9")
                          (bigdec "9273972760690975.2044306442955715221042")]
                         [6
                          (bigdec "-930788.888")
                          (bigdec "-16158665537.52793427")
                          (bigdec "317128912264908522034101781.3")
                          (bigdec "29515565286974.1188802122612813004366")]
                         [7
                          (bigdec "297119.425")
                          (bigdec "-18936997313.82878795")
                          (bigdec "785790751998475189769700669.4")
                          (bigdec "9176089101347578.2596296292040288441238")]
                         [8
                          (bigdec "-979982.499")
                          (bigdec "-16958207592.78259005")
                          (bigdec "-443720922771981578168660472.3")
                          (bigdec "-8416853039392703.306423225471199148379")]
                         [9
                          (bigdec "-559083.613")
                          (bigdec "-25617164075.05356027")
                          (bigdec "53485768844071274332702409.4")
                          (bigdec "1285266411314091.3002668125515694162268")]
                         [10
                          (bigdec "649660.203")
                          (bigdec "66112072142.9833074")
                          (bigdec "670910153415438093285062942.2")
                          (bigdec "6051872750342125.3812886238958681227336")]
                         [11
                          (bigdec "416845.542")
                          (bigdec "-26909145239.03955264")
                          (bigdec "146681478952442530283844797.3")
                          (bigdec "-1132031605459408.5571559429308939781468")]
                         [12
                          (bigdec "605161.613")
                          (bigdec "77104913216.30429458")
                          (bigdec "-688544269586696335858539447.1")
                          (bigdec "-6387836755056303.0038029604189860431045")]
                         [13
                          (bigdec "-517647.884")
                          (bigdec "39898978520.27084742")
                          (bigdec "-810149456946558620902883940.6")
                          (bigdec "4526059300505413.566511729263231254806")]
                         ])

(def test-data-floats [[0
                        (bigdec "-8084.015625")
                        (bigdec "-8084.017")]
                       [1
                        (bigdec "-8084.0156")
                        (bigdec "-808.4018")]
                       [2
                        (bigdec "-2.4927882e+29")
                        (bigdec "0.8084019")]
                       [3
                        (bigdec "-2.4927882284206863e+29")
                        (bigdec "-3.201234E+37")]
                       [4
                        (bigdec "0.363981306552887")
                        (bigdec "3.391234E+37")]
                       [5
                        (bigdec "9.99999993922529E-09")
                        (bigdec "1.3E-37")]])

(def test-data-money [[0
                       (bigdec "-99999.999")
                       (bigdec "-99999999999.9999")]
                      [1 (bigdec 0) (bigdec 0)]
                      [2 (bigdec 1) (bigdec 1)]
                      [3
                       (bigdec "99999.999")
                       (bigdec "99999999999.999")]
                      [4
                       (bigdec "-5.667")
                       (bigdec "99847407548.3606")]
                      [5
                       (bigdec "-64772.214")
                       (bigdec "-74662665258.7147")]
                      [6
                       (bigdec "-90788.888")
                       (bigdec "-16158665537.5279")]
                      [7
                       (bigdec "213119.425")
                       (bigdec "-18936997313.8287")]
                      [8
                       (bigdec "-99982.499")
                       (bigdec "-16958207592.7825")]
                      [9
                       (bigdec "-59083.613")
                       (bigdec "-25617164075.0535")]
                      [10
                       (bigdec "64660.203")
                       (bigdec "66112072142.9833")]
                      [11
                       (bigdec "41845.542")
                       (bigdec "-26909145239.0395")]
                      [12
                       (bigdec "60161.613")
                       (bigdec "77104913216.3042")]
                      [13
                       (bigdec "-57647.884")
                       (bigdec "39898978520.2708")]])

(defn populate-data
  [config]
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "precision"))
                      "numeric_precisions"
                      (->> test-data-numerics
                           (map (partial zipmap[:pk :numeric_9_3 :numeric_19_8 :numeric_28_1 :numeric_38_22]))))
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "precision"))
                      "float_precisions"
                      (->> test-data-floats
                           (map (partial zipmap[:pk :float_53 :float_24]))))
  (jdbc/insert-multi! (-> (config/->conn-map config)
                          (assoc :dbname "precision"))
                      "money_precisions"
                      (->> test-data-money
                           (map (partial zipmap[:pk :smallmoney :money])))))

(comment
  ;; To reach into core.clj and define a new config
  (intern 'tap-mssql.core 'config test-db-config)
  )

(defn test-db-fixture [f config]
  (with-out-and-err-to-dev-null
    (maybe-destroy-test-db config)
    (create-test-db config)
    (populate-data config)
    (f)))

(defn select-stream
  [stream-name catalog]
  (-> (assoc-in catalog ["streams" stream-name "metadata" "selected"] true)
      (assoc-in ["streams" stream-name "metadata" "replication-method"] "FULL_TABLE")))

(deftest precision-should-be-specified-in-discovered-schema
  (with-matrix-assertions test-db-configs test-db-fixture
    (is (every? #((set (keys (second %))) "multipleOf")
                (filter #(string/starts-with? (first %) "numeric")
                        (get-in (catalog/discover test-db-config)
                                ["streams"
                                 "precision_dbo_numeric_precisions"
                                 "schema"
                                 "properties"]))))
    (is (= #{0.1 1.0E-22 0.001 1.0E-8}
           (set (map #((second %) "multipleOf")
                     (filter #(string/starts-with? (first %) "numeric")
                             (get-in (catalog/discover test-db-config)
                                     ["streams"
                                      "precision_dbo_numeric_precisions"
                                      "schema"
                                      "properties"]))))))))

(defn run-sync [config state catalog]
  (with-out-str
    (do-sync config catalog state)))

(defn write-and-read [sync-output]
  (as-> sync-output output
      (string/split output #"\n")
      (filter (complement empty?) output)
      ;; Bigdec specified to ensure that our reading doesn't introduce
      ;; error
      (map #(json/read-str % :bigdec true) output)))

;; Decimal
(deftest precision-should-be-maintained-in-written-records-from-json
  (with-matrix-assertions test-db-configs test-db-fixture
    (is (every? #(or (nil? %)
                     ;; Casting to bigdec for calculations to ensure that
                     ;; checking precision doesn't introduce error
                     (= (bigdec 0.0) (rem % (bigdec (Math/pow 10 -22)))))
                (->> (catalog/discover test-db-config)
                     (select-stream "precision_dbo_numeric_precisions")
                     (run-sync test-db-config {})
                     write-and-read
                     (filter #(= "RECORD" (% "type")))
                     (map #(get % "record"))
                     (map #(% "numeric_38_22")))))))

;; Floats
(deftest precision-should-be-maintained-in-written-records-from-json-float
  (with-matrix-assertions test-db-configs test-db-fixture
    (is (= (map second test-data-floats)
           (->> (catalog/discover test-db-config)
                (select-stream "precision_dbo_float_precisions")
                (run-sync test-db-config {})
                write-and-read
                (filter #(= "RECORD" (% "type")))
                (map #(get % "record"))
                (map #(% "float_53")))))
    (is (= (map #(nth % 2) test-data-floats)
           (->> (catalog/discover test-db-config)
                (select-stream "precision_dbo_float_precisions")
                (run-sync test-db-config {})
                write-and-read
                (filter #(= "RECORD" (% "type")))
                (map #(get % "record"))
                (map #(% "float_24")))))))

;; Money
(deftest precision-should-be-maintained-in-written-records-from-json
  (with-matrix-assertions test-db-configs test-db-fixture
    (doseq [rec (->> (catalog/discover test-db-config)
                     (select-stream "precision_dbo_money_precisions")
                     (run-sync test-db-config {})
                     write-and-read
                     (filter #(= "RECORD" (% "type")))
                     (map #(get % "record")))]
      (def record-test rec)
      (is (= (rec "smallmoney")
             (nth (nth test-data-money (rec "pk")) 1)))
      (is (= (rec "money")
             (nth (nth test-data-money (rec "pk")) 2)))
      (is (= (bigdec 0.0) (rem (rec "smallmoney") (bigdec 0.0001))))
      (is (= (bigdec 0.0) (rem (rec "money") (bigdec 0.0001)))))))

;; Single/Float
;; TODO: Harrison had this case come up in testing.
;; -8084.015625 vs -8084.0156 for a single  and -2.4927882e+29 vs -2.4927882284206863e+29 for a single
;; the first is what the target has and the second is what is actually _[returned from a select]_ in the db
