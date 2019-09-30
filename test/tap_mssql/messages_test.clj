(ns tap-mssql.messages-test
  (:require [tap-mssql.catalog :as catalog]
            [tap-mssql.serialized-catalog :as serialized-catalog]
            [tap-mssql.config :as config]
            [tap-mssql.singer.messages :as singer-messages]
            [tap-mssql.singer.parse :as singer-parse]
            [clojure.test :refer [is deftest]]
            [clojure.string :as string]
            [clojure.data.json :as json]
            [tap-mssql.core :refer :all]))

(defn get-messages-from-output
  [catalog stream-name]
   (as-> (with-out-str
           (singer-messages/write-schema! catalog stream-name))
       output
       (string/split output #"\n")
       (filter (complement empty?) output)
       (map json/read-str
            output)
       (vec output)))

(deftest view-primary-key-test
  (let [stream-name             "test-stream"
        view-catalog                 {"streams" {stream-name {"metadata"      {"is-view"              true
                                                                               "view-key-properties"  ["2"]
                                                                               "table-key-properties" []
                                                                               "replication-key"      "4"}
                                                              "schema"        {"type"       "object"
                                                                               "properties" {"id" {"type" ["null" "integer"]}}}
                                                              "tap_stream_id" stream-name
                                                              "table_name"    stream-name}}}
        view-expected-schema-message {"type"                "SCHEMA"
                                      "stream"              "test-stream"
                                      "key_properties"      ["2"]
                                      "schema"              {"type"       "object"
                                                             "properties" {"id" {"type" ["null"
                                                                                         "integer"]}}}
                                      "bookmark_properties" ["4"]}
        view-actual-schema-message   (first (get-messages-from-output view-catalog stream-name))
        table-catalog                 {"streams" {stream-name {"metadata"      {"is-view"              false
                                                                                "table-key-properties" ["3"]
                                                                                "replication-key"      "4"}
                                                               "schema"        {"type"       "object"
                                                                                "properties" {"id" {"type" ["null" "integer"]}}}
                                                               "tap_stream_id" stream-name
                                                               "table_name"    stream-name}}}
        table-expected-schema-message {"type"                "SCHEMA"
                                       "stream"              "test-stream"
                                       "key_properties"      ["3"]
                                       "schema"              {"type"       "object"
                                                              "properties" {"id" {"type" ["null"
                                                                                          "integer"]}}}
                                       "bookmark_properties" ["4"]}
        table-actual-schema-message   (first (get-messages-from-output table-catalog stream-name))]
    (is (= table-actual-schema-message
           table-expected-schema-message))
    (is (= view-actual-schema-message
           view-expected-schema-message))))
