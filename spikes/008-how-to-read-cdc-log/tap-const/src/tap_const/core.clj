(ns tap-const.core
  (:require [clojure.tools.nrepl.server :as nrepl-server]
            [clojure.java.jdbc :as jdbc])
  (:gen-class))

(def db-spec {:dbtype "sqlserver"
              :dbname "spike_tap_mssql"
              :host "taps-tvisher1-test-mssql.cqaqbfvfo67k.us-east-1.rds.amazonaws.com"
              :user "spike_mssql"
              :password "spike_mssql"})

(comment
  (jdbc/query db-spec ["select * from foo"])

  (jdbc/db-query-with-resultset
   db-spec
   "exec sys.sp_cdc_help_change_data_capture"
   (comp doall jdbc/result-set-seq))
  ;; => ({:index_name "PK__foo__3213E83FA6198C88",
  ;;      :index_column_list "[id]",
  ;;      :object_id 1269579561,
  ;;      :start_lsn [0, 0, 0, 36, 0, 0, 10, 24, 0, 66],
  ;;      :source_table "foo",
  ;;      :create_date #inst "2019-04-23T19:33:47.923000000-00:00",
  ;;      :captured_column_list "[id]",
  ;;      :role_name nil,
  ;;      :supports_net_changes true,
  ;;      :source_object_id 1237579447,
  ;;      :has_drop_pending nil,
  ;;      :end_lsn nil,
  ;;      :filegroup_name nil,
  ;;      :capture_instance "dbo_foo",
  ;;      :source_schema "dbo"})

  ;; https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-help-change-data-capture-transact-sql?view=sql-server-2017#remarks
  ;;
  ;; Remarks
  ;;
  ;; When both source_schema and source_name default to NULL, or are
  ;; explicitly set the NULL, this stored procedure returns information
  ;; for all of the database capture instances that the caller has SELECT
  ;; access to. When source_schema and source_name are non-NULL, only
  ;; information on the specific named enabled table is returned.
  ;;
  ;; Could make that a decent way for us to get information about all the
  ;; tables we have CDC access for.

  (jdbc/db-query-with-resultset
   db-spec
   "exec sys.sp_cdc_help_change_data_capture @source_schema = 'dbo', @source_name = 'foo'"
   (comp doall jdbc/result-set-seq))
  ;; => ({:index_name "PK__foo__3213E83FA6198C88",
  ;;      :index_column_list "[id]",
  ;;      :object_id 1269579561,
  ;;      :start_lsn [0, 0, 0, 36, 0, 0, 10, 24, 0, 66],
  ;;      :source_table "foo",
  ;;      :create_date #inst "2019-04-23T19:33:47.923000000-00:00",
  ;;      :captured_column_list "[id]",
  ;;      :role_name nil,
  ;;      :supports_net_changes true,
  ;;      :source_object_id 1237579447,
  ;;      :has_drop_pending nil,
  ;;      :end_lsn nil,
  ;;      :filegroup_name nil,
  ;;      :capture_instance "dbo_foo",
  ;;      :source_schema "dbo"})

  (jdbc/query db-spec ["select sys.fn_cdc_get_min_lsn ( 'dbo_foo' ) as min_lsn"])
  ;; => ({:min_lsn [0, 0, 0, 36, 0, 0, 10, 24, 0, 66]})

  (jdbc/query db-spec ["select sys.fn_cdc_get_max_lsn () as max_lsn"])
  ;; => ({:max_lsn [0, 0, 0, 40, 0, 0, 8, -13, 0, 1]})

  (jdbc/query db-spec ["select * from cdc.fn_cdc_get_all_changes_dbo_foo(sys.fn_cdc_get_min_lsn ( 'dbo_foo' ), sys.fn_cdc_get_max_lsn (), 'all');"])
  ;; => ({:__$start_lsn [0, 0, 0, 40, 0, 0, 8, 87, 0, 5],
  ;;      :__$seqval [0, 0, 0, 40, 0, 0, 8, 87, 0, 2],
  ;;      :__$operation 2,
  ;;      :__$update_mask [1],
  ;;      :id 7}
  ;;     {:__$start_lsn [0, 0, 0, 40, 0, 0, 8, 87, 0, 5],
  ;;      :__$seqval [0, 0, 0, 40, 0, 0, 8, 87, 0, 3],
  ;;      :__$operation 2,
  ;;      :__$update_mask [1],
  ;;      :id 8}
  ;;     {:__$start_lsn [0, 0, 0, 40, 0, 0, 8, 87, 0, 5],
  ;;      :__$seqval [0, 0, 0, 40, 0, 0, 8, 87, 0, 4],
  ;;      :__$operation 2,
  ;;      :__$update_mask [1],
  ;;      :id 9}
  ;;     {:__$start_lsn [0, 0, 0, 40, 0, 0, 8, 103, 0, 6],
  ;;      :__$seqval [0, 0, 0, 40, 0, 0, 8, 103, 0, 2],
  ;;      :__$operation 1,
  ;;      :__$update_mask [1],
  ;;      :id 4}
  ;;     {:__$start_lsn [0, 0, 0, 40, 0, 0, 8, 103, 0, 6],
  ;;      :__$seqval [0, 0, 0, 40, 0, 0, 8, 103, 0, 2],
  ;;      :__$operation 2,
  ;;      :__$update_mask [1],
  ;;      :id 2}
  ;;     {:__$start_lsn [0, 0, 0, 40, 0, 0, 8, 111, 0, 5],
  ;;      :__$seqval [0, 0, 0, 40, 0, 0, 8, 111, 0, 2],
  ;;      :__$operation 2,
  ;;      :__$update_mask [1],
  ;;      :id 4}
  ;;     {:__$start_lsn [0, 0, 0, 40, 0, 0, 8, 111, 0, 5],
  ;;      :__$seqval [0, 0, 0, 40, 0, 0, 8, 111, 0, 3],
  ;;      :__$operation 2,
  ;;      :__$update_mask [1],
  ;;      :id 5}
  ;;     {:__$start_lsn [0, 0, 0, 40, 0, 0, 8, 111, 0, 5],
  ;;      :__$seqval [0, 0, 0, 40, 0, 0, 8, 111, 0, 4],
  ;;      :__$operation 2,
  ;;      :__$update_mask [1],
  ;;      :id 6})

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
