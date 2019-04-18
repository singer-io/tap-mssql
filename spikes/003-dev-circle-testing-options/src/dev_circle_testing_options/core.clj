(ns dev-circle-testing-options
  (:require [clojure.java.jdbc :as jdbc]))

(def db-spec
  {:dbtype "sqlserver"
   :dbname "spike_tap_mssql"
   :host "spike-tap-mssql-2.cqaqbfvfo67k.us-east-1.rds.amazonaws.com"
   :user "spike_tap_mssql"
   :password "spike_tap_mssql"})

(jdbc/query db-spec ["select @@version"])
;; ({: "Microsoft SQL Server 2017 (RTM-CU13-OD) (KB4483666) - 14.0.3049.1 (X64) \n\tDec 15 2018 11:16:42 \n\tCopyright (C) 2017 Microsoft Corporation\n\tStandard Edition (64-bit) on Windows Server 2016 Datacenter 10.0 <X64> (Build 14393: ) (Hypervisor)\n"})

(.getDatabaseMajorVersion (.getMetaData (jdbc/get-connection db-spec)))
;; 14
(.getDatabaseMinorVersion (.getMetaData (jdbc/get-connection db-spec)))
;; 0
