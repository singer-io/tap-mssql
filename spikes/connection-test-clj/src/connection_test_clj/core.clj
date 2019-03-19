;;; For https://stitchdata.atlassian.net/browse/SRCE-515

(ns connection-test-clj.core
  (:require [clojure.java.jdbc :as jdbc]))

(def db-spec
  {:dbtype "sqlserver"
   :dbname "SampleDB"
   :user "sa"
   :password "password123!"})

(jdbc/query db-spec ["select @@version"])
;; ({: "Microsoft SQL Server 2017 (RTM-CU13) (KB4466404) - 14.0.3048.4 (X64) \n\tNov 30 2018 12:57:58 \n\tCopyright (C) 2017 Microsoft Corporation\n\tDeveloper Edition (64-bit) on Linux (Ubuntu 16.04.4 LTS)"})
