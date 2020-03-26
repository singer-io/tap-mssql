(ns tap-mssql.sync-strategies.common
  (:require [clojure.string :as string])
  (:import [com.microsoft.sqlserver.jdbc SQLServerResultSet]))

(def result-set-opts {:raw? true
                      ;; Using SQLServerResultSet/TYPE_SS_SERVER_CURSOR_FORWARD_ONLY raises:
                      ;; com.microsoft.sqlserver.jdbc.TDSParser throwUnexpectedTokenException
                      :result-type SQLServerResultSet/TYPE_FORWARD_ONLY
                      :concurrency SQLServerResultSet/CONCUR_READ_ONLY})

;; Square brackets or quotes can be used interchangeably to sanitize names. Square brackets
;; are used by SSMS so we are using the same pattern.
(defn sanitize-names
  "Used for escaping column or table names that contain special characters or reserved words"
  [table-name]
  (format "[%s]" (-> table-name
                     (string/replace "]" "]]"))))
