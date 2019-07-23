(ns tap-mssql.sync-strategies.common
  (:require [clojure.string :as string]))

;; Square brackets or quotes can be used interchangeably to sanitize names. Square brackets
;; are used by SSMS so we are using the same pattern.
(defn sanitize-names
  "Used for escaping column or table names that contain special characters or reserved words"
  [table-name]
  (format "[%s]" (-> table-name
                     (string/replace "]" "]]"))))
