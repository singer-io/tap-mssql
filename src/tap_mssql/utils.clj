(ns tap-mssql.utils)

(defmacro try-read-only
  "
  Note: This macro is structured similar to if-let.

  Tries macro body with ApplicationIntent set, then without (if first
  fails). The db-spec used is defined in the binding supplied to this
  macro.

  Example:

  (try-read-only [a-db-spec-binding (config/->conn-map config)]
    (jdbc/query a-db-spec-binding
                \"SELECT 'this should work with read-only, if possible'\"))
  "
  [bindings & body]
  (assert (vector? bindings) "try-read-only requires a vector for its binding.")
  (assert (= 2 (count bindings)) "try-read-only requires exactly 2 forms in binding vector")
  (let [inner-name (bindings 0)
        binding-val (bindings 1)]
    `(let [db-spec-initial# ~binding-val]
       (loop [~inner-name (assoc db-spec-initial# :ApplicationIntent "ReadOnly")
              should-retry# true]
         (if-let [result# (try
                           ~@body
                           (catch com.microsoft.sqlserver.jdbc.SQLServerException ex#
                             (when-not should-retry# (throw ex#))))]
           result#
           (recur (dissoc ~inner-name :ApplicationIntent) false))))))
