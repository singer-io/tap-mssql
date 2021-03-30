(ns tap-mssql.utils)

;; TODO: This needs to be a binding situation, like `let`
;; E.g., `(try-with-readonly [my-conn (get-a-conn-map)] (do-thing-with-readonly-or-not-conn-map my-conn)`
;; ---- result of this would be that it always trys with readonly then without, which could be rough in situations where it's not available
(defmacro with-read-only
  "
  Note: This macro is structured similar to if-let.

  Tries macro body with ApplicationIntent set, then without (if first
  fails). The db-spec used is defined in the binding supplied to this
  macro."
  [bindings & body]
  (assert (vector? bindings) "try-with-read-only requires a vector for its binding.")
  (assert (= 2 (count bindings)) "try-with-read-only requires exactly 2 forms in binding vector")
  (let [inner-name (bindings 0)
        binding-val (bindings 1)]
    `(let [db-spec-initial# ~binding-val]
       (loop [~inner-name (assoc db-spec-initial# :ApplicationIntent "ReadOnly")
              should-retry# true]
         (if-let [result# (try
                           (do ~@body)
                           (catch com.microsoft.sqlserver.jdbc.SQLServerException ex#
                             (when-not should-retry# (throw ex#))))]
           result#
           (recur (dissoc ~inner-name :ApplicationIntent) false))))))
