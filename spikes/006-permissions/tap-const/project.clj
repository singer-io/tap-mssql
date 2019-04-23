(defproject tap-const "0.0.1-SNAPSHOT"
  ;; 1.9.0 is the max we can get without bumping CIDER and we can't bump
  ;; CIDER until we can bump Java everywhere.
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [com.microsoft.sqlserver/mssql-jdbc "7.2.1.jre8"]
                 [org.clojure/tools.nrepl "0.2.13"
                  :exclusions [org.clojure/clojure]]
                 [cider/cider-nrepl "0.17.0"]]
  :profiles {:system {:java-cmd "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java"}})
