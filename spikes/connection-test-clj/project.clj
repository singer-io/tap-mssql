(defproject connection-test-clj "0.0.1-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/java.jdbc "0.7.9"]
                 [com.microsoft.sqlserver/mssql-jdbc "7.2.1.jre8"]]
  :profiles {:system {:java-cmd "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java"}})
