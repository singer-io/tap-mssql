(defproject tap-mssql
  "0.1.0"
  :description "Singer.io tap for extracting data from a Microsft SQL Server "
  :url "https://github.com/stitchdata/tap-mssql"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.cli "0.4.1"]
                 [org.clojure/data.json "0.2.6"]

                 ;; jdbc
                 [org.clojure/java.jdbc "0.7.9"]
                 [com.microsoft.sqlserver/mssql-jdbc "7.2.1.jre8"]

                 ;; logging
                 [org.clojure/tools.logging "0.3.1"]
                 [log4j "1.2.17" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]
                 ;; repl
                 [org.clojure/tools.nrepl "0.2.13"]
                 [cider/cider-nrepl "0.17.0"]]
  :main tap-mssql.core
  :profiles {:uberjar {:uberjar-name "tap-mssql-standalone.jar"
                       :aot [tap-mssql.core]}
             :system {:java-cmd "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java"}})
