(defproject tap-mssql
  "1.6.12"
  :description "Singer.io tap for extracting data from a Microsft SQL Server "
  :url "https://github.com/stitchdata/tap-mssql"
  :license {:name "GNU Affero General Public License Version 3; Other commercial licenses available."
            :url "https://www.gnu.org/licenses/agpl-3.0.en.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.cli "0.4.1"]
                 [org.clojure/data.json "0.2.6"]

                 ;; jdbc
                 [org.clojure/java.jdbc "0.7.9"]
                 [com.microsoft.sqlserver/mssql-jdbc "7.2.1.jre8"]

                 ;; logging
                 ;; Basic log4j dependency to declare bare minimum
                 [org.clojure/tools.logging "1.2.4"]
                 [org.slf4j/slf4j-log4j12 "1.7.36"]
                 [org.apache.logging.log4j/log4j-1.2-api "2.17.1"]
                 [org.apache.logging.log4j/log4j-core "2.17.1"]

                 ;; repl
                 [nrepl "0.6.0"] ;; For Lein 2.9.X
                 [cider/cider-nrepl "0.25.4"] ;; For cider-emacs 0.26.1

                 ;; test
                 [org.clojure/data.generators "0.1.2"]
                 ]
  :plugins [[lein-pprint "1.2.0"]]
  :main tap-mssql.core
  :manifest {"Multi-Release" "true"}
  :profiles {:uberjar {:aot [tap-mssql.core]}
             :system {:java-cmd "/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java"}})
