(defproject tendre "0.1.0-beta2"
  :description "Persistent transactional disk-based implementation of a map in Clojure based on xodus"
  :url "https://www.github.com/ngrunwald/tendre"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [org.jetbrains.xodus/xodus-openAPI "2.0.1"]
                 [org.jetbrains.xodus/xodus-environment "2.0.1"]]
  :profiles {:dev {:dependencies [[com.taoensso/nippy "3.3.0-alpha2"]
                                  [metosin/testit "0.4.0"]
                                  [expound "0.8.4"]]}}
  :repl-options {:init-ns tendre.core})
