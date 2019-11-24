(defproject tendre "0.1.0-SNAPSHOT"
  :description "Persistent transactional disk-based implementation of a map in Clojure based on xodus"
  :url "https://www.github.com/ngrunwald/tendre"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.jetbrains.xodus/xodus-openAPI "1.3.124"]
                 [org.jetbrains.xodus/xodus-environment "1.3.124"]]
  :profiles {:dev {:dependencies [[com.taoensso/nippy "2.14.0"]]}}
  :repl-options {:init-ns tendre.core})
