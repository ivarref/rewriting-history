(ns no.nsd.log-init
  (:require [clojure.test :refer :all]
            [no.nsd.envelope :as envelope]
            [clojure.tools.reader.edn :as edn]))

(envelope/init!
  {:min-level  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
                [#{"*"} (edn/read-string
                          (System/getProperty "TAOENSSO_TIMBRE_MIN_LEVEL_EDN" ":info"))]]
   :log-to-elk false})