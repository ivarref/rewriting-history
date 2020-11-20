(ns no.nsd.log-init
  (:require [clojure.test :refer :all]
            [no.nsd.envelope :as envelope]))

(envelope/init!
  {:min-level  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
                [#{"*"} :info]]
   :log-to-elk false})