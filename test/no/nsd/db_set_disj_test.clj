(ns no.nsd.db-set-disj-test
  (:require [clojure.test :refer :all]
            [no.nsd.datomic-generate-fn :as genfn]))

(defn db-fn
  ([] (db-fn false))
  ([generate]
   (genfn/generate-function
     'no.nsd.rewriting-history.dbfns.set-disj/set-disj
     :set/disj
     generate)))

(deftest write-fn
  (db-fn true)
  (is (= 1 1)))