(ns no.nsd.db-some-retract-fn-test
  (:require [clojure.test :refer :all]
            [no.nsd.datomic-generate-fn :as genfn]
            [no.nsd.rewriting-history.db-some-retract-fn :as s]
            [no.nsd.utils :as u]
            [no.nsd.shorter-stacktrace]
            [no.nsd.log-init]
            [datomic-schema.core]
            [datomic.api :as d]))

(defn db-fn
  ([] (db-fn false))
  ([generate]
   (genfn/generate-function
     'no.nsd.rewriting-history.db-some-retract-fn/some-retract
     :some/retract
     generate)))

(def empty-conn u/empty-conn)

(deftest gen-fn
  (db-fn true)
  (is (= 1 1)))