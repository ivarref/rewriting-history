(ns no.nsd.bugfix-tempid-ref-to-previous-datomic-tx
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.log-init]
            [datomic-schema.core]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [no.nsd.shorter-stacktrace]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.schema :as schema]
            [no.nsd.rewriting-history.replay-impl :as replay]
            [no.nsd.big-data :as data]))

(deftest stage-test
  (let [conn1 (u/empty-conn)]
    @(d/transact conn1 impl/schema)
    @(d/transact conn1 #d/schema[[:m/id :one :string :id]
                                 [:m/ref :one :ref]])

    @(d/transact conn1 [{:m/id "id" :m/ref "datomic.tx"}])
    @(d/transact conn1 [{:m/id "id" :m/ref "datomic.tx"}])

    (impl/apply-txes!
      (u/empty-conn)
      (impl/history->transactions
        conn1
        (rh/pull-flat-history conn1 [:m/id "id"])))))