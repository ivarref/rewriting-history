(ns no.nsd.wipe-rewrite-job-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [clojure.tools.logging :as log]
            [datomic.api :as d]
            [datomic-schema.core]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.shorter-stacktrace]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.log-init]
            [no.nsd.rewriting-history.replay-impl :as replay]))

(defn db-values-set [conn]
  (into (sorted-set) (d/q '[:find [?v ...]
                            :in $
                            :where
                            [?e :m/id "id" _ _ _]
                            [?e :m/info ?v _ _]]
                          (d/history (d/db conn)))))

(deftest wipe-test
  (is (= 1 1))
  (when-let [conn (u/empty-stage-conn "rewriting-history-integration-test-1")]
    @(d/transact conn (into
                        impl/schema
                        #d/schema[[:m/id :one :string :id]
                                  [:m/info :one :string]
                                  [:tx/txInstant :one :instant]]))
    @(d/transact conn [{:m/id "id" :m/info "original-data"}])
    @(d/transact conn [{:m/id "id" :m/info "bad-data"}])
    @(d/transact conn [{:m/id "id" :m/info "good-data"}])))