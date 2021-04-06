(ns no.nsd.rollback-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.log-init]
            [datomic-schema.core]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [no.nsd.shorter-stacktrace]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay]))

(deftest rollback-test
  (let [conn (u/empty-stage-conn)]
    @(d/transact conn impl/schema)
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/info :one :string]])

    @(d/transact conn [{:m/id "id" :m/info "original-data"}])
    @(d/transact conn [{:m/id "id" :m/info "bad-data"}])
    @(d/transact conn [{:m/id "id" :m/info "good-data"}])

    ; schedule a mistake replacement:
    (rh/schedule-replacement! conn [:m/id "id"] "a" "b")
    (replay/process-until-state conn [:m/id "id"] :done)

    (is (= (rh/pull-flat-history conn [:m/id "id"])
           [[1 :tx/txInstant #inst "1973" 1 true]
            [4 :m/id "id" 1 true]
            [4 :m/info "originbl-dbtb" 1 true]
            [2 :tx/txInstant #inst "1974" 2 true]
            [4 :m/info "originbl-dbtb" 2 false]
            [4 :m/info "bbd-dbtb" 2 true]
            [3 :tx/txInstant #inst "1975" 3 true]
            [4 :m/info "bbd-dbtb" 3 false]
            [4 :m/info "good-dbtb" 3 true]]))

    (is (= (rh/available-rollback-times conn [:m/id "id"])
           #{#inst "1981"}))

    ; rollback the mistake:
    (rh/rollback! conn [:m/id "id"] #inst"1981")
    (replay/process-until-state conn [:m/id "id"] :done)
    (is (= (rh/pull-flat-history conn [:m/id "id"])
           [[1 :tx/txInstant #inst "1973" 1 true]
            [4 :m/id "id" 1 true]
            [4 :m/info "original-data" 1 true]
            [2 :tx/txInstant #inst "1974" 2 true]
            [4 :m/info "original-data" 2 false]
            [4 :m/info "bad-data" 2 true]
            [3 :tx/txInstant #inst "1975" 3 true]
            [4 :m/info "bad-data" 3 false]
            [4 :m/info "good-data" 3 true]]))

    ; it is possible to rollback the rollback:
    (is (= (rh/available-rollback-times conn [:m/id "id"])
           #{#inst "1981" #inst "1992"}))

    ; rollback the rollback:
    (rh/rollback! conn [:m/id "id"] #inst"1992")
    (replay/process-until-state conn [:m/id "id"] :done)
    (is (= (rh/pull-flat-history conn [:m/id "id"])
           [[1 :tx/txInstant #inst "1973" 1 true]
            [4 :m/id "id" 1 true]
            [4 :m/info "originbl-dbtb" 1 true]
            [2 :tx/txInstant #inst "1974" 2 true]
            [4 :m/info "originbl-dbtb" 2 false]
            [4 :m/info "bbd-dbtb" 2 true]
            [3 :tx/txInstant #inst "1975" 3 true]
            [4 :m/info "bbd-dbtb" 3 false]
            [4 :m/info "good-dbtb" 3 true]]))))
