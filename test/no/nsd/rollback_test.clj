(ns no.nsd.rollback-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.log-init]
            [datomic-schema.core]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [no.nsd.faketime2 :as ft]
            [no.nsd.shorter-stacktrace]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay]))

(deftest rollback-test
  (is (= 1 1))
  (when-let [conn (ft/empty-stage-conn)]
    @(d/transact conn impl/schema)
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/info :one :string]])

    @(d/transact conn [{:m/id "id" :m/info "original-data"}])
    (ft/next-year! conn)

    @(d/transact conn [{:m/id "id" :m/info "bad-data"}])
    (ft/next-year! conn)

    @(d/transact conn [{:m/id "id" :m/info "good-data"}])
    (ft/next-year! conn)

    ; schedule a mistake replacement:
    (rh/schedule-replacement! conn [:m/id "id"] "a" "b")
    (replay/process-until-state conn [:m/id "id"] :done)

    (is (= [[-1 :tx/txInstant #inst "1970" -1 true]
            [1 :m/id "id" -1 true]
            [1 :m/info "originbl-dbtb" -1 true]
            [-2 :tx/txInstant #inst "1971" -2 true]
            [1 :m/info "originbl-dbtb" -2 false]
            [1 :m/info "bbd-dbtb" -2 true]
            [-3 :tx/txInstant #inst "1972" -3 true]
            [1 :m/info "bbd-dbtb" -3 false]
            [1 :m/info "good-dbtb" -3 true]]
           (rh/pull-flat-history conn [:m/id "id"])))

    (is (= (rh/available-rollback-times conn [:m/id "id"])
           #{#inst "1973"}))

    (ft/next-year! conn)

    ; rollback the mistake:
    (rh/rollback! conn [:m/id "id"] #inst"1973")
    (replay/process-until-state conn [:m/id "id"] :done)
    (is (= [[-1 :tx/txInstant #inst "1970" -1 true]
            [1 :m/id "id" -1 true]
            [1 :m/info "original-data" -1 true]
            [-2 :tx/txInstant #inst "1971" -2 true]
            [1 :m/info "original-data" -2 false]
            [1 :m/info "bad-data" -2 true]
            [-3 :tx/txInstant #inst "1972" -3 true]
            [1 :m/info "bad-data" -3 false]
            [1 :m/info "good-data" -3 true]]
           (rh/pull-flat-history conn [:m/id "id"])))

    ; it is possible to rollback the rollback:
    (is (= (rh/available-rollback-times conn [:m/id "id"])
           #{#inst "1973" #inst "1974"}))

    (ft/next-year! conn)

    ; rollback the rollback:
    (rh/rollback! conn [:m/id "id"] #inst"1974")
    (replay/process-until-state conn [:m/id "id"] :done)
    (is (= [[-1 :tx/txInstant #inst "1970" -1 true]
            [1 :m/id "id" -1 true]
            [1 :m/info "originbl-dbtb" -1 true]
            [-2 :tx/txInstant #inst "1971" -2 true]
            [1 :m/info "originbl-dbtb" -2 false]
            [1 :m/info "bbd-dbtb" -2 true]
            [-3 :tx/txInstant #inst "1972" -3 true]
            [1 :m/info "bbd-dbtb" -3 false]
            [1 :m/info "good-dbtb" -3 true]]
           (rh/pull-flat-history conn [:m/id "id"])))))
