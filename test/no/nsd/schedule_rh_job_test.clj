(ns no.nsd.schedule-rh-job-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.shorter-stacktrace]
            [no.nsd.log-init]
            [datomic-schema.core]
            [no.nsd.rewriting-history :as rh]
            [clojure.tools.logging :as log]
            [datomic.api :as d]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay]
            [no.nsd.rewriting-history.schedule-impl :as schedule]
            [taoensso.timbre :as timbre]))

(def schema
  (into impl/schema
        #d/schema[[:tx/txInstant :one :instant]
                  [:m/id :one :string :id]
                  [:m/info :one :string]]))

(def empty-conn u/empty-conn)

(deftest schedule-rh-job-test
  (testing "Verify that the most basic scheduling of string replacement works"
    (let [conn1 (empty-conn)
          conn2 (empty-conn)]

      @(d/transact conn1 schema)
      @(d/transact conn2 schema)

      @(d/transact conn1 [{:m/id "id" :m/info "original-data"}])
      @(d/transact conn1 [{:m/id "id" :m/info "bad-data"}])
      @(d/transact conn1 [{:m/id "id" :m/info "good-data"}])

      (let [org-history (rh/pull-flat-history conn1 [:m/id "id"])]
        (is (= org-history
               [[1 :tx/txInstant #inst "1972" 1 true]
                [4 :m/id "id" 1 true]
                [4 :m/info "original-data" 1 true]
                [2 :tx/txInstant #inst "1973" 2 true]
                [4 :m/info "original-data" 2 false]
                [4 :m/info "bad-data" 2 true]
                [3 :tx/txInstant #inst "1974" 3 true]
                [4 :m/info "bad-data" 3 false]
                [4 :m/info "good-data" 3 true]]))

        (rh/schedule-replacement! conn1 [:m/id "id"] "bad-data" "corrected-data")

        ; duplicates are ignored
        (rh/schedule-replacement! conn1 [:m/id "id"] "bad-data" "corrected-data")

        ; Prepare for re-write
        (schedule/process-single-schedule! conn1 [:m/id "id"])
        (replay/job-init! conn1 [:m/id "id"])

        ; In memory datomic does not have excision, so we need to fake
        ; it using a different connection:
        (impl/apply-txes! conn2
                          (->> (rh/pull-flat-history conn1 [:rh/lookup-ref (pr-str [:m/id "id"])])
                               (impl/history->transactions conn1)))
        ; conn2 now only holds the rewrite data. That is the original data has been "excised".

        (replay/process-until-state conn2 [:m/id "id"] :done)

        (is (= (rh/pull-flat-history conn2 [:m/id "id"])
               [[1 :tx/txInstant #inst "1972" 1 true]
                [4 :m/id "id" 1 true]
                [4 :m/info "original-data" 1 true]
                [2 :tx/txInstant #inst "1973" 2 true]
                [4 :m/info "original-data" 2 false]
                [4 :m/info "corrected-data" 2 true]
                [3 :tx/txInstant #inst "1974" 3 true]
                [4 :m/info "corrected-data" 3 false]
                [4 :m/info "good-data" 3 true]]))))))

