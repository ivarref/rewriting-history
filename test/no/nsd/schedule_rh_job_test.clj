(ns no.nsd.schedule-rh-job-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [datomic-schema.core]
            [no.nsd.rewriting-history :as rh]
            [clojure.tools.logging :as log]
            [datomic.api :as d]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay]
            [no.nsd.rewriting-history.schedule-init :as schedule]
            [no.nsd.rewriting-history.init :as init]))

(def schema
  (into impl/schema
        #d/schema[[:m/id :one :string :id]
                  [:m/info :one :string]]))

(def empty-conn u/empty-conn)

(deftest schedule-rh-bad-input-test
  (testing "Verify that missing lookup ref throws exception"
    (let [conn1 (empty-conn)]
      @(d/transact conn1 schema)
      (u/is-assert-msg "Expected to find lookup-ref"
                       (rh/schedule-replacement! conn1 [:m/id "id"] "bad-data" "corrected-data")))))

(deftest schedule-rh-job-test
  (testing "Verify that the most basic scheduling of string replacement works"
    (let [conn (empty-conn)]
      @(d/transact conn schema)
      @(d/transact conn [{:m/id "id" :m/info "original-data"}])
      @(d/transact conn [{:m/id "id" :m/info "bad-data"}])
      @(d/transact conn [{:m/id "id" :m/info "good-data"}])

      (let [org-history (rh/pull-flat-history conn [:m/id "id"])]
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

        (is (= [{:match       "bad-data"
                 :replacement "corrected-data"}]
               (rh/schedule-replacement! conn [:m/id "id"] "bad-data" "corrected-data")))

        ; duplicates are ignored
        (is (= [{:match       "bad-data"
                 :replacement "corrected-data"}]
               (rh/schedule-replacement! conn [:m/id "id"] "bad-data" "corrected-data")))

        ; schedule something by mistake
        (is (= [{:match       "a"
                 :replacement "oops"}
                {:match       "bad-data"
                 :replacement "corrected-data"}]
               (rh/schedule-replacement! conn [:m/id "id"] "a" "oops")))

        ; abort the mistake
        (is (= [{:match       "bad-data"
                 :replacement "corrected-data"}]
               (rh/cancel-replacement! conn [:m/id "id"] "a" "oops")))

        ; Prepare for re-write
        (schedule/process-single-schedule! conn [:m/id "id"])
        (init/job-init! conn [:m/id "id"])

        ; in memory database uses retractEntity for excision
        (replay/process-until-state conn [:m/id "id"] :done)

        (is (= (rh/pull-flat-history conn [:m/id "id"])
               [[1 :tx/txInstant #inst "1972" 1 true]
                [4 :m/id "id" 1 true]
                [4 :m/info "original-data" 1 true]
                [2 :tx/txInstant #inst "1973" 2 true]
                [4 :m/info "original-data" 2 false]
                [4 :m/info "corrected-data" 2 true]
                [3 :tx/txInstant #inst "1974" 3 true]
                [4 :m/info "corrected-data" 3 false]
                [4 :m/info "good-data" 3 true]]))))))

(deftest cancelling-everything-also-cancels-job
  (let [conn (empty-conn)]
    @(d/transact conn schema)
    @(d/transact conn [{:m/id "id" :m/info "original-data"}])
    @(d/transact conn [{:m/id "id" :m/info "bad-data"}])
    @(d/transact conn [{:m/id "id" :m/info "good-data"}])

    (is (= [{:match       "bad-data"
             :replacement "corrected-data"}]
           (rh/schedule-replacement! conn [:m/id "id"] "bad-data" "corrected-data")))

    (is (= :scheduled (impl/job-state conn [:m/id "id"])))

    (is (= []
           (rh/cancel-replacement! conn [:m/id "id"] "bad-data" "corrected-data")))

    (is (nil? (impl/job-state conn [:m/id "id"])))))


