(ns no.nsd.add-rh-job-test
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
            [taoensso.timbre :as timbre]))

(defn setup-schema! [conn]
  @(d/transact conn #d/schema[[:db/txInstant2 :one :instant]])

  @(d/transact conn impl/schema)

  ; Setup application schema:
  @(d/transact conn #d/schema[[:m/id :one :string :id]
                              [:m/info :one :string]]))

(deftest replay-history-job-test
  (testing "Replay history job works"
    (let [conn1 (u/empty-conn)
          conn2 (u/empty-conn)]
      (setup-schema! conn1)
      (setup-schema! conn2)

      @(d/transact conn1 [{:m/id "id" :m/info "original-data"}])
      @(d/transact conn1 [{:m/id "id" :m/info "bad-data"}])
      @(d/transact conn1 [{:m/id "id" :m/info "good-data"}])

      (let [org-history (rh/pull-flat-history conn1 [:m/id "id"])]
        (is (= org-history
               [[1 :db/txInstant2 #inst "1974-01-01T00:00:00.000-00:00" 1 true]
                [4 :m/id "id" 1 true]
                [4 :m/info "original-data" 1 true]
                [2 :db/txInstant2 #inst "1975-01-01T00:00:00.000-00:00" 2 true]
                [4 :m/info "original-data" 2 false]
                [4 :m/info "bad-data" 2 true]
                [3 :db/txInstant2 #inst "1976-01-01T00:00:00.000-00:00" 3 true]
                [4 :m/info "bad-data" 3 false]
                [4 :m/info "good-data" 3 true]]))

        (replay/add-rewrite-job! conn1 "job" org-history org-history)
        (replay/add-rewrite-job! conn2 "job" org-history org-history)

        ; This will wipe the existing data:
        (replay/job-init! conn1 "job")

        ; Fake excision is done for conn2:
        @(d/transact conn2 [{:rh/id "job" :rh/state :rewrite-history}])

        (is (= (replay/get-new-history conn1 "job") org-history))
        (is (= (replay/get-new-history conn2 "job") org-history))

        (replay/process-until-state conn2 "job" :done)

        (is (= (rh/pull-flat-history conn2 [:m/id "id"])
               org-history))))))

(deftest error-replay-history-job-test
  (testing "Replay history job detects error if unexpected write occurs"
    (let [conn1 (u/empty-conn)
          conn2 (u/empty-conn)]
      (setup-schema! conn1)
      (setup-schema! conn2)

      @(d/transact conn1 [{:m/id "id" :m/info "original-data"}])
      @(d/transact conn1 [{:m/id "id" :m/info "bad-data"}])
      @(d/transact conn1 [{:m/id "id" :m/info "good-data"}])

      (let [org-history (rh/pull-flat-history conn1 [:m/id "id"])]
        (is (= org-history
               [[1 :db/txInstant2 #inst "1974-01-01T00:00:00.000-00:00" 1 true]
                [1000000000 :m/id "id" 1 true]
                [1000000000 :m/info "original-data" 1 true]
                [2 :db/txInstant2 #inst "1975-01-01T00:00:00.000-00:00" 2 true]
                [1000000000 :m/info "original-data" 2 false]
                [1000000000 :m/info "bad-data" 2 true]
                [3 :db/txInstant2 #inst "1976-01-01T00:00:00.000-00:00" 3 true]
                [1000000000 :m/info "bad-data" 3 false]
                [1000000000 :m/info "good-data" 3 true]]))

        ; Add job
        (replay/add-rewrite-job! conn2 "job" org-history org-history)

        ; Fake excision
        @(d/transact conn2 [{:rh/id "job" :rh/state :rewrite-history}])

        (log/info "************")
        (replay/process-job-step! conn2 "job")
        (log/info "***********")

        (is (= (rh/pull-flat-history conn2 [:m/id "id"])
               [[1 :db/txInstant2 #inst "1974-01-01T00:00:00.000-00:00" 1 true]
                [1000000000 :m/id "id" 1 true]
                [1000000000 :m/info "original-data" 1 true]]))

        @(d/transact conn2 [{:m/id   "id"
                             :m/info "oh no somebody wrote data in the middle of a re-write!"}])

        (let [{:keys [expected-history]
               :as   exd}
              (try
                (replay/rewrite-history! conn2 "job")
                nil
                (catch Exception e
                  (log/error (ex-message e))
                  (ex-data e)))]
          (is (some? exd))
          (is (= expected-history
                 [[1 :db/txInstant2 #inst "1974-01-01T00:00:00.000-00:00" 1 true]
                  [1000000000 :m/id "id" 1 true]
                  [1000000000 :m/info "original-data" 1 true]])))


        #_@(d/transact conn2 [{:rh/id "job" :m/info "oops unexpected write!"}
                              {:db/id "datomic.tx" :db/txInstant #inst"2100"}])


        #_(replay/process-until-state conn2 "job" :done)

        #_(is (= (rh/pull-flat-history conn2 [:m/id "id"])
                 org-history))))))