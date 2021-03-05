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
            [no.nsd.rewriting-history.replay-impl :as replay]))

(defn setup-schema! [tx!]
  (tx! #d/schema[[:db/txInstant2 :one :instant]])

  (tx! impl/schema)

  ; Setup application schema:
  (tx! #d/schema[[:m/id :one :string :id]
                 [:m/info :one :string]]))

(deftest replay-history-job-test
  (testing "Replay history job works"
    (let [conn1 (u/empty-conn)
          conn2 (u/empty-conn)
          tx1! (u/tx-fn! conn1)
          tx2! (u/tx-fn! conn2)]
      (setup-schema! tx1!)
      (setup-schema! tx2!)

      (tx1! [{:m/id "id" :m/info "original-data"}])
      (tx1! [{:m/id "id" :m/info "bad-data"}])
      (tx1! [{:m/id "id" :m/info "good-data"}])

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

        (impl/add-rewrite-job! conn1 "job" org-history org-history)
        (impl/add-rewrite-job! conn2 "job" org-history org-history)

        ; This will wipe the existing data:
        (job-init! conn1 "job")

        ; Fake excision is done for conn2:
        @(d/transact conn2 [{:rh/id "job" :rh/state :rewrite-history}])

        (is (= (replay/get-new-history conn1 "job") org-history))
        (is (= (replay/get-new-history conn2 "job") org-history))

        (while (not= :done (replay/job-state conn2 "job"))
          (replay/process-job-step! conn2 "job"))

        (is (= (rh/pull-flat-history conn2 [:m/id "id"])
               org-history))))))
