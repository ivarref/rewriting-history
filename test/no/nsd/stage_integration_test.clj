(ns no.nsd.stage-integration-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [clojure.tools.logging :as log]
            [datomic.api :as d]
            [datomic-schema.core]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.shorter-stacktrace]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.log-init]))

(defn db-values-set [conn]
  (into (sorted-set) (d/q '[:find [?v ...]
                            :in $
                            :where
                            [?e :m/id "id" _ _ _]
                            [?e :m/info ?v _ _]]
                          (d/history (d/db conn)))))

(deftest integration-test
  (is (= 1 1))
  (when-let [conn (u/empty-stage-conn "rewriting-history-integration-test-1")]
    @(d/transact conn (into
                        impl/schema
                        #d/schema[[:m/id :one :string :id]
                                  [:m/info :one :string]
                                  [:tx/txInstant :one :instant]]))
    @(d/transact conn [{:m/id "id" :m/info "original-data"}])
    @(d/transact conn [{:m/id "id" :m/info "bad-data"}])
    @(d/transact conn [{:m/id "id" :m/info "good-data"}])

    ; Verify that all expected data is present in the database:
    (is (= #{"original-data" "bad-data" "good-data"} (db-values-set conn)))

    (let [org-history (rh/pull-flat-history conn [:m/id "id"])]
      ; Original history looks like this:
      (is (= [[-1 :tx/txInstant #inst "1972-01-01T00:00:00.000-00:00" -1 true]
              [1 :m/id "id" -1 true]
              [1 :m/info "original-data" -1 true]
              [-2 :tx/txInstant #inst "1973-01-01T00:00:00.000-00:00" -2 true]
              [1 :m/info "original-data" -2 false]
              [1 :m/info "bad-data" -2 true]
              [-3 :tx/txInstant #inst "1974-01-01T00:00:00.000-00:00" -3 true]
              [1 :m/info "bad-data" -3 false]
              [1 :m/info "good-data" -3 true]]
             org-history))


      (rh/schedule-replacement! conn [:m/id "id"] "bad-data" "corrected-data")
      (rh/rewrite-scheduled! conn)

      (is (= #{"original-data" "corrected-data" "good-data"} (db-values-set conn)))

      (is (= [[-1 :tx/txInstant #inst "1972-01-01T00:00:00.000-00:00" -1 true]
              [1 :m/id "id" -1 true]
              [1 :m/info "original-data" -1 true]
              [-2 :tx/txInstant #inst "1973-01-01T00:00:00.000-00:00" -2 true]
              [1 :m/info "original-data" -2 false]
              [1 :m/info "corrected-data" -2 true]
              [-3 :tx/txInstant #inst "1974-01-01T00:00:00.000-00:00" -3 true]
              [1 :m/info "corrected-data" -3 false]
              [1 :m/info "good-data" -3 true]]
             (rh/pull-flat-history conn [:m/id "id"]))))))
