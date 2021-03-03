(ns no.nsd.rationale-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [clojure.tools.logging :as log]
            [datomic.api :as d]
            [no.nsd.log-init]
            [datomic-schema.core]))

(defn db-values-set [conn]
  (into (sorted-set) (d/q '[:find [?v ...]
                            :in $
                            :where
                            [?e :m/id "id" _ _ _]
                            [?e _ ?v _ _]]
                          (d/history (d/db conn)))))

(defn demo-rationale [stage-uri]
  (let [conn (u/empty-stage-conn "rationale-demo-test-1")]
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/info :one :string]])
    @(d/transact conn [{:m/id "id" :m/info "original-data"}])
    @(d/transact conn [{:m/id "id" :m/info "bad-data"}])
    @(d/transact conn [{:m/id "id" :m/info "good-data"}])

    ; Verify that all expected data is present in the database:
    (is (contains? (db-values-set conn) "original-data"))
    (is (contains? (db-values-set conn) "bad-data"))
    (is (contains? (db-values-set conn) "good-data"))

    ; Excise database up to (but not including) the good data.
    ; Find entity id and exicision point:
    (let [[eid t] (d/q '[:find [?e ?t]
                         :in $
                         :where
                         [?e :m/id "id" _ _ _]
                         [?e :m/info "good-data" ?t true]]
                       (d/history (d/db conn)))]
      ; Do excision:
      (let [{:keys [db-after]} @(d/transact conn [{:db/excise         eid
                                                   :db.excise/attrs   [:m/info]
                                                   :db.excise/beforeT t}])]
        ; Sync:
        @(d/sync-excise conn (d/basis-t db-after))

        ; Original data is deleted:
        (is (false? (contains? (db-values-set conn) "original-data")))

        ; The bad data is still present in the history database as a retraction!
        ; This is the problem, and I consider it a bug in Datomic:
        (is (contains? (db-values-set conn) "bad-data"))

        ; The good data is still present as well:
        (is (contains? (db-values-set conn) "good-data"))))))

(deftest rationale-test
  (testing "Demonstrate rationale"
    (if-let [stage-uri (u/stage-uri)]
      (demo-rationale stage-uri)
      (do (log/info "not demonstrating rationale (requires real database uri)")
          (is (= 1 1))))))