(ns no.nsd.rationale-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [clojure.tools.logging :as log]
            [datomic.api :as d]
            [no.nsd.log-init]
            [datomic-schema.core]
            [com.github.sikt-no.datomic-testcontainers :as dtc]))

(defn db-values-set [db]
  (into (sorted-set) (d/q '[:find [?v ...]
                            :in $
                            :where
                            [?e :m/id "id" _ _ _]
                            [?e :m/info ?v _ _]]
                          (d/history db))))

(defn demo-rationale [conn]
  @(d/transact conn #d/schema[[:m/id :one :string :id]
                              [:m/info :one :string]])
  @(d/transact conn [{:m/id "id" :m/info "original-data"}])
  @(d/transact conn [{:m/id "id" :m/info "bad-data"}])
  @(d/transact conn [{:m/id "id" :m/info "good-data"}])

  ; Verify that all expected data is present in the database:
  (is (= #{"original-data" "bad-data" "good-data"} (db-values-set (d/db conn))))

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
                                                 :db.excise/beforeT t}])
          ; sync:
          db @(d/sync-excise conn (d/basis-t db-after))]

      ; Original data is deleted:
      (is (false? (contains? (db-values-set db) "original-data")))

      ; The bad data is still present in the history database as a retraction!
      ; This is the problem, and I consider it a bug in Datomic:
      (is (contains? (db-values-set db) "bad-data"))

      ; The good data is still present as well:
      (is (contains? (db-values-set db) "good-data")))))

(deftest rationale-test
  (testing "Demonstrate rationale"
    (demo-rationale (dtc/get-conn {:db-name "my-test" :delete? true}))))
