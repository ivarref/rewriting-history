(ns no.nsd.stage-integration-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [clojure.tools.logging :as log]
            [datomic.api :as d]
            [datomic-schema.core]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.shorter-stacktrace]
            [no.nsd.rewriting-history.impl :as impl]))

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
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/info :one :string]
                                [:db/txInstant2 :one :instant]])
    @(d/transact conn [{:m/id "id" :m/info "original-data"}])
    @(d/transact conn [{:m/id "id" :m/info "bad-data"}])
    @(d/transact conn [{:m/id "id" :m/info "good-data"}])

    ; Verify that all expected data is present in the database:
    (is (= #{"original-data" "bad-data" "good-data"} (db-values-set conn)))

    (let [org-history (rh/pull-flat-history conn [:m/id "id"])
          new-history (mapv (fn [[e a v t op]]
                              [e a (if (= v "bad-data")
                                     "nice-data"
                                     v)
                               t op])
                            org-history)]

      ; Original history looks like this:
      (is (= org-history
             [[1 :db/txInstant2 #inst "1972-01-01T00:00:00.000-00:00" 1 true]
              [4 :m/id "id" 1 true]
              [4 :m/info "original-data" 1 true]
              [2 :db/txInstant2 #inst "1973-01-01T00:00:00.000-00:00" 2 true]
              [4 :m/info "original-data" 2 false]
              [4 :m/info "bad-data" 2 true]
              [3 :db/txInstant2 #inst "1974-01-01T00:00:00.000-00:00" 3 true]
              [4 :m/info "bad-data" 3 false]
              [4 :m/info "good-data" 3 true]]))

      ; Excise original data:
      (if-let [eids (some->> org-history (meta) :original-eids not-empty)]
        (let [{:keys [db-after]} @(d/transact conn (mapv (fn [eid] {:db/excise eid}) eids))]
          @(d/sync-excise conn (d/basis-t db-after)))
        (do
          (log/error "original eids not found!")
          (assert false "original eids not found!")))

      ; Verify that the database is empty after excision:
      (is (= #{} (db-values-set conn)))

      ; Replay new history:
      (impl/apply-txes! conn (impl/history->transactions conn new-history))

      ; Verify that the database is correct after replay of history:
      ; Notice that now 'nice-data' is here:
      (is (= #{"original-data" "nice-data" "good-data"} (db-values-set conn)))

      ; The new history is identical to what we put in:
      (is (= new-history
             (rh/pull-flat-history conn [:m/id "id"]))))))

