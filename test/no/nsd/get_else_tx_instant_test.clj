(ns no.nsd.get-else-tx-instant-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [datomic.api :as d]
            [no.nsd.rewriting-history.impl :as impl]))

(deftest get-else-tx-instant-test
  (let [conn (u/empty-conn)]
    @(d/transact conn impl/schema)
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/info :one :string]])

    @(d/transact conn [{:m/id "id" :m/info "info"}])

    (is (= #{#inst"1973"}
           (into #{} (d/q '[:find [?inst ...]
                            :in $
                            :where
                            [?e :m/id "id"]
                            [?e :m/info "info" ?tx true]
                            [?tx :db/txInstant ?inst]]
                          (d/history (d/db conn))))))

    (is (= #{#inst"1973"}
           (into #{} (d/q '[:find [?inst ...]
                            :in $
                            :where
                            [?e :m/id "id"]
                            [?e :m/info "info" ?tx true]
                            [?tx :db/txInstant ?inst-default]
                            [(get-else $ ?tx :tx/txInstant ?inst-default) ?inst]]
                          (d/history (d/db conn))))))

    (u/rewrite-noop! conn [:m/id "id"])

    ; rewriting-history cannot set :db/txInstant arbitrarly:
    (is (= #{#inst"1973" #inst"1981"}
           (into #{} (d/q '[:find [?inst ...]
                            :in $
                            :where
                            [?e :m/id "id"]
                            [?e :m/info "info" ?tx true]
                            [?tx :db/txInstant ?inst]]
                          (d/history (d/db conn))))))

    ; using get-else fixes this problem though:
    (is (= #{#inst"1973"}
           (into #{} (d/q '[:find [?inst ...]
                            :in $
                            :where
                            [?e :m/id "id"]
                            [?e :m/info "info" ?tx true]
                            [?tx :db/txInstant ?inst-default]
                            [(get-else $ ?tx :tx/txInstant ?inst-default) ?inst]]
                          (d/history (d/db conn))))))))
