(ns no.nsd.component-attribute-test
  (:require [clojure.test :refer :all]
            [datomic-schema.core]
            [no.nsd.utils :as u]
            [no.nsd.log-init]
            [datomic.api :as d]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.shorter-stacktrace]
            [no.nsd.rewriting-history.impl :as impl]))

(deftest component-attribute-test
  (testing "Verify component attribute gets new entity id if we do not specify :db/id"
    (let [schema (conj #d/schema[[:m/id :one :string :id]
                                 [:m/address :one :ref :component]
                                 [:m/country :one :ref :component]
                                 [:country/name :one :string :id]
                                 [:db/txInstant2 :one :instant]]
                       {:db/id        "datomic.tx"
                        :db/txInstant #inst"1999"})
          conn (u/empty-conn schema)
          _ @(d/transact conn [{:m/id      "id"
                                :m/address {:m/country {:country/name "Norway"}}}
                               {:db/id        "datomic.tx"
                                :db/txInstant #inst"2000"}])
          _ @(d/transact conn [{:m/id      "id"
                                :m/address {:m/country {:country/name "Norway"}}}
                               {:db/id        "datomic.tx"
                                :db/txInstant #inst"2001"}])
          fh (rh/pull-flat-history conn [:m/id "id"])]
      (is (= (->> fh
                  (filterv #(= :m/address (second %)))
                  (mapv last))
             [true false true]))
      (let [txes (impl/history->transactions conn fh)
            conn (u/empty-conn schema)]
        (impl/apply-txes! conn txes)
        (is (= fh (rh/pull-flat-history conn [:m/id "id"])))))))
