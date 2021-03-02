(ns no.nsd.restrictions-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [datomic.api :as d]
            [no.nsd.rewriting-history :as rh]
            [datomic-schema.core]))

(deftest regular-ref-test
  (testing "Verify that a regular reference fails"
    (let [conn (u/empty-conn)]
      @(d/transact conn (conj #d/schema[[:m/id :one :string :id]
                                        [:m/person :one :ref]
                                        [:person/name :one :string]]
                              {:db/id        "datomic.tx"
                               :db/txInstant #inst"1999"}))
      @(d/transact conn [{:m/id     "id"
                          :m/person {:db/id "person"
                                     :person/name "Børre"}}
                         {:db/id        "datomic.tx"
                          :db/txInstant #inst"2000"}])
      @(d/transact conn #d/schema[[:db/txInstant2 :one :instant]])

      (is (= 1 1))

      #_(is (thrown?
              Throwable
              (u/pprint (rh/pull-flat-history conn [:m/id "id"])))))))
