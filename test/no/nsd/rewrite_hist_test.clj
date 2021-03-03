(ns no.nsd.rewrite-hist-test
  (:require [clojure.test :refer :all]
            [datomic-schema.core]
            [no.nsd.utils :as u]
            [no.nsd.log-init]
            [datomic.api :as d]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.shorter-stacktrace]
            [no.nsd.rewriting-history.impl :as impl]))

(deftest rewrite-hist-test
  (let [schema (conj #d/schema[[:m/id :one :string :id]
                               [:m/info :one :string]
                               [:m/address :one :ref :component]
                               [:addr/country :one :ref :component]
                               [:country/name :one :string :id]
                               [:country/region :one :string]
                               [:db/txInstant2 :one :instant]]
                     {:db/id        "datomic.tx"
                      :db/txInstant #inst"1999"})
        conn (u/empty-conn schema)
        _  @(d/transact conn [{:m/id      "id"
                               :m/address {:addr/country
                                           {:country/name   "Norway"
                                            :country/region "West Europe"}}}
                              {:db/id        "datomic.tx"
                               :db/txInstant #inst"2000"}])
        _@(d/transact conn [{:m/id      "id"
                             :m/address {:addr/country
                                         {:country/name   "Norway"
                                          :country/region "Europe"}}}
                            {:db/id        "datomic.tx"
                             :db/txInstant #inst"2001"}])
        fh (rh/pull-flat-history conn [:m/id "id"])
        txes (impl/history->transactions conn fh)]
    (is (= [[[:db/add "datomic.tx" :db/txInstant2 #inst"2000"]
             [:db/add "3" :m/address "4"]
             [:db/add "3" :m/id "id"]
             [:db/add "4" :addr/country "5"]
             [:db/add "5" :country/name "Norway"]
             [:db/add "5" :country/region "West Europe"]]

            [[:db/add "datomic.tx" :db/txInstant2 #inst"2001"]
             [:db/retract [:tempid "3"] :m/address [:tempid "4"]]
             [:db/add [:tempid "3"] :m/address "6"]
             [:db/retract [:tempid "5"] :country/region "West Europe"]
             [:db/add [:tempid "5"] :country/region "Europe"]
             [:db/add "6" :addr/country [:tempid "5"]]]]
           txes))
    (let [conn (u/empty-conn schema)]
      (impl/apply-txes! conn txes)
      (is (= fh (rh/pull-flat-history conn [:m/id "id"]))))))
