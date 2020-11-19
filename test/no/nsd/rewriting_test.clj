(ns no.nsd.rewriting-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [datomic-schema.core :as ds]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.envelope :as envelope]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.shorter-stacktrace]
            [no.nsd.spy :as sc]
            [clojure.pprint :as pprint]))

(envelope/init!
  {:min-level  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
                [#{"*"} :info]]
   :log-to-elk false})

(deftest basic-history-pull-test
  (let [conn (u/empty-conn)]
    @(d/transact conn (conj #d/schema[[:m/id :one :string :id]
                                      [:m/info :one :string]
                                      [:m/address :one :ref :component]
                                      [:m/vedlegg :many :ref :component]
                                      [:m/type :one :ref]
                                      [:type/standard :enum]
                                      [:type/special :enum]
                                      [:vedlegg/id :one :string :id]
                                      [:vedlegg/info :one :string]
                                      [:addr/country :one :ref :component]
                                      [:country/name :one :string :id]
                                      [:country/region :one :string]]
                            {:db/id        "datomic.tx"
                             :db/txInstant #inst"2000"}))

    @(d/transact conn [{:m/id      "id-1"
                        :m/info    "hello world"
                        :m/type    :type/standard
                        :m/vedlegg [{:vedlegg/id   "vedlegg-1"
                                     :vedlegg/info "vedlegg 1: hei"}
                                    {:vedlegg/id   "vedlegg-2"
                                     :vedlegg/info "hei2"}]
                        :m/address {:addr/country
                                    {:country/name   "Norway"
                                     :country/region "West Europe"}}}
                       {:db/id        "datomic.tx"
                        :db/txInstant #inst"2001"}])

    @(d/transact conn [{:m/id      "id-1"
                        :m/vedlegg [{:vedlegg/id   "vedlegg-1"
                                     :vedlegg/info "vedlegg 1: XXX har syfilis"}]}
                       {:db/id        "datomic.tx"
                        :db/txInstant #inst"2002"}])

    @(d/transact conn [{:m/id      "id-1"
                        :m/type    :type/special
                        :m/vedlegg [{:vedlegg/id   "vedlegg-1"
                                     :vedlegg/info "vedlegg 1: oops!"}]
                        :m/address {:addr/country
                                    {:country/name   "Norway"
                                     :country/region "Europe"}}}
                       {:db/id        "datomic.tx"
                        :db/txInstant #inst"2003"}])

    (let [fh (impl/pull-flat-history-simple conn [:m/id "id-1"])]
      (is (= [[1 :db/txInstant2 #inst "2001" 1 true]
              [4 :m/address 7 1 true]
              [4 :m/id "id-1" 1 true]
              [4 :m/info "hello world" 1 true]
              [4 :m/type :type/standard 1 true]
              [4 :m/vedlegg 5 1 true]
              [4 :m/vedlegg 6 1 true]
              [5 :vedlegg/id "vedlegg-1" 1 true]
              [5 :vedlegg/info "vedlegg 1: hei" 1 true]
              [6 :vedlegg/id "vedlegg-2" 1 true]
              [6 :vedlegg/info "hei2" 1 true]
              [7 :addr/country 8 1 true]
              [8 :country/name "Norway" 1 true]
              [8 :country/region "West Europe" 1 true]

              [2 :db/txInstant2 #inst "2002" 2 true]
              [5 :vedlegg/info "vedlegg 1: hei" 2 false]
              [5 :vedlegg/info "vedlegg 1: XXX har syfilis" 2 true]

              [3 :db/txInstant2 #inst "2003" 3 true]
              [4 :m/address 7 3 false]
              [4 :m/address 9 3 true]
              [4 :m/type :type/standard 3 false]
              [4 :m/type :type/special 3 true]
              [5 :vedlegg/info "vedlegg 1: XXX har syfilis" 3 false]
              [5 :vedlegg/info "vedlegg 1: oops!" 3 true]
              [8 :country/region "West Europe" 3 false]
              [8 :country/region "Europe" 3 true]
              [9 :addr/country 8 3 true]]
             fh))
      @(d/transact conn [[:db.fn/retractEntity [:m/id "id-1"]]])
      (impl/apply-txes! conn (impl/history->transactions conn fh))
      (is (= fh (impl/pull-flat-history-simple conn [:m/id "id-1"]))))))

(deftest history->txes-test
  (let [conn (u/empty-conn)]
    @(d/transact conn (conj #d/schema[[:m/id :one :string :id]
                                      [:m/info :one :string]
                                      [:m/address :one :ref :component]
                                      [:m/vedlegg :many :ref :component]
                                      [:m/type :one :ref]
                                      [:type/standard :enum]
                                      [:type/special :enum]
                                      [:vedlegg/id :one :string :id]
                                      [:vedlegg/info :one :string]
                                      [:addr/country :one :ref :component]
                                      [:country/name :one :string :id]
                                      [:country/region :one :string]]
                            {:db/id        "datomic.tx"
                             :db/txInstant #inst"1999"}))

    (let [entity (get-in @(d/transact conn [{:db/id     "entity"
                                             :m/id      "id-1"
                                             :m/type    :type/standard
                                             :m/address {:addr/country
                                                         {:country/name   "Norway"
                                                          :country/region "West Europe"}}}
                                            {:db/id        "datomic.tx"
                                             :db/txInstant #inst"2000"}])
                         [:tempids "entity"])
          hist (impl/pull-flat-history-simple conn [:m/id "id-1"])
          [tx] (impl/history->transactions conn hist)]
      (is (= [[:db/add "datomic.tx" :db/txInstant2 #inst"2000"]
              [:db/add "2" :m/address "3"]
              [:db/add "2" :m/id "id-1"]
              [:db/add "2" :m/type :type/standard]
              [:db/add "3" :addr/country "4"]
              [:db/add "4" :country/name "Norway"]
              [:db/add "4" :country/region "West Europe"]]
             tx))
      @(d/transact conn [[:db.fn/retractEntity [:m/id "id-1"]]])
      (is (not= entity
                (-> @(d/transact conn tx)
                    (get-in [:tempids "1"]))))
      (is (= hist (impl/pull-flat-history-simple conn [:m/id "id-1"]))))))

(deftest history->txes-test-unsimplified
  (let [conn (u/empty-conn)]
    @(d/transact conn (conj #d/schema[[:m/id :one :string :id]
                                      [:m/info :one :string]
                                      [:m/address :one :ref :component]
                                      [:m/vedlegg :many :ref :component]
                                      [:m/type :one :ref]
                                      [:type/standard :enum]
                                      [:type/special :enum]
                                      [:vedlegg/id :one :string :id]
                                      [:vedlegg/info :one :string]
                                      [:addr/country :one :ref :component]
                                      [:country/name :one :string :id]
                                      [:country/region :one :string]]
                            {:db/id        "datomic.tx"
                             :db/txInstant #inst"1999"}))

    @(d/transact conn [{:db/id     "entity"
                        :m/id      "id-1"
                        :m/type    :type/standard
                        :m/address {:addr/country
                                    {:country/name   "Norway"
                                     :country/region "West Europe"}}}
                       {:db/id "datomic.tx"
                        :db/txInstant #inst"2000"}])
    (let [hist (impl/pull-flat-history conn [:m/id "id-1"])
          [tx] (impl/history->transactions conn hist)]
      @(d/transact conn [[:db.fn/retractEntity [:m/id "id-1"]]])
      @(d/transact conn tx)
      (is (= [[1 :db/txInstant2 #inst"2000" 1 true]
              [2 :m/address 3 1 true]
              [2 :m/id "id-1" 1 true]
              [2 :m/type :type/standard 1 true]
              [3 :addr/country 4 1 true]
              [4 :country/name "Norway" 1 true]
              [4 :country/region "West Europe" 1 true]]
             (impl/pull-flat-history-simple conn [:m/id "id-1"]))))))
