(ns no.nsd.rewriting-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [datomic-schema.core :as ds]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.envelope :as envelope]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.spy :as sc]
            [clojure.pprint :as pprint]))

(envelope/init!
  {:min-level  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
                [#{"*"} :info]]
   :log-to-elk false})

(deftest basic-history-pull-test
  (let [conn (u/empty-conn)]
    @(d/transact conn #d/schema[[:m/id :one :string :id]
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
                                [:country/region :one :string]])

    @(d/transact conn [{:m/id      "id-1"
                        :m/info    "hello world"
                        :m/type    :type/standard
                        :m/vedlegg [{:vedlegg/id   "vedlegg-1"
                                     :vedlegg/info "vedlegg 1: hei"}
                                    {:vedlegg/id   "vedlegg-2"
                                     :vedlegg/info "hei2"}]
                        :m/address {:addr/country
                                    {:country/name   "Norway"
                                     :country/region "West Europe"}}}])

    @(d/transact conn [{:m/id      "id-1"
                        :m/vedlegg [{:vedlegg/id   "vedlegg-1"
                                     :vedlegg/info "vedlegg 1: XXX har syfilis"}]}])

    @(d/transact conn [{:m/id      "id-1"
                        :m/type    :type/special
                        :m/vedlegg [{:vedlegg/id   "vedlegg-1"
                                     :vedlegg/info "vedlegg 1: oops!"}]
                        :m/address {:addr/country
                                    {:country/name   "Norway"
                                     :country/region "Europe"}}}])
    (let [db (d/db conn)
          fh (u/simplify-eavtos db (rh/pull-flat-history db [:m/id "id-1"]))]
      (is (= [[1 :m/address 4 1 true]
              [1 :m/id "id-1" 1 true]
              [1 :m/info "hello world" 1 true]
              [1 :m/type :type/standard 1 true]
              [1 :m/vedlegg 2 1 true]
              [1 :m/vedlegg 3 1 true]
              [2 :vedlegg/id "vedlegg-1" 1 true]
              [2 :vedlegg/info "vedlegg 1: hei" 1 true]
              [3 :vedlegg/id "vedlegg-2" 1 true]
              [3 :vedlegg/info "hei2" 1 true]
              [4 :addr/country 5 1 true]
              [5 :country/name "Norway" 1 true]
              [5 :country/region "West Europe" 1 true]

              [2 :vedlegg/info "vedlegg 1: hei" 2 false]
              [2 :vedlegg/info "vedlegg 1: XXX har syfilis" 2 true]

              [1 :m/address 4 3 false]
              [1 :m/address 6 3 true]
              [1 :m/type :type/standard 3 false]
              [1 :m/type :type/special 3 true]
              [2 :vedlegg/info "vedlegg 1: XXX har syfilis" 3 false]
              [2 :vedlegg/info "vedlegg 1: oops!" 3 true]
              [5 :country/region "West Europe" 3 false]
              [5 :country/region "Europe" 3 true]
              [6 :addr/country 5 3 true]]
             fh))
      (sc/spy)
      #_(pprint/pprint (impl/history->transactions (d/db conn) fh)))))

(deftest history->txes-test
  (let [conn (u/empty-conn)]
    @(d/transact conn #d/schema[[:m/id :one :string :id]
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
                                [:country/region :one :string]])

    (let [entity (get-in @(d/transact conn [{:db/id     "entity"
                                             :m/id      "id-1"
                                             :m/type    :type/standard
                                             :m/address {:addr/country
                                                         {:country/name   "Norway"
                                                          :country/region "West Europe"}}}])
                         [:tempids "entity"])
          db (d/db conn)
          hist (impl/pull-flat-history db [:m/id "id-1"])
          [tx] (impl/history->transactions db (u/simplify-eavtos db hist))]
      (is (= [[:db/add "1" :m/address "2"]
              [:db/add "1" :m/id "id-1"]
              [:db/add "1" :m/type :type/standard]
              [:db/add "2" :addr/country "3"]
              [:db/add "3" :country/name "Norway"]
              [:db/add "3" :country/region "West Europe"]]
             tx))
      @(d/transact conn [[:db.fn/retractEntity [:m/id "id-1"]]])
      (is (not= entity
                (-> @(d/transact conn tx)
                    (get-in [:tempids "1"]))))
      (let [new-db (d/db conn)]
        (is (= (u/simplify-eavtos db hist)
               (u/simplify-eavtos new-db (impl/pull-flat-history new-db [:m/id "id-1"]))))
        (sc/spy)))))

(deftest history->txes-test-unsimplified
  (let [conn (u/empty-conn)]
    @(d/transact conn #d/schema[[:m/id :one :string :id]
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
                                [:country/region :one :string]])

    @(d/transact conn [{:db/id     "entity"
                        :m/id      "id-1"
                        :m/type    :type/standard
                        :m/address {:addr/country
                                    {:country/name   "Norway"
                                     :country/region "West Europe"}}}])

    (let [hist (impl/pull-flat-history (d/db conn) [:m/id "id-1"])
          [tx] (impl/history->transactions (d/db conn) hist)]
      @(d/transact conn [[:db.fn/retractEntity [:m/id "id-1"]]])
      @(d/transact conn tx)
      (is (= [[1 :m/address 2 1 true]
              [1 :m/id "id-1" 1 true]
              [1 :m/type :type/standard 1 true]
              [2 :addr/country 3 1 true]
              [3 :country/name "Norway" 1 true]
              [3 :country/region "West Europe" 1 true]]
             (u/simplify-eavtos (d/db conn) (impl/pull-flat-history (d/db conn) [:m/id "id-1"]))))
      (sc/spy))))
