(ns no.nsd.rewriting-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [datomic-schema.core]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history.impl :as impl]
            [clojure.pprint :as pprint]
            [no.nsd.rewriting-history :as rh]))

(deftest basic-history-pull-test
  (let [conn (u/empty-conn)]
    @(d/transact conn (reduce into []
                              [#d/schema[[:m/id :one :string :id]
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
                               impl/schema]))

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

    (let [fh (rh/pull-flat-history conn [:m/id "id-1"])]
      (is (= [[-1 :tx/txInstant #inst "1972" -1 true]
              [1 :m/address 4 -1 true]
              [1 :m/id "id-1" -1 true]
              [1 :m/info "hello world" -1 true]
              [1 :m/type :type/standard -1 true]
              [1 :m/vedlegg 2 -1 true]
              [1 :m/vedlegg 3 -1 true]
              [2 :vedlegg/id "vedlegg-1" -1 true]
              [2 :vedlegg/info "vedlegg 1: hei" -1 true]
              [3 :vedlegg/id "vedlegg-2" -1 true]
              [3 :vedlegg/info "hei2" -1 true]
              [4 :addr/country 5 -1 true]
              [5 :country/name "Norway" -1 true]
              [5 :country/region "West Europe" -1 true]

              [-2 :tx/txInstant #inst "1973" -2 true]
              [2 :vedlegg/info "vedlegg 1: hei" -2 false]
              [2 :vedlegg/info "vedlegg 1: XXX har syfilis" -2 true]

              [-3 :tx/txInstant #inst "1974" -3 true]
              [1 :m/address 4 -3 false]
              [1 :m/address 6 -3 true]
              [1 :m/type :type/standard -3 false]
              [1 :m/type :type/special -3 true]
              [2 :vedlegg/info "vedlegg 1: XXX har syfilis" -3 false]
              [2 :vedlegg/info "vedlegg 1: oops!" -3 true]
              [5 :country/region "West Europe" -3 false]
              [5 :country/region "Europe" -3 true]
              [6 :addr/country 5 -3 true]]
             fh))
      (u/rewrite-noop! conn [:m/id "id-1"])
      (is (= fh (rh/pull-flat-history conn [:m/id "id-1"]))))))
