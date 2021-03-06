(ns no.nsd.rewriting-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [datomic-schema.core :as ds]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.log-init]
            [no.nsd.shorter-stacktrace]
            [clojure.pprint :as pprint]
            [no.nsd.rewriting-history :as rh]))

(deftest basic-history-pull-test
  (let [schema #d/schema[[:m/id :one :string :id]
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
                         [:country/region :one :string]
                         [:tx/txInstant :one :instant]]
        conn (u/empty-conn schema)]

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
      (is (= [[1 :tx/txInstant #inst "1972" 1 true]
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

              [2 :tx/txInstant #inst "1973" 2 true]
              [5 :vedlegg/info "vedlegg 1: hei" 2 false]
              [5 :vedlegg/info "vedlegg 1: XXX har syfilis" 2 true]

              [3 :tx/txInstant #inst "1974" 3 true]
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
      (let [txes (impl/history->transactions conn fh)
            conn (u/empty-conn schema)]
        (impl/apply-txes! conn txes)
        (is (= fh (rh/pull-flat-history conn [:m/id "id-1"])))))))


