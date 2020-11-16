(ns no.nsd.rewriting-test
  (:require [clojure.test :refer :all]
            [datomic.api :as d]
            [datomic-schema.core :as ds]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.envelope :as envelope]))

(envelope/init!
  {:min-level  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
                [#{"*"} :info]]
   :log-to-elk false})

(defn empty-conn []
  (let [uri "datomic:mem://hello-world"]
    (d/delete-database uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      conn)))

(def fourth #(nth % 3))

(defn simplify-eavtos [db eavtos]
  (let [eid-map (zipmap (map first eavtos) (iterate inc 1))
        tx-map (zipmap (map fourth eavtos) (iterate inc 1))]))

(deftest asdf
  (let [conn (empty-conn)]
    (def c conn)
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
                                    {:country/name "Norway"
                                     :country/region "Europe"}}}])

    (def fh (rh/pull-flat-history (d/db conn) [:m/id "id-1"]))))
    
