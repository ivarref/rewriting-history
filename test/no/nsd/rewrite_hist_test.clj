(ns no.nsd.rewrite-hist-test
  (:require [clojure.test :refer :all]
            [datomic-schema.core :as ds]
            [no.nsd.utils :as u]
            [no.nsd.envelope :as envelope]
            [no.nsd.spy :as sc]
            [datomic.api :as d]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.shorter-stacktrace]
            [no.nsd.rewriting-history.impl :as impl]
            [taoensso.timbre :as timbre]))

(envelope/init!
  {:min-level  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
                [#{"*"} :info]]
   :log-to-elk false})

(defn setup! [conn]
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
                      :m/address {:addr/country
                                  {:country/name   "Norway"
                                   :country/region "West Europe"}}}])

  @(d/transact conn [{:m/id      "id-1"
                      :m/address {:addr/country
                                  {:country/name   "Norway"
                                   :country/region "Europe"}}}]))

(deftest rewrite-hist-test
  (let [conn (u/empty-conn)
        _ (setup! conn)
        db (d/db conn)
        fh (impl/pull-flat-history-simple db [:m/id "id-1"])
        txes (impl/history->transactions db fh)]
    (is (= [[[:db/add "1" :m/address "2"]
             [:db/add "1" :m/id "id-1"]
             [:db/add "2" :addr/country "3"]
             [:db/add "3" :country/name "Norway"]
             [:db/add "3" :country/region "West Europe"]]

            [[:db/retract [:tempid "1"] :m/address [:tempid "2"]]
             [:db/add [:tempid "1"] :m/address "4"]
             [:db/retract [:tempid "3"] :country/region "West Europe"]
             [:db/add [:tempid "3"] :country/region "Europe"]
             [:db/add "4" :addr/country [:tempid "3"]]]]
           txes))
    @(d/transact conn [[:db.fn/retractEntity [:m/id "id-1"]]])
    (try
      (timbre/with-merged-config {:min-level :fatal}
        (impl/pull-flat-history (d/db conn) [:m/id "id-1"]))
      (assert false "should not get here")
      (catch Exception e
        (is (= "Could not find lookup ref" (.getMessage e)))))
    (impl/apply-txes! conn txes)
    (let [fh2 (impl/pull-flat-history-simple (d/db conn) [:m/id "id-1"])]
      (is (= fh2 fh)))))
