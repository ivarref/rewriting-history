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

  @(d/transact conn [{:m/id      "id-1"
                      :m/address {:addr/country
                                  {:country/name   "Norway"
                                   :country/region "West Europe"}}}
                     {:db/id        "datomic.tx"
                      :db/txInstant #inst"2000"}])

  @(d/transact conn [{:m/id      "id-1"
                      :m/address {:addr/country
                                  {:country/name   "Norway"
                                   :country/region "Europe"}}}
                     {:db/id        "datomic.tx"
                      :db/txInstant #inst"2001"}]))

(deftest rewrite-hist-test
  (let [conn (u/empty-conn)
        _ (setup! conn)
        fh (impl/pull-flat-history-simple conn [:m/id "id-1"])
        txes (impl/history->transactions conn fh)]
    (is (= [[[:db/add "datomic.tx" :db/txInstant2 #inst"2000"]
             [:db/add "3" :m/address "4"]
             [:db/add "3" :m/id "id-1"]
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
    @(d/transact conn [[:db.fn/retractEntity [:m/id "id-1"]]])
    (try
      (timbre/with-merged-config {:min-level :fatal}
                                 (impl/pull-flat-history conn [:m/id "id-1"]))
      (assert false "should not get here")
      (catch Exception e
        (is (= "Could not find lookup ref" (.getMessage e)))))
    (impl/apply-txes! conn txes)
    (let [fh2 (impl/pull-flat-history-simple conn [:m/id "id-1"])]
      (is (= fh2 fh)))))
