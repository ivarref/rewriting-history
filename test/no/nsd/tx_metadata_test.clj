(ns no.nsd.tx-metadata-test
  (:require [clojure.test :refer :all]
            [no.nsd.envelope :as envelope]
            [datomic-schema.core]
            [datomic.api :as d]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.spy :as sc]
            [clojure.pprint :as pprint]))

(envelope/init!
  {:min-level  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
                [#{"*"} :info]]
   :log-to-elk false})

(deftest tx-meta-test
  (let [conn (u/empty-conn)]
    @(d/transact conn (conj #d/schema[[:m/id :one :string :id]
                                      [:m/info :one :string]
                                      [:tx/info :one :string]]
                            {:db/id        "datomic.tx"
                             :db/txInstant #inst "1980"}))

    @(d/transact conn [{:m/id   "id"
                        :m/info "hello world"}
                       {:db/id        "datomic.tx"
                        :db/txInstant #inst"2000"
                        :tx/info      "meta"}])

    (let [fh (impl/pull-flat-history-simple (d/db conn) [:m/id "id"])]
      (is (= [[1 :db/txInstant #inst "2000-01-01T00:00:00.000-00:00" 1 true]
              [1 :tx/info "meta" 1 true]
              [2 :m/id "id" 1 true]
              [2 :m/info "hello world" 1 true]
              [2 :m/tx 1 1 true]]
             fh)))))

