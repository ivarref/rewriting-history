(ns no.nsd.tx-metadata-test
  (:require [clojure.test :refer :all]
            [no.nsd.envelope :as envelope]
            [datomic-schema.core]
            [datomic.api :as d]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.shorter-stacktrace]
            [no.nsd.spy :as sc]
            [clojure.pprint :as pprint]))

(envelope/init!
  {:min-level  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
                [#{"*"} :info]]
   :log-to-elk false})

(deftest tx-meta-test
  (let [schema (conj #d/schema[[:m/id :one :string :id]
                               [:m/info :one :string]
                               [:tx/info :one :string]
                               [:db/txInstant2 :one :instant]]
                     {:db/id        "datomic.tx"
                      :db/txInstant #inst "1980"})
        conn (u/empty-conn schema)]
    @(d/transact conn [{:m/id   "id"
                        :m/info "hello world"}
                       {:db/id        "datomic.tx"
                        :db/txInstant #inst"2000"
                        :tx/info      "meta"}])
    (let [fh (impl/pull-flat-history-simple conn [:m/id "id"])]
      (is (= [[1 :db/txInstant2 #inst "2000" 1 true]
              [1 :tx/info "meta" 1 true]
              [2 :m/id "id" 1 true]
              [2 :m/info "hello world" 1 true]]
             fh))
      (let [[tx] (impl/history->transactions conn fh)]
        (is (= tx
               [[:db/add "datomic.tx" :db/txInstant2 #inst "2000"]
                [:db/add "datomic.tx" :tx/info "meta"]
                [:db/add "2" :m/id "id"]
                [:db/add "2" :m/info "hello world"]]))
        (let [conn (u/empty-conn schema)]
          @(d/transact conn tx)
          (let [fh2 (impl/pull-flat-history-simple conn [:m/id "id"])]
            (is (= fh2 fh))))))))


(deftest tx-meta-ref-test
  (let [schema (conj #d/schema[[:m/id :one :string :id]
                               [:m/info :one :string]
                               [:m/ref :one :ref]
                               [:tx/info :one :string]
                               [:db/txInstant2 :one :instant]]
                     {:db/id        "datomic.tx"
                      :db/txInstant #inst "1980"})
        conn (u/empty-conn schema)]
    @(d/transact conn [{:m/id   "id"
                        :m/info "hello world"
                        :m/ref  "datomic.tx"}
                       {:db/id        "datomic.tx"
                        :db/txInstant #inst"2000"
                        :tx/info      "meta"}])
    @(d/transact conn [{:m/id "id2"} {:db/id "datomic.tx" :db/txInstant #inst"2001"}])

    (let [fh (impl/pull-flat-history-simple conn [:m/id "id"])]
      (is (= [[1 :db/txInstant2 #inst "2000" 1 true]
              [1 :tx/info "meta" 1 true]
              [2 :m/id "id" 1 true]
              [2 :m/info "hello world" 1 true]
              [2 :m/ref 1 1 true]]
             fh))
      (let [[tx] (impl/history->transactions conn fh)]
        (is (= tx
               [[:db/add "datomic.tx" :db/txInstant2 #inst "2000"]
                [:db/add "datomic.tx" :tx/info "meta"]
                [:db/add "2" :m/id "id"]
                [:db/add "2" :m/info "hello world"]
                [:db/add "2" :m/ref "datomic.tx"]]))
        (let [conn (u/empty-conn schema)]
          @(d/transact conn tx)
          (is (= fh (impl/pull-flat-history-simple conn [:m/id "id"]))))))))
