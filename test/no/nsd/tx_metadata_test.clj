(ns no.nsd.tx-metadata-test
  (:require [clojure.test :refer :all]
            [datomic-schema.core]
            [datomic.api :as d]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.shorter-stacktrace]
            [no.nsd.log-init]
            [no.nsd.spy :as sc]
            [clojure.pprint :as pprint]))

(deftest tx-meta-test
  (testing "Verify that transaction metadata is included in history"
    (let [schema #d/schema[[:m/id :one :string :id]
                           [:m/info :one :string]
                           [:tx/info :one :string]
                           [:tx/txInstant :one :instant]]
          conn (u/empty-conn schema)]
      @(d/transact conn [{:m/id   "id"
                          :m/info "hello world"}
                         {:db/id        "datomic.tx"
                          :tx/info      "meta"}])
      (let [fh (impl/pull-flat-history-simple conn [:m/id "id"])]
        (is (= [[1 :tx/info "meta" 1 true]
                [1 :tx/txInstant #inst "1972" 1 true]
                [2 :m/id "id" 1 true]
                [2 :m/info "hello world" 1 true]]
               fh))
        (let [[tx] (impl/history->transactions conn fh)]
          (is (= tx
                 [[:db/add "datomic.tx" :tx/info "meta"]
                  [:db/add "datomic.tx" :tx/txInstant #inst "1972"]
                  [:db/add "2" :m/id "id"]
                  [:db/add "2" :m/info "hello world"]]))
          (let [conn (u/empty-conn schema)]
            @(d/transact conn tx)
            (let [fh2 (impl/pull-flat-history-simple conn [:m/id "id"])]
              (is (= fh2 fh)))))))))

(deftest tx-meta-ref-test
  (testing "Verify that :ent/ref 'datomic.tx' works"
    (let [schema #d/schema[[:m/id :one :string :id]
                           [:m/info :one :string]
                           [:m/ref :one :ref]
                           [:tx/info :one :string]
                           [:tx/txInstant :one :instant]]
          conn (u/empty-conn schema)]
      @(d/transact conn [{:m/id   "id"
                          :m/info "hello world"
                          :m/ref  "datomic.tx"}
                         {:db/id        "datomic.tx"
                          :tx/info      "meta"}])
      @(d/transact conn [{:m/id "id2"}])

      (let [fh (impl/pull-flat-history-simple conn [:m/id "id"])]
        (is (= [[1 :tx/info "meta" 1 true]
                [1 :tx/txInstant #inst "1972" 1 true]
                [2 :m/id "id" 1 true]
                [2 :m/info "hello world" 1 true]
                [2 :m/ref 1 1 true]]
               fh))
        (let [[tx] (impl/history->transactions conn fh)]
          (is (= tx
                 [[:db/add "datomic.tx" :tx/info "meta"]
                  [:db/add "datomic.tx" :tx/txInstant #inst "1972"]
                  [:db/add "2" :m/id "id"]
                  [:db/add "2" :m/info "hello world"]
                  [:db/add "2" :m/ref "datomic.tx"]]))
          (let [conn (u/empty-conn schema)]
            @(d/transact conn tx)
            (is (= fh (impl/pull-flat-history-simple conn [:m/id "id"])))))))))
