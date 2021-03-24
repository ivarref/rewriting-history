(ns no.nsd.tx-metadata-test
  (:require [clojure.test :refer :all]
            [datomic-schema.core]
            [datomic.api :as d]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.tx :as tx]
            [clojure.pprint :as pprint]))

(deftest tx-meta-test
  (testing "Verify that transaction metadata is included in history"
    (let [conn (u/empty-conn)]
      @(d/transact conn (reduce into []
                                [#d/schema[[:m/id :one :string :id]
                                           [:m/info :one :string]
                                           [:tx/info :one :string]]
                                 impl/schema]))
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
        (let [[tx] (tx/generate-tx conn [:m/id "id"] fh)]
          (is (= tx
                 [[:db/add "datomic.tx" :tx/info "meta"]
                  [:db/add "datomic.tx" :tx/txInstant #inst "1972-01-01T00:00:00.000-00:00"]
                  [:db/add "2" :m/id "id"]
                  [:db/add "2" :m/info "hello world"]
                  [:set/union
                   [:rh/lookup-ref "[:m/id \"id\"]"]
                   :rh/tempids
                   #{#:rh{:tempid-str "2", :tempid-ref "2"} #:rh{:tempid-str "1", :tempid-ref "datomic.tx"}}]
                  [:db/cas [:rh/lookup-ref "[:m/id \"id\"]"] :rh/state :rewrite-history :verify]]))
          (u/rewrite-noop! conn [:m/id "id"])
          (is (= fh (impl/pull-flat-history-simple conn [:m/id "id"]))))))))

(deftest tx-meta-ref-test
  (testing "Verify that :ent/ref 'datomic.tx' works"
    (let [conn (u/empty-conn)]
      @(d/transact conn (reduce into []
                                [#d/schema[[:m/id :one :string :id]
                                           [:m/info :one :string]
                                           [:m/ref :one :ref]
                                           [:tx/info :one :string]
                                           [:tx/txInstant :one :instant]]
                                 impl/schema]))
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
        (let [[tx] (tx/generate-tx conn [:m/id "id"] fh)]
          (is (= tx
                 [[:db/add "datomic.tx" :tx/info "meta"]
                  [:db/add "datomic.tx" :tx/txInstant #inst "1972-01-01T00:00:00.000-00:00"]
                  [:db/add "2" :m/id "id"]
                  [:db/add "2" :m/info "hello world"]
                  [:db/add "2" :m/ref "datomic.tx"]
                  [:set/union
                   [:rh/lookup-ref "[:m/id \"id\"]"]
                   :rh/tempids
                   #{#:rh{:tempid-str "2", :tempid-ref "2"} #:rh{:tempid-str "1", :tempid-ref "datomic.tx"}}]
                  [:db/cas [:rh/lookup-ref "[:m/id \"id\"]"] :rh/state :rewrite-history :verify]]))
          (u/rewrite-noop! conn [:m/id "id"])
          (is (= fh (impl/pull-flat-history-simple conn [:m/id "id"]))))))))