(ns no.nsd.dbfns.db-cas-contains-test
  (:require [clojure.test :refer :all]
            [no.nsd.dbfns.datomic-generate-fn :as genfn]
            [no.nsd.utils :as u]
            [no.nsd.log-init]
            [datomic-schema.core]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [no.nsd.shorter-stacktrace]
            [clojure.string :as str]))

(defn db-fn
  ([] (db-fn false))
  ([generate]
   (genfn/generate-function
     'no.nsd.rewriting-history.dbfns.cas-contains/cas-contains
     :cas/contains
     generate)))

(deftest write-fn
  (db-fn true)
  (is (= 1 1)))

(deftest cas-contains-should-fail-tests
  (testing "fails on many attribute"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/status :many :keyword]]))
      (u/is-assert-msg "expected attribute to have cardinality :db.cardinality/one"
                       @(d/transact conn [[:cas/contains [:m/id "id"] :m/status #{nil} :init]]))))

  (testing "fails on ref attribute"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/status :one :ref]]))
      (u/is-assert-msg "expected attribute to be of type"
                       @(d/transact conn [[:cas/contains [:m/id "id"] :m/status #{nil} :init]]))))

  (testing "fails on map input"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/status :one :keyword]]))
      (u/is-assert-msg "Assert failed: (primitive? key)"
                       @(d/transact conn [[:cas/contains [:m/id "id"] :m/status #{nil} {:a :b}]]))))

  (testing "fails on empty coll input"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/status :one :keyword]]))
      (u/is-assert-msg "expected coll to be non-empty"
                       @(d/transact conn [[:cas/contains [:m/id "id"] :m/status #{} :init]])))))

(deftest cas-ok-tests
  (let [conn (u/empty-conn)]
    @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                [:m/status :one :keyword]]))
    @(d/transact conn [[:cas/contains [:m/id "id"] :m/status #{nil} :init]])

    (u/is-assert-msg "expected key :init to be found in coll #{nil}"
                     @(d/transact conn [[:cas/contains [:m/id "id"] :m/status #{nil} :init]]))

    ; no-ops is fine:
    @(d/transact conn [[:cas/contains [:m/id "id"] :m/status #{:init} :init]])

    @(d/transact conn [[:cas/contains [:m/id "id"] :m/status #{:init} :pending]])

    @(d/transact conn [[:cas/contains [:m/id "id"] :m/status #{:init nil :pending} :done]])))