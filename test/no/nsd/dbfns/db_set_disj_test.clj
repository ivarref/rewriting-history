(ns no.nsd.dbfns.db-set-disj-test
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
     'no.nsd.rewriting-history.dbfns.set-disj/set-disj
     :set/disj
     generate)))

(deftest write-fn
  (db-fn true)
  (is (= 1 1)))

(deftest set-disj-should-fail-tests
  (testing "fails on not-many attribute"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/ref :one :ref :component]]))
      (u/is-assert-msg "expected attribute to have cardinality :db.cardinality/many"
                       @(d/transact conn [[:set/disj [:m/id "id"] :m/ref {:x :x}]]))))

  (testing "fails on non-ref attribute"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/ref :many :string]]))
      (u/is-assert-msg "expected :m/ref to be of valueType :db.type/ref"
                     @(d/transact conn [[:set/disj [:m/id "id"] :m/ref {:x :x}]]))))

  (testing "fails on non-map input"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/ref :many :ref :component]]))
      (u/is-assert-msg "Assert failed: (map? value)"
                     @(d/transact conn [[:set/disj [:m/id "id"] :m/ref "asdf"]])))))

(deftest set-disj-should-ok-tests
  (testing "OK on missing lookup ref"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/ref :many :ref :component]]))
      @(d/transact conn [[:set/disj [:m/id "id"] :m/ref {:x :x}]])))

  (testing "OK on empty set"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/ref :many :ref :component]]))
      @(d/transact conn [{:m/id "id"}])
      @(d/transact conn [[:set/disj [:m/id "id"] :m/ref {:x :x}]])))

  (testing "OK on missing element"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/set :many :ref :component]
                                                  [:e/id :one :string]]))
      @(d/transact conn [{:m/id "id" :m/set #{{:e/id "a"}}}])
      @(d/transact conn [[:set/disj [:m/id "id"] :m/set {:e/id "b"}]])

      (is (= (->> (d/pull (d/db conn) [:*] [:m/id "id"])
                  :m/set
                  (mapv :e/id)
                  (sort)
                  (vec))
             ["a"]))))

  (testing "OK on found element"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/set :many :ref :component]
                                                  [:e/id :one :string]]))
      @(d/transact conn [{:m/id "id" :m/set #{{:e/id "a"} {:e/id "b"} {:e/id "c"}}}])

      @(d/transact conn [[:set/disj [:m/id "id"] :m/set {:e/id "b"}]])

      (is (= (->> (d/pull (d/db conn) [:*] [:m/id "id"])
                  :m/set
                  (mapv :e/id)
                  (sort)
                  (vec))
             ["a" "c"])))))