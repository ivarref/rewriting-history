(ns no.nsd.dbfns.set-disj-if-empty-test
  (:require [clojure.test :refer :all]
            [no.nsd.dbfns.datomic-generate-fn :as genfn]
            [no.nsd.utils :as u]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [no.nsd.rewriting-history :as rh]
            [no.nsd.rewriting-history.dbfns.set-disj-if-empty :as s]))

(defn db-fn
  ([] (db-fn true))
  ([generate]
   (genfn/generate-function
     'no.nsd.rewriting-history.dbfns.set-disj-if-empty/set-disj-if-empty-fn
     :set/disj-if-empty
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
                       @(d/transact conn [[:set/disj-if-empty [:m/id "id"] :m/ref {:x :x} nil nil]]))))

  (testing "fails on non-ref attribute"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/ref :many :string]]))
      (u/is-assert-msg "expected :m/ref to be of valueType :db.type/ref"
                       @(d/transact conn [[:set/disj-if-empty [:m/id "id"] :m/ref {:x :x} nil nil]]))))

  (testing "fails on non-map input"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/ref :many :ref :component]]))
      (u/is-assert-msg "Assert failed: (map? value)"
                       @(d/transact conn [[:set/disj-if-empty [:m/id "id"] :m/ref "asdf" nil nil]])))))

(deftest set-disj-should-ok-tests
  (testing "OK on missing lookup ref"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/ref :many :ref :component]]))
      @(d/transact conn [[:set/disj-if-empty [:m/id "id"] :m/ref {:x :x} nil nil]])))

  (testing "OK on empty set"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/ref :many :ref :component]]))
      @(d/transact conn [{:m/id "id"}])
      @(d/transact conn [[:set/disj-if-empty [:m/id "id"] :m/ref {:x :x} nil nil]])))

  (testing "OK on missing element"
    (let [conn (u/empty-conn)]
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/set :many :ref :component]
                                                  [:e/id :one :string]]))
      @(d/transact conn [{:m/id "id" :m/set #{{:e/id "a"}}}])
      @(d/transact conn [[:set/disj-if-empty [:m/id "id"] :m/set {:e/id "b"} nil nil]])

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

      @(d/transact conn [[:set/disj-if-empty [:m/id "id"] :m/set {:e/id "b"} nil nil]])

      (is (= (->> (d/pull (d/db conn) [:*] [:m/id "id"])
                  :m/set
                  (mapv :e/id)
                  (sort)
                  (vec))
             ["a" "c"])))))

(deftest set-disj-should-ok-tests-2
  (testing "if empty"
    (let [conn (u/empty-conn)]
      @(d/transact conn rh/schema)
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/state :one :keyword]
                                                  [:m/set :many :ref :component]
                                                  [:e/a :one :string]]))
      @(d/transact conn [{:m/id "id" :m/state :pending}])
      (is (= (s/set-disj-if-empty-fn (d/db conn)
                                     [:m/id "id"]
                                     :m/set {:e/a "nothing"}
                                     [[:some/retract [:m/id "id"] :m/state]]
                                     nil)
             [[:some/retract [:m/id "id"] :m/state]]))
      @(d/transact conn [(s/set-disj-if-empty [:m/id "id"]
                                              :m/set {:e/a "nothing"}
                                              [[:some/retract [:m/id "id"] :m/state]]
                                              nil)])
      (is (nil? (d/q '[:find ?state .
                       :where
                       [?e :m/id "id"]
                       [?e :m/state ?state]]
                     (d/db conn))))))

  (testing "if some"
    (let [conn (u/empty-conn)]
      @(d/transact conn rh/schema)
      @(d/transact conn (into [(db-fn)] #d/schema[[:m/id :one :string :id]
                                                  [:m/state :one :keyword]
                                                  [:m/set :many :ref :component]
                                                  [:e/a :one :string]]))
      @(d/transact conn [{:m/id "id" :m/state :pending
                          :m/set #{{:e/a "a"}}}])
      (is (= (s/set-disj-if-empty-fn (d/db conn)
                                     [:m/id "id"]
                                     :m/set {:e/a "nothing"}
                                     nil
                                     [{:m/id "id" :m/state :scheduled}])
             [#:m{:id "id", :state :scheduled}]))
      @(d/transact conn [(s/set-disj-if-empty [:m/id "id"]
                                              :m/set {:e/a "nothing"}
                                              [[:some/retract [:m/id "id"] :m/state]]
                                              [{:m/id "id" :m/state :scheduled}])])
      (is (= :scheduled (d/q '[:find ?state .
                               :where
                               [?e :m/id "id"]
                               [?e :m/state ?state]]
                             (d/db conn)))))))

