(ns no.nsd.db-set-union-fn-test
  (:require [clojure.test :refer :all]
            [no.nsd.datomic-generate-fn :as genfn]
            [no.nsd.rewriting-history.db-set-union-fn :as s]
            [no.nsd.utils :as u]
            [no.nsd.shorter-stacktrace]
            [no.nsd.log-init]
            [datomic.api :as d]))

(defn db-fn
  ([] (db-fn false))
  ([generate]
   (genfn/generate-function
     'no.nsd.rewriting-history.db-set-union-fn/set-union
     :set/union
     generate)))

(def empty-conn u/empty-conn)

(deftest rationale
  (testing "Show how Datomic's default set addition is not optimal for components"
    (let [conn (empty-conn)]
      @(d/transact conn #d/schema[[:m/id :one :string :id]
                                  [:m/set :many :ref :component]
                                  [:r/name :one :string]])

      @(d/transact conn [{:m/id "id"
                          :m/set #{{:r/name "banana"}}}])

      @(d/transact conn [{:m/id "id"
                          :m/set #{{:r/name "banana"}}}])

      ; There are now two bananas, but I would expect (and like) a single one
      (is (= (->> (d/pull (d/db conn) '[:*] [:m/id "id"])
                  :m/set
                  (mapv :r/name))
             ["banana" "banana"]))))

  (testing "Show how Datomic's default set addition behaves for refs"
    (let [conn (empty-conn)]
      @(d/transact conn #d/schema[[:m/id :one :string :id]
                                  [:m/set :many :ref]
                                  [:r/name :one :string]])

      @(d/transact conn [{:m/id "id"
                          :m/set #{{:db/id "banana1" :r/name "banana"}}}])

      @(d/transact conn [{:m/id "id"
                          :m/set #{{:db/id "banana1" :r/name "banana"}}}])

      (is (= (->> (d/pull (d/db conn) '[:*] [:m/id "id"])
                  :m/set
                  (mapv (fn [{:db/keys [id]}] (d/pull (d/db conn) '[:*] id)))
                  (mapv :r/name))
             ["banana" "banana"]))))

  (testing "Show how Datomic's default set addition behaves for primitives"
    (let [conn (empty-conn)]
      @(d/transact conn #d/schema[[:m/id :one :string :id]
                                  [:m/set :many :string]])

      @(d/transact conn [{:m/id "id"
                          :m/set #{"banana"}}])

      @(d/transact conn [{:m/id "id"
                          :m/set #{"banana"}}])

      (is (= (->> (d/pull (d/db conn) '[:*] [:m/id "id"])
                  :m/set)
             ["banana"])))))

(deftest verify-primitives-work
  (testing "verify that primitives work"
    (with-redefs [s/rand-id (let [c (atom 0)]
                              (fn [] (str "randid-" (swap! c inc))))]
      (let [conn (empty-conn)]
        @(d/transact conn #d/schema[[:m/id :one :string :id]
                                    [:m/set :many :string]])
        @(d/transact conn [(db-fn)])

        @(d/transact conn [[:set/union [:m/id "id"] :m/set #{"banana"}]])
        @(d/transact conn [[:set/union [:m/id "id"] :m/set #{"banana"}]])

        (is (= (->> (d/pull (d/db conn) '[:*] [:m/id "id"])
                    :m/set)
               ["banana"]))

        @(d/transact conn [[:set/union [:m/id "id"] :m/set #{"pancakes"}]])

        (is (= (->> (d/pull (d/db conn) '[:*] [:m/id "id"])
                    :m/set
                    (sort)
                    (vec))
               ["banana" "pancakes"]))))))

(deftest verify-component-refs-work
  (with-redefs [s/rand-id (let [c (atom 0)]
                            (fn [] (str "randid-" (swap! c inc))))]
    (let [conn (empty-conn)]
      @(d/transact conn #d/schema[[:m/id :one :string :id]
                                  [:m/set :many :ref :component]
                                  [:r/name :one :string]])
      @(d/transact conn [(db-fn)])

      (is (= (s/set-union conn [:m/id "id"] :m/set #{{:r/name "banana"}})
             [{:m/id "id", :db/id "randid-1"}
              {:db/id "randid-2", :r/name "banana"}
              [:db/add "randid-1" :m/set "randid-2"]]))
      @(d/transact conn [[:set/union [:m/id "id"] :m/set #{{:r/name "banana"}}]])

      (is (= (->> (d/pull (d/db conn) '[:*] [:m/id "id"])
                  :m/set
                  (mapv :r/name))
             ["banana"]))

      ; Adding identical set element is a no-op:
      (is (= (s/set-union conn [:m/id "id"] :m/set #{{:r/name "banana"}})
             [{:m/id "id", :db/id "randid-3"}]))

      @(d/transact conn [[:set/union [:m/id "id"] :m/set #{{:r/name "pancakes"}}]])

      (is (= (->> (d/pull (d/db conn) '[:*] [:m/id "id"])
                  :m/set
                  (mapv :r/name)
                  (sort)
                  (vec))
             ["banana" "pancakes"])))))

(deftest verify-refs-work
  (with-redefs [s/rand-id (let [c (atom 0)]
                            (fn [] (str "randid-" (swap! c inc))))]
    (let [conn (empty-conn)]
      @(d/transact conn #d/schema[[:m/id :one :string :id]
                                  [:m/set :many :ref]
                                  [:r/name :one :string]])
      @(d/transact conn [(db-fn)])

      (is (= (s/set-union conn [:m/id "id"] :m/set #{{:r/name "banana"}})
             [{:m/id "id", :db/id "randid-1"}
              {:db/id "randid-2", :r/name "banana"}
              [:db/add "randid-1" :m/set "randid-2"]]))
      @(d/transact conn [[:set/union [:m/id "id"] :m/set #{{:r/name "banana"}}]])

      (is (= (->> (d/pull (d/db conn) '[:*] [:m/id "id"])
                  :m/set
                  (mapv :db/id)
                  (mapv (partial u/pull-id (d/db conn)))
                  (mapv :r/name))
             ["banana"]))

      ; Adding identical set element is a no-op:
      (is (= (s/set-union conn [:m/id "id"] :m/set #{{:r/name "banana"}})
             [{:m/id "id", :db/id "randid-3"}]))

      @(d/transact conn [[:set/union [:m/id "id"] :m/set #{{:r/name "pancakes"}}]])

      (is (= (->> (d/pull (d/db conn) '[:*] [:m/id "id"])
                  :m/set
                  (mapv :db/id)
                  (mapv (partial u/pull-id (d/db conn)))
                  (mapv :r/name)
                  (sort)
                  (vec))
             ["banana" "pancakes"])))))

(deftest generate-fn-test
  (db-fn true)
  (is (= 1 1)))