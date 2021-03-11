(ns no.nsd.db-set-union-fn-test
  (:require [clojure.test :refer :all]
            [no.nsd.datomic-generate-fn :as genfn]
            [no.nsd.utils :as u]
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
             ["banana" "banana"])))))