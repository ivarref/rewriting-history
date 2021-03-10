(ns no.nsd.db-set-intersection-fn-test
  (:require [clojure.test :refer :all]
            [no.nsd.log-init]
            [no.nsd.shorter-stacktrace]
            [datomic-schema.core]
            [datomic.api :as d]
            [no.nsd.utils :as u]
            [clojure.tools.logging :as log]
            [no.nsd.datomic-generate-fn :as genfn]
            [no.nsd.rewriting-history.db-set-intersection-fn :as s])
  (:import (clojure.lang ExceptionInfo)))

(defn db-fn
  []
  (genfn/generate-function
    'no.nsd.rewriting-history.db-set-intersection-fn/set-intersection
    :set/intersection
    false))

(defn empty-conn
  ([]
   (u/empty-conn))
  ([schema]
   (u/empty-conn schema)))

(deftest verify-genfn-works
  (let [conn (empty-conn)]
    @(d/transact conn [(db-fn)])
    (is (= 1 1))))

(deftest write-fn
  (genfn/generate-function
    'no.nsd.rewriting-history.db-set-intersection-fn/set-intersection
    :set/intersection
    true)
  (is (= 1 1)))

(defn get-curr-set [conn]
  (->> (d/pull (d/db conn) '[:*] [:m/id "id"])
       :m/set
       (mapv (fn [v] (if (and
                           (map? v)
                           (= [:db/id] (vec (keys v))))
                       (d/pull (d/db conn) '[:*] (:db/id v))
                       v)))
       (mapv (fn [v] (if (map? v)
                       (dissoc v :db/id)
                       v)))
       (into #{})))

(deftest verify-primitives-work
  (with-redefs [s/rand-id (fn [] "randid")]
    (let [schema (reduce into []
                         [[(db-fn)]
                          #d/schema[[:m/id :one :string :id]
                                    [:m/set :many :string]]])
          conn (empty-conn schema)]

      (is (= (s/find-upsert-id (d/db conn) {:m/id  "id"
                                            :m/set #{"a" "b"}})
             [:m/id "id"]))
      (is (thrown? ExceptionInfo (s/find-upsert-id (d/db conn) {:m/set #{"a" "b"}})))

      (is (= (s/set-intersection conn {:m/id  "id"
                                       :m/set #{"a" "b"}})
             [{:m/id "id", :db/id "randid"}
              [:db/add "randid" :m/set "a"]
              [:db/add "randid" :m/set "b"]]))

      @(d/transact conn [{:m/id "id" :db/id "id"}
                         [:db/add "id" :m/set "a"]
                         [:db/add "id" :m/set "b"]])
      (is (= #{"a" "b"} (get-curr-set conn)))

      @(d/transact conn [[:set/intersection
                          {:m/id  "id"
                           :m/set #{"b" "c"}}]])
      (is (= #{"b" "c"} (get-curr-set conn)))

      @(d/transact conn [[:set/intersection
                          {:m/id  "id"
                           :m/set #{}}]])
      (is (= #{} (get-curr-set conn))))))

(deftest handles-multiple-attributes
  (with-redefs [s/rand-id (fn [] "randid")]
    (let [schema (reduce into []
                         [[(db-fn)]
                          #d/schema[[:m/id :one :string :id]
                                    [:m/s1 :many :string]
                                    [:m/s2 :many :string]]])
          conn (empty-conn schema)]

      (is (= (s/set-intersection conn {:m/id "id"
                                       :m/s1 #{"a" "b"}
                                       :m/s2 #{"c" "d"}})
             [{:m/id "id" :db/id "randid"}
              [:db/add "randid" :m/s1 "a"]
              [:db/add "randid" :m/s1 "b"]
              [:db/add "randid" :m/s2 "c"]
              [:db/add "randid" :m/s2 "d"]])))))

(deftest verify-component-refs-work
  (with-redefs [s/rand-id (let [c (atom 0)]
                            (fn [] (str "randid-" (swap! c inc))))]
    (let [schema (reduce into []
                         [[(db-fn)]
                          #d/schema[[:m/id :one :string :id]
                                    [:m/set :many :ref :component]
                                    [:c/a :one :string]]])
          conn (empty-conn schema)]
      (is (= (s/set-intersection
               (d/db conn)
               {:m/id "id"
                :m/set #{{:c/a "a"} {:c/a "b"}}})
             [{:m/id "id", :db/id "randid-1"}
              {:db/id "randid-1", :m/set {:c/a "a", :db/id "randid-2"}}
              {:db/id "randid-1", :m/set {:c/a "b", :db/id "randid-3"}}]))

      @(d/transact conn [[:set/intersection {:m/id "id"
                                             :m/set #{{:c/a "a"} {:c/a "b"}}}]])
      (is (= #{{:c/a "a"} {:c/a "b"}} (get-curr-set conn)))

      (let [b-eid (d/q
                    '[:find ?e .
                      :in $
                      :where
                      [?e :c/a "b"]]
                    (d/db conn))]
        @(d/transact conn [[:set/intersection {:m/id "id"
                                               :m/set #{{:c/a "b"} {:c/a "c"}}}]])
        (is (= #{{:c/a "b"} {:c/a "c"}} (get-curr-set conn)))
        (is (= b-eid
               (d/q
                 '[:find ?e .
                   :in $
                   :where
                   [?e :c/a "b"]]
                 (d/db conn))))))))

(deftest verify-refs-work
  (let [schema (reduce into []
                       [[(db-fn)]
                        #d/schema[[:m/id :one :string :id]
                                  [:m/set :many :ref]
                                  [:c/a :one :string]]])
        conn (empty-conn schema)]
    @(d/transact conn [[:set/intersection
                        {:m/id "id"
                         :m/set #{{:c/a "a"}
                                  {:c/a "b"}}}]])
    (is (= #{{:c/a "a"} {:c/a "b"}} (get-curr-set conn)))

    (let [b-eid (d/q
                  '[:find ?e .
                    :in $
                    :where
                    [?e :c/a "b"]]
                  (d/db conn))]
      @(d/transact conn [[:set/intersection {:m/id "id"
                                             :m/set #{{:c/a "b"} {:c/a "c"}}}]])
      (is (= #{{:c/a "b"} {:c/a "c"}} (get-curr-set conn)))
      (is (= b-eid
             (d/q
               '[:find ?e .
                 :in $
                 :where
                 [?e :c/a "b"]]
               (d/db conn)))))))
