(ns no.nsd.db-set-intersection-fn-test
  (:require [clojure.test :refer :all]
            [no.nsd.log-init]
            [no.nsd.shorter-stacktrace]
            [clojure.string :as str]
            [datomic-schema.core]
            [datomic.api :as d]
            [no.nsd.utils :as u]
            [clojure.tools.logging :as log]
            [no.nsd.datomic-generate-fn :as genfn]
            [no.nsd.rewriting-history.db-set-intersection-fn :as s]
            [clojure.pprint :as pprint])
  (:import (clojure.lang ExceptionInfo)))

(defn db-fn
  []
  (genfn/generate-function
    'no.nsd.rewriting-history.db-set-intersection-fn/set-intersection
    :set/intersection
    false))

(deftest verify-genfn-works
  (let [conn (u/empty-conn)]
    @(d/transact conn [(db-fn)])
    (is (= 1 1))))

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
  (u/clear)
  (let [schema (reduce into []
                       [[(db-fn)]
                        #d/schema[[:m/id :one :string :id]
                                  [:m/set :many :string]]])
        conn (u/empty-conn schema)]

    (is (= (s/find-upsert-id (d/db conn) {:m/id      "id"
                                          :m/set #{"a" "b"}})
           [:m/id "id"]))
    (is (thrown? ExceptionInfo (s/find-upsert-id (d/db conn) {:m/set #{"a" "b"}})))


    #_@(d/transact conn [[:set/intersection
                          {:m/id  "id"
                           :m/set #{"a" "b"}}]])
    #_(is (= #{"a" "b"} (get-curr-set conn)))))

;@(d/transact conn [[:set/intersection [:m/id "id"] :m/set #{"b" "c"}]])
;(is (= #{"b" "c"} (get-curr-set conn)))
;
;@(d/transact conn [[:set/intersection [:m/id "id"] :m/set #{}]])
;(is (= #{} (get-curr-set conn)))))

#_(deftest verify-component-refs-work
    (let [schema (reduce into []
                         [[(generate-function false)]
                          #d/schema[[:m/id :one :string :id]
                                    [:m/set :many :ref :component]
                                    [:c/a :one :string]]])
          conn (u/empty-conn, schema)]
      @(d/transact conn [{:db/id "id" :m/id "id"}
                         [:set/intersection "id" :m/set #{{:c/a "a"} {:c/a "b"}}]])
      (is (= #{{:c/a "a"} {:c/a "b"}} (get-curr-set conn)))

      (let [b-eid (d/q
                    '[:find ?e .
                      :in $
                      :where
                      [?e :c/a "b"]]
                    (d/db conn))]
        @(d/transact conn [[:set/intersection [:m/id "id"] :m/set #{{:c/a "b"} {:c/a "c"}}]])
        (is (= #{{:c/a "b"} {:c/a "c"}} (get-curr-set conn)))
        (is (= b-eid
               (d/q
                 '[:find ?e .
                   :in $
                   :where
                   [?e :c/a "b"]]
                 (d/db conn)))))))

#_(deftest verify-refs-work
    (let [schema (reduce into []
                         [[(generate-function false)]
                          #d/schema[[:m/id :one :string :id]
                                    [:m/set :many :ref]
                                    [:c/a :one :string]]])
          conn (u/empty-conn, schema)]
      @(d/transact conn [{:db/id "id" :m/id "id"}
                         [:set/intersection "id" :m/set #{{:c/a "a"} {:c/a "b"}}]])
      (is (= #{{:c/a "a"} {:c/a "b"}} (get-curr-set conn)))

      (let [b-eid (d/q
                    '[:find ?e .
                      :in $
                      :where
                      [?e :c/a "b"]]
                    (d/db conn))]
        @(d/transact conn [[:set/intersection [:m/id "id"] :m/set #{{:c/a "b"} {:c/a "c"}}]])
        (is (= #{{:c/a "b"} {:c/a "c"}} (get-curr-set conn)))
        (is (= b-eid
               (d/q
                 '[:find ?e .
                   :in $
                   :where
                   [?e :c/a "b"]]
                 (d/db conn)))))))

#_(deftest gen-fn
    (generate-function true)
    (is (= 1 1)))