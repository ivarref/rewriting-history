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
   (let [conn (empty-conn)]
     @(d/transact conn schema)
     conn)))

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

      (is (= (s/set-intersection conn [:m/id "id"] :m/set #{"a" "b"})
             [{:m/id "id", :db/id "randid"}
              [:db/add "randid" :m/set "a"]
              [:db/add "randid" :m/set "b"]]))

      @(d/transact conn [[:set/intersection [:m/id "id"] :m/set #{"a" "b"}]])
      (is (= #{"a" "b"} (get-curr-set conn)))

      (is (= (s/set-intersection (d/db conn) [:m/id "id"] :m/set #{"b" "c"})
             [{:m/id "id", :db/id "randid"}
              [:db/retract "randid" :m/set "a"]
              [:db/add "randid" :m/set "c"]]))

      @(d/transact conn [[:set/intersection [:m/id "id"] :m/set #{"b" "c"}]])

      (is (= #{"b" "c"} (get-curr-set conn)))

      @(d/transact conn [[:set/intersection [:m/id "id"] :m/set #{}]])
      (is (= #{} (get-curr-set conn))))))

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
               {:m/id  "id"
                :m/set #{{:c/a "a"} {:c/a "b"}}})
             [{:m/id "id", :db/id "randid-1"}
              {:db/id "randid-1", :m/set {:c/a "a", :db/id "randid-2"}}
              {:db/id "randid-1", :m/set {:c/a "b", :db/id "randid-3"}}]))

      @(d/transact conn [[:set/intersection {:m/id  "id"
                                             :m/set #{{:c/a "a"} {:c/a "b"}}}]])
      (is (= #{{:c/a "a"} {:c/a "b"}} (get-curr-set conn)))

      (let [b-eid (d/q
                    '[:find ?e .
                      :in $
                      :where
                      [?e :c/a "b"]]
                    (d/db conn))]
        @(d/transact conn [[:set/intersection {:m/id  "id"
                                               :m/set #{{:c/a "b"} {:c/a "c"}}}]])
        (is (= #{{:c/a "b"} {:c/a "c"}} (get-curr-set conn)))
        (is (= b-eid
               (d/q
                 '[:find ?e .
                   :in $
                   :where
                   [?e :c/a "b"]]
                 (d/db conn))))

        @(d/transact conn [[:set/intersection {:m/id  "id"
                                               :m/set #{}}]])
        (is (= #{} (get-curr-set conn)))))))

(deftest verify-refs-work
  (with-redefs [s/rand-id (let [c (atom 0)]
                            (fn [] (str "randid-" (swap! c inc))))]
    (let [schema (reduce into []
                         [[(db-fn)]
                          #d/schema[[:m/id :one :string :id]
                                    [:m/set :many :ref]
                                    [:c/id :one :string :id]]])
          conn (empty-conn schema)
          payload {:m/id  "id"
                   :m/set #{{:c/id "a"}
                            {:c/id "b"}}}]
      @(d/transact conn [[:set/intersection payload]])

      (is (= #{{:c/id "a"} {:c/id "b"}} (get-curr-set conn)))

      (let [b-eid (d/q
                    '[:find ?e .
                      :in $
                      :where
                      [?e :c/id "b"]]
                    (d/db conn))]
        @(d/transact conn [[:set/intersection {:m/id  "id"
                                               :m/set #{{:c/id "b"} {:c/id "c"}}}]])

        (is (= #{{:c/id "b"} {:c/id "c"}} (get-curr-set conn)))
        (is (= b-eid
               (d/q
                 '[:find ?e .
                   :in $
                   :where
                   [?e :c/id "b"]]
                 (d/db conn))))

        @(d/transact conn [[:set/intersection {:m/id  "id"
                                               :m/set #{}}]])
        (is (= #{} (get-curr-set conn)))))))

(deftest feav-style-is-cleaner
  (let [conn (u/empty-conn)]
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/desc :one :string]
                                [:m/set :many :string]])
    @(d/transact conn
                 [{:m/id   "id"
                   :m/desc "description"
                   :db/id  "jalla"}
                  {:m/id "id" :db/id "asdf"}
                  [:db/add "asdf" :m/set "a"]
                  [:db/add "asdf" :m/set "b"]])

    (is (= (-> (d/pull (d/db conn) '[:*] [:m/id "id"])
               (dissoc :db/id)
               (update :m/set (partial into #{})))
           #:m{:id "id" :desc "description" :set #{"a" "b"}}))

    @(d/transact conn
                 [{:m/id   "id"
                   :m/desc "description2"
                   :db/id  "jalla"}
                  {:m/id "id" :db/id "asdf"}
                  [:db/add "asdf" :m/set "c"]
                  [:db/retract "asdf" :m/set "a"]])

    (is (= (-> (d/pull (d/db conn) '[:*] [:m/id "id"])
               (dissoc :db/id)
               (update :m/set (partial into #{})))
           #:m{:id "id" :desc "description2" :set #{"b" "c"}}))))

(deftest verify-primitives-feav
  (let [conn-fn u/empty-stage-conn]
    (testing "using feav when other data is present"
      (with-redefs [s/rand-id (let [c (atom 0)]
                                (fn [] (str "randid-" (swap! c inc))))]
        (let [conn (conn-fn)]
          @(d/transact conn (reduce into []
                                    [[(db-fn)]
                                     #d/schema[[:m/id :one :string :id]
                                               [:m/desc :one :string]
                                               [:m/set :many :string]]]))
          @(d/transact conn [{:m/id   "id"
                              :m/desc "description"}
                             [:set/intersection [:m/id "id"] :m/set #{"a" "b"}]])

          (is (= (-> (d/pull (d/db conn) '[:*] [:m/id "id"])
                     (dissoc :db/id)
                     (update :m/set (partial into #{})))
                 #:m{:id "id" :desc "description" :set #{"a" "b"}}))

          @(d/transact conn [{:m/id   "id"
                              :m/desc "description2"}
                             [:set/intersection [:m/id "id"] :m/set #{"b" "c"}]])

          (is (= (-> (d/pull (d/db conn) '[:*] [:m/id "id"])
                     (dissoc :db/id)
                     (update :m/set (partial into #{})))
                 #:m{:id "id" :desc "description2" :set #{"b" "c"}})))))

    (testing "using feav when no other data is present"
      (with-redefs [s/rand-id (let [c (atom 0)]
                                (fn [] (str "randid-" (swap! c inc))))]
        (let [conn (conn-fn)]
          @(d/transact conn (reduce into []
                                    [[(db-fn)]
                                     #d/schema[[:m/id :one :string :id]
                                               [:m/set :many :string]]]))
          @(d/transact conn [[:set/intersection [:m/id "id"] :m/set #{"a" "b"}]])

          (is (= (-> (d/pull (d/db conn) '[:*] [:m/id "id"])
                     (dissoc :db/id)
                     (update :m/set (partial into #{})))
                 #:m{:id "id" :set #{"a" "b"}}))

          @(d/transact conn [[:set/intersection [:m/id "id"] :m/set #{"b" "c"}]])

          (is (= (-> (d/pull (d/db conn) '[:*] [:m/id "id"])
                     (dissoc :db/id)
                     (update :m/set (partial into #{})))
                 #:m{:id "id" :set #{"b" "c"}})))))))
