(ns no.nsd.dbfns.db-set-reset-fn-test
  (:require [clojure.test :refer [deftest is testing]]
            [datomic-schema.core]
            [datomic.api :as d]
            [no.nsd.dbfns.datomic-generate-fn :as genfn]
            [no.nsd.log-init]
            [no.nsd.rewriting-history.dbfns.set-reset :as s]
            [no.nsd.shorter-stacktrace]
            [no.nsd.utils :as u])
  (:import (java.util.concurrent ExecutionException)))

(defn db-fn
  []
  (genfn/generate-function
    'no.nsd.rewriting-history.dbfns.set-reset/set-reset
    :set/reset
    false))

(def empty-conn u/empty-conn)

(deftest verify-genfn-works
  (let [conn (empty-conn)]
    @(d/transact conn [(db-fn)])
    (is (= 1 1))))

(deftest write-fn
  (genfn/generate-function
    'no.nsd.rewriting-history.dbfns.set-reset/set-reset
    :set/reset
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

(deftest feav-style-is-cleaner
  (testing "verify database behaviour and show how we would like our transactions to look like"
    (let [conn (empty-conn)]
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
             #:m{:id "id" :desc "description2" :set #{"b" "c"}})))))

(deftest verify-primitives-work
  (with-redefs [s/rand-id (fn [] "randid")]
    (let [schema (reduce into []
                         [[(db-fn)]
                          #d/schema[[:m/id :one :string :id]
                                    [:m/set :many :string]]])
          conn (empty-conn)]
      @(d/transact conn schema)

      (is (= (s/set-reset conn [:m/id "id"] :m/set #{"a" "b"})
             [{:m/id "id", :db/id "randid"}
              [:db/add "randid" :m/set "a"]
              [:db/add "randid" :m/set "b"]]))

      @(d/transact conn [[:set/reset [:m/id "id"] :m/set #{"a" "b"}]])
      (is (= #{"a" "b"} (get-curr-set conn)))

      (is (= (s/set-reset (d/db conn) [:m/id "id"] :m/set #{"b" "c"})
             [{:m/id "id", :db/id "randid"}
              [:db/retract "randid" :m/set "a"]
              [:db/add "randid" :m/set "c"]]))

      @(d/transact conn [[:set/reset [:m/id "id"] :m/set #{"b" "c"}]])

      (is (= #{"b" "c"} (get-curr-set conn)))

      @(d/transact conn [[:set/reset [:m/id "id"] :m/set #{}]])
      (is (= #{} (get-curr-set conn))))))

(deftest verify-can-use-vector
  (with-redefs [s/rand-id (fn [] "randid")]
    (let [schema (reduce into []
                         [[(db-fn)]
                          #d/schema[[:m/id :one :string :id]
                                    [:m/set :many :string]]])
          conn (empty-conn)]
      @(d/transact conn schema)

      (is (= (s/set-reset conn [:m/id "id"] :m/set ["a" "b"])
             [{:m/id "id", :db/id "randid"}
              [:db/add "randid" :m/set "a"]
              [:db/add "randid" :m/set "b"]]))

      @(d/transact conn [[:set/reset [:m/id "id"] :m/set ["a" "b"]]])
      (is (= #{"a" "b"} (get-curr-set conn)))

      @(d/transact conn [[:set/reset [:m/id "id"] :m/set []]])
      (is (= #{} (get-curr-set conn))))))


(deftest verify-can-use-unique-id-values
  (let [schema (reduce into []
                       [[(db-fn)]
                        #d/schema[[:m/id :one :string :unique]
                                  [:m/set :many :string]]])
        conn (empty-conn)]
    @(d/transact conn schema)

    @(d/transact conn [{:m/id "id"}])

    @(d/transact conn [[:set/reset [:m/id "id"] :m/set ["a" "b"]]])
    (is (= #{"a" "b"} (get-curr-set conn)))

    @(d/transact conn [[:set/reset [:m/id "id"] :m/set []]])
    (is (= #{} (get-curr-set conn)))))

(deftest verify-component-refs-work
  (with-redefs [s/rand-id (let [c (atom 0)]
                            (fn [] (str "randid-" (swap! c inc))))]
    (let [schema (reduce into []
                         [[(db-fn)]
                          #d/schema[[:m/id :one :string :id]
                                    [:m/set :many :ref :component]
                                    [:c/a :one :string]]])
          conn (empty-conn)]
      @(d/transact conn schema)

      @(d/transact conn [[:set/reset [:m/id "id"] :m/set #{{:c/a "a"} {:c/a "b"}}]])
      (is (= #{{:c/a "a"} {:c/a "b"}} (get-curr-set conn)))

      (let [a-eid (d/q
                    '[:find ?e .
                      :in $
                      :where
                      [?e :c/a "a"]]
                    (d/db conn))
            b-eid (d/q
                    '[:find ?e .
                      :in $
                      :where
                      [?e :c/a "b"]]
                    (d/db conn))]
        (is (pos-int? a-eid))
        @(d/transact conn [[:set/reset [:m/id "id"] :m/set #{{:c/a "b"} {:c/a "c"}}]])

        ; Asserting identical set is a no-op:
        (is (= (s/set-reset conn [:m/id "id"] :m/set #{{:c/a "b"} {:c/a "c"}})
               [{:m/id "id", :db/id "randid-1"}]))

        (is (= #{{:c/a "b"} {:c/a "c"}} (get-curr-set conn)))
        (is (= b-eid
               (d/q
                 '[:find ?e .
                   :in $
                   :where
                   [?e :c/a "b"]]
                 (d/db conn))))
        (is (nil? (d/q
                    '[:find ?e .
                      :in $
                      :where
                      [?e :c/a "a"]]
                    (d/db conn))))

        @(d/transact conn [[:set/reset [:m/id "id"] :m/set #{}]])
        (is (= #{} (get-curr-set conn)))))))

(deftest verify-refs-work
  (with-redefs [s/rand-id (let [c (atom 0)]
                            (fn [] (str "randid-" (swap! c inc))))]
    (let [schema (reduce into []
                         [[(db-fn)]
                          #d/schema[[:m/id :one :string :id]
                                    [:m/set :many :ref]
                                    [:c/id :one :string :id]]])
          conn (empty-conn)]
      @(d/transact conn schema)
      (is (= (s/set-reset conn [:m/id "id"] :m/set #{{:c/id "b"} {:c/id "c"}})
             [{:m/id "id", :db/id "randid-1"}
              {:db/id "randid-2", :c/id "b"}
              {:db/id "randid-3", :c/id "c"}
              [:db/add "randid-1" :m/set "randid-2"]
              [:db/add "randid-1" :m/set "randid-3"]]))

      @(d/transact conn [[:set/reset [:m/id "id"] :m/set #{{:c/id "a"} {:c/id "b"}}]])

      (is (= #{{:c/id "a"} {:c/id "b"}} (get-curr-set conn)))

      (let [b-eid (d/q
                    '[:find ?e .
                      :in $
                      :where
                      [?e :c/id "b"]]
                    (d/db conn))
            tx (s/set-reset conn [:m/id "id"] :m/set #{{:c/id "b"} {:c/id "c"}})]
        (is (= tx
               [{:m/id "id", :db/id "randid-4"}
                [:db/retract "randid-4" :m/set (d/q
                                                 '[:find ?e .
                                                   :in $
                                                   :where
                                                   [?e :c/id "a"]]
                                                 (d/db conn))]
                {:db/id "randid-5", :c/id "c"}
                [:db/add "randid-4" :m/set "randid-5"]]))
        @(d/transact conn [[:set/reset [:m/id "id"] :m/set #{{:c/id "b"} {:c/id "c"}}]])

        ; Asserting identical set is a no-op:
        (is (= (s/set-reset conn [:m/id "id"] :m/set #{{:c/id "b"} {:c/id "c"}})
               [{:m/id "id", :db/id "randid-6"}]))

        (is (= #{{:c/id "b"} {:c/id "c"}} (get-curr-set conn)))
        (is (= b-eid
               (d/q
                 '[:find ?e .
                   :in $
                   :where
                   [?e :c/id "b"]]
                 (d/db conn))))
        (is (some? (d/q
                     '[:find ?e .
                       :in $
                       :where
                       [?e :c/id "a"]]
                     (d/db conn))))

        @(d/transact conn [[:set/reset [:m/id "id"] :m/set #{}]])
        (is (= #{} (get-curr-set conn)))))))

(deftest strange-db-id-gives-exception
  (with-redefs [s/rand-id (let [c (atom 0)]
                            (fn [] (str "randid-" (swap! c inc))))]
    (let [conn (empty-conn)]
      @(d/transact conn (reduce into []
                                [[(db-fn)]
                                 #d/schema[[:m/id :one :string :id]
                                           [:m/set :many :ref :component]
                                           [:c/a :one :string]]]))
      (is (thrown? ExecutionException @(d/transact conn [[:set/reset [:m/id "id"]
                                                          :m/set #{{:c/a   "a"
                                                                    :db/id [:m/id "parent"]}}]])))
      (is (= (s/set-reset conn [:m/id "id"]
                          :m/set #{{:c/a   "a"
                                    :db/id "customstr"}})
             [{:m/id "id", :db/id "randid-1"}
              {:db/id "customstr", :c/a "a"}
              [:db/add "randid-1" :m/set "customstr"]]))
      @(d/transact conn [[:set/reset [:m/id "id"]
                          :m/set #{{:c/a   "a"
                                    :db/id "customstr"}}]])
      (is (= #{{:c/a "a"}} (get-curr-set conn))))))

(deftest verify-set-reset-when-other-data-is-present
  (with-redefs [s/rand-id (let [c (atom 0)]
                            (fn [] (str "randid-" (swap! c inc))))]
    (let [conn (empty-conn)]
      @(d/transact conn (reduce into []
                                [[(db-fn)]
                                 #d/schema[[:m/id :one :string :id]
                                           [:m/desc :one :string]
                                           [:m/set :many :string]]]))
      @(d/transact conn [{:m/id   "id"
                          :m/desc "description"}
                         [:set/reset [:m/id "id"] :m/set #{"a" "b"}]])

      (is (= (-> (d/pull (d/db conn) '[:*] [:m/id "id"])
                 (dissoc :db/id)
                 (update :m/set (partial into #{})))
             #:m{:id "id" :desc "description" :set #{"a" "b"}}))

      @(d/transact conn [{:m/id   "id"
                          :m/desc "description2"}
                         [:set/reset [:m/id "id"] :m/set #{"b" "c"}]])

      (is (= (-> (d/pull (d/db conn) '[:*] [:m/id "id"])
                 (dissoc :db/id)
                 (update :m/set (partial into #{})))
             #:m{:id "id" :desc "description2" :set #{"b" "c"}})))))
