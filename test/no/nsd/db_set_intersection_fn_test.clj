(ns no.nsd.db-set-intersection-fn-test
  (:require [clojure.test :refer :all]
            [rewrite-clj.zip :as z]
            [rewrite-clj.node :as n]
            [no.nsd.log-init]
            [no.nsd.shorter-stacktrace]
            [clojure.string :as str]
            [datomic-schema.core]
            [datomic.api :as d]
            [no.nsd.utils :as u]))

(defn generate-function [write-to-file]
  (let [fil (str "src/no/nsd/rewriting_history/db_set_intersection_fn.clj")
        reqs (-> (z/of-file fil)
                 (z/find-value z/next 'ns)
                 (z/find-value z/next :require)
                 (z/up)
                 (z/child-sexprs))
        ident (-> (z/of-file fil)
                  (z/find-value z/next 'defn)
                  (z/right)
                  (z/sexpr)
                  (str)
                  (str/replace "--" "/")
                  (keyword))
        params (-> (z/of-file fil)
                   (z/find-value z/next 'defn)
                   (z/right)
                   (z/right)
                   (z/sexpr))
        code (-> (z/of-file fil)
                 (z/find-value z/next 'defn)
                 (z/right)
                 (z/right)
                 (z/right)
                 (z/sexpr))
        out-str (str "{:db/ident " ident "\n"
                     " :db/fn #db/fn \n"
                     (pr-str {:lang     "clojure"
                              :params   params
                              :requires (vec (drop 1 reqs))
                              :code     code})
                     "\n"
                     "}")]
    (when write-to-file
      (spit fil (-> (z/of-file fil)
                    (z/find-value z/next 'clojure.edn/read-string)
                    (z/right)
                    (z/right)
                    (z/replace out-str)
                    (z/root)
                    (n/string))))
    (clojure.edn/read-string
      {:readers {'db/id  datomic.db/id-literal
                 'db/fn  datomic.function/construct
                 'base64 datomic.codec/base-64-literal}}
      out-str)))

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
  (let [schema (reduce into []
                       [[(generate-function false)]
                        #d/schema[[:m/id :one :string :id]
                                  [:m/set :many :string]]])
        conn (u/empty-conn schema)]
    @(d/transact conn [{:db/id "id" :m/id "id"}
                       [:set/intersection "id" :m/set #{"a" "b"}]])
    (is (= #{"a" "b"} (get-curr-set conn)))

    @(d/transact conn [[:set/intersection [:m/id "id"] :m/set #{"b" "c"}]])
    (is (= #{"b" "c"} (get-curr-set conn)))

    @(d/transact conn [[:set/intersection [:m/id "id"] :m/set #{}]])
    (is (= #{} (get-curr-set conn)))))

(deftest verify-component-refs-work
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

(deftest verify-refs-work
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

(deftest generate-function
  (generate-function true)
  (is (= 1 1)))