(ns no.nsd.db-set-intersection-fn-test
  (:require [clojure.test :refer :all]
            [rewrite-clj.zip :as z]
            [rewrite-clj.node :as n]
            [no.nsd.log-init]
            [no.nsd.shorter-stacktrace]
            [clojure.string :as str]
            [datomic-schema.core]
            [datomic.api :as d]
            [no.nsd.utils :as u]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pprint]))

(defn fqn->fil [fqn]
  (str
    "src/"
    (-> fqn
        (str)
        (str/split #"/")
        (first)
        (str/replace "." "/")
        (str/replace "-" "_")
        (str ".clj"))))

(defn fqn->fn [fqn]
  (-> fqn
      (str)
      (str/split #"/")
      (last)
      (symbol)))

(defn find-fns [start-pos]
  (loop [pos start-pos
         fns []]
    (if-let [next-fn (some-> pos
                             (z/find-value z/next 'defn)
                             (z/up))]
      (recur (z/right next-fn)
             (conj fns (z/sexpr next-fn))),
      fns)))


(defn generate-function [fqn db-name write-to-file]
  (let [fil (fqn->fil fqn)
        reqs (-> (z/of-file fil)
                 (z/find-value z/next 'ns)
                 (z/find-value z/next :require)
                 (z/up)
                 (z/child-sexprs))
        impos (-> (z/of-file fil)
                  (z/find-value z/next 'ns)
                  (z/find-value z/next :import)
                  (z/up)
                  (z/child-sexprs))
        ident (fqn->fn fqn)
        fndef (-> (z/of-file fil)
                  (z/find-value z/next ident)
                  (z/up)
                  (z/sexpr))
        other-defns (->> (find-fns (z/of-file fil))
                         (remove #(= ident (second %)))
                         (mapv (fn [[_defn id args & body]]
                                 (list id args (apply list 'do body)))))
        params (some-> (z/of-file fil)
                       (z/find-value z/next ident)
                       (z/right)
                       (z/sexpr))
        code (some-> (z/of-file fil)
                     (z/find-value z/next ident)
                     (z/remove)
                     (z/remove)
                     (z/down)
                     (z/remove)
                     (z/edit (fn [n]
                               (list 'letfn other-defns
                                     (apply list 'do n))))
                     (z/sexpr))
        db-fn {:lang     "clojure"
               :requires (vec (drop 1 reqs))
               :imports (vec (drop 1 impos))
               :params   params
               :code     code}
        out-str (str "{:db/ident " db-name "\n"
                     " :db/fn #db/fn \n"
                     (pr-str db-fn)
                     "\n"
                     "}")]
    (clojure.edn/read-string
      {:readers {'db/id  datomic.db/id-literal
                 'db/fn  datomic.function/construct
                 'base64 datomic.codec/base-64-literal}}
      out-str)))

(comment
  (generate-function 'no.nsd.rewriting-history.db-set-intersection-fn/set-intersection false))
#_(defn get-curr-set [conn]
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
  (log/info "code is:\n"
            (with-out-str
              (pprint/pprint
                (generate-function
                  'no.nsd.rewriting-history.db-set-intersection-fn/set-intersection
                  :set/intersection
                  false))))
  #_(let [schema (reduce into []
                         [[(generate-function false)]
                          #d/schema[[:m/id :one :string :id]
                                    [:m/set :many :string]]])
          conn (u/empty-conn schema)]
      @(d/transact conn [[:set/intersection
                          {:m/id  "id"
                           :m/set #{"a" "b"}}]
                         #_[:set/intersection "id"]])
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