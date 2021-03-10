(ns no.nsd.rewriting-history.db-set-intersection-fn
  (:require [datomic.api :as d]
            [clojure.set :as set]
            [clojure.pprint :as pprint]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk])
  (:import (java.util UUID HashSet List)
           (datomic Database)))

(defn to-clojure-types [m]
  (walk/prewalk
    (fn [e]
      (cond (instance? HashSet e)
            (into #{} e)

            (and (instance? List e) (not (vector? e)))
            (vec e)

            :else e))
    m))

(defn find-upsert-id [db m]
  (assert (map? m))
  (let [id (reduce-kv (fn [_ k v]
                        (when (= :db.unique/identity
                                 (d/q '[:find ?ident .
                                        :in $ ?id
                                        :where
                                        [?e :db/ident ?id]
                                        [?e :db/unique ?u]
                                        [?u :db/ident ?ident]]
                                      db
                                      k))
                          (reduced [k v])))
                      nil
                      m)]
    (if-not id
      (throw (ex-info "could not find :db.unique/identity" {:m m}))
      id)))

(defn rand-id []
  (str "id-" (UUID/randomUUID)))

(defn set-intersection-single [db dbid [id-k id-v] [a v]]
  (let [is-ref? (= :db.type/ref (d/q '[:find ?type .
                                       :in $ ?attr
                                       :where
                                       [?attr :db/valueType ?t]
                                       [?t :db/ident ?type]]
                                     db a))
        e (d/q '[:find ?e .
                 :in $ ?k ?v
                 :where
                 [?e ?k ?v]]
               db id-k id-v)]
    (if is-ref?
      (let [curr-set (when e
                       (into #{} (d/q '[:find [(pull ?v [*]) ...]
                                        :in $ ?e ?a
                                        :where
                                        [?e ?a ?v]]
                                      db e a)))
            curr-set-without-eid (into #{} (mapv (fn [ent]
                                                   (with-meta
                                                     (dissoc ent :db/id)
                                                     {:db/id (:db/id ent)}))
                                                 curr-set))
            to-remove (->> (set/difference curr-set-without-eid v)
                           (mapv (fn [e] (:db/id (meta e))))
                           (into #{}))
            to-add (set/difference v (set/intersection curr-set-without-eid v))
            tx (reduce
                 into
                 []
                 [(mapv (fn [rm] [:db/retract dbid a rm]) to-remove)
                  (mapv (fn [add] {:db/id dbid
                                   a (update add :db/id (fn [v] (or v (rand-id))))})
                        to-add)])]
        tx)
      (let [curr-set (when e
                       (into #{} (d/q '[:find [?v ...]
                                        :in $ ?e ?a
                                        :where
                                        [?e ?a ?v]]
                                      db e a)))
            to-remove (set/difference curr-set v)
            to-add (set/difference v (set/intersection curr-set v))
            tx (reduce
                 into
                 []
                 [(mapv (fn [rm] [:db/retract dbid a rm]) to-remove)
                  (mapv (fn [add] [:db/add dbid a add]) to-add)])]
        tx))))

(defn set-intersection
  [db m]
  (let [m (to-clojure-types m)
        db (if (instance? Database db) db (d/db db))]
    (assert (map? m) "expected m to be a map")
    (let [e (find-upsert-id db m)
          id (or (:db/id m) (rand-id))
          regular-map (->> m
                           (remove (comp set? second))
                           (into {}))
          sets (->> m
                    (filter (comp set? second))
                    (into []))
          tx (reduce into []
                     [[(assoc regular-map :db/id id)]
                      (vec (sort-by (fn [v]
                                      (if (map? v)
                                        (->> v
                                             (into (sorted-map))
                                             (into [])
                                             (pr-str))
                                        (pr-str v)))
                                    (mapcat (partial set-intersection-single db id e) sets)))])]
      tx)))

(def datomic-fn-def
  (clojure.edn/read-string
    {:readers {'db/id  datomic.db/id-literal
               'db/fn  datomic.function/construct
               'base64 datomic.codec/base-64-literal}}
    "{:db/ident :set/intersection\n :db/fn #db/fn \n{:lang \"clojure\", :requires [[datomic.api :as d] [clojure.set :as set]], :imports [(java.util UUID) (datomic Database)], :params [db m], :code (letfn [(find-upsert-id [db m] (do (assert (map? m)) (let [id (reduce-kv (fn [_ k v] (when (= :db.unique/identity (d/q (quote [:find ?ident . :in $ ?id :where [?e :db/ident ?id] [?e :db/unique ?u] [?u :db/ident ?ident]]) db k)) (reduced [k v]))) nil m)] (if-not id (throw (ex-info \"could not find :db.unique/identity\" {:m m})) id)))) (rand-id [] (do (str \"id-\" (UUID/randomUUID)))) (set-intersection-single [db dbid [id-k id-v] [a v]] (do (let [is-ref? (= :db.type/ref (d/q (quote [:find ?type . :in $ ?attr :where [?attr :db/valueType ?t] [?t :db/ident ?type]]) db a)) e (d/q (quote [:find ?e . :in $ ?k ?v :where [?e ?k ?v]]) db id-k id-v)] (if is-ref? (let [curr-set (when e (into #{} (d/q (quote [:find [(pull ?v [*]) ...] :in $ ?e ?a :where [?e ?a ?v]]) db e a))) curr-set-without-eid (into #{} (mapv (fn [ent] (with-meta (dissoc ent :db/id) #:db{:id (:db/id ent)})) curr-set)) to-remove (->> (set/difference curr-set-without-eid v) (mapv (fn [e] (:db/id (meta e)))) (into #{})) to-add (set/difference v (set/intersection curr-set-without-eid v)) tx (reduce into [] [(mapv (fn [rm] [:db/retract dbid a rm]) to-remove) (mapv (fn [add] {a (update add :db/id (fn [v] (or v (rand-id)))), :db/id dbid}) to-add)])] tx) (let [curr-set (when e (into #{} (d/q (quote [:find [?v ...] :in $ ?e ?a :where [?e ?a ?v]]) db e a))) to-remove (set/difference curr-set v) to-add (set/difference v (set/intersection curr-set v)) tx (reduce into [] [(mapv (fn [rm] [:db/retract dbid a rm]) to-remove) (mapv (fn [add] [:db/add dbid a add]) to-add)])] tx)))))] (do (let [db (if (instance? Database db) db (d/db db))] (assert (map? m) \"expected m to be a map\") (let [e (find-upsert-id db m) id (or (:db/id m) (rand-id)) regular-map (->> m (remove (comp set? second)) (into {})) sets (->> m (filter (comp set? second)) (into []))] (reduce into [] [[(assoc regular-map :db/id id)] (vec (sort-by (fn [v] (if (map? v) (->> v (into (sorted-map)) (into []) (pr-str)) (pr-str v))) (mapcat (partial set-intersection-single db id e) sets)))])))))}\n}"))

