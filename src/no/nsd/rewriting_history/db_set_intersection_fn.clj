(ns no.nsd.rewriting-history.db-set-intersection-fn
  (:require [datomic.api :as d]
            [clojure.set :as set])
  (:import (java.util UUID)
           (datomic Database)))

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
  (let [db (if (instance? Database db) db (d/db db))]
    (assert (map? m) "expected m to be a map")
    (let [e (find-upsert-id db m)
          id (or (:db/id m) (rand-id))
          regular-map (->> m
                           (remove (comp set? second))
                           (into {}))
          sets (->> m
                    (filter (comp set? second))
                    (into []))]
      (reduce into []
              [[(assoc regular-map :db/id id)]
               (vec (sort-by (fn [v]
                               (if (map? v)
                                 (->> v
                                      (into (sorted-map))
                                      (into [])
                                      (pr-str))
                                 (pr-str v)))
                             (mapcat (partial set-intersection-single db id e) sets)))]))))

(def datomic-fn-def
  (clojure.edn/read-string
    {:readers {'db/id  datomic.db/id-literal
               'db/fn  datomic.function/construct
               'base64 datomic.codec/base-64-literal}}
    "{:db/ident :set/intersection\n :db/fn #db/fn \n{:lang \"clojure\", :params [db lookup-ref-or-eid a new-set], :requires [[datomic.api :as d] [clojure.set :as set] [clojure.pprint :as pprint]], :code (let [is-ref? (= :db.type/ref (d/q (quote [:find ?type . :in $ ?attr :where [?attr :db/valueType ?t] [?t :db/ident ?type]]) db a)) e (cond (string? lookup-ref-or-eid) lookup-ref-or-eid (vector? lookup-ref-or-eid) (let [[id v] lookup-ref-or-eid] (d/q (quote [:find ?e . :in $ ?id ?v :where [?e ?id ?v]]) db id v)) :else lookup-ref-or-eid)] (cond (string? e) (let [retval (mapv (fn [set-entry] {a (if (map? set-entry) (update set-entry :db/id (fn [v] (or v (str \"a\" (java.util.UUID/randomUUID))))) set-entry), :db/id e}) new-set)] retval) (false? is-ref?) (let [curr-set (into #{} (d/q (quote [:find [?v ...] :in $ ?e ?a :where [?e ?a ?v]]) db e a)) to-remove (set/difference curr-set new-set) to-add (set/difference new-set (set/intersection curr-set new-set)) tx (reduce into [] [(mapv (fn [rm] [:db/retract e a rm]) to-remove) (mapv (fn [add] [:db/add e a add]) to-add)])] tx) :else (let [curr-set (into #{} (d/q (quote [:find [(pull ?v [*]) ...] :in $ ?e ?a :where [?e ?a ?v]]) db e a)) curr-set-without-eid (into #{} (mapv (fn [ent] (with-meta (dissoc ent :db/id) #:db{:id (:db/id ent)})) curr-set)) to-remove (->> (set/difference curr-set-without-eid new-set) (mapv (fn [e] (:db/id (meta e)))) (into #{})) to-add (set/difference new-set (set/intersection curr-set-without-eid new-set)) tx (reduce into [] [(mapv (fn [rm] [:db/retract e a rm]) to-remove) (mapv (fn [add] {a (update add :db/id (fn [v] (or v (str \"a\" (java.util.UUID/randomUUID))))), :db/id e}) to-add)])] tx)))}\n}"))

