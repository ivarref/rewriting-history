(ns no.nsd.rewriting-history.db-set-intersection-fn
  (:require [datomic.api :as d]
            [clojure.set :as set]
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

(defn set-intersection [db lookup-ref attr value]
  (let [value (to-clojure-types value)
        lookup-ref (to-clojure-types lookup-ref)
        _ (assert (vector? lookup-ref))
        _ (assert (set? value))
        db (if (instance? Database db) db (d/db db))
        dbid (rand-id)
        is-ref? (= :db.type/ref (d/q '[:find ?type .
                                       :in $ ?attr
                                       :where
                                       [?attr :db/valueType ?t]
                                       [?t :db/ident ?type]]
                                     db attr))
        [id-a id-v] lookup-ref
        e (d/q '[:find ?e .
                 :in $ ?a ?v
                 :where
                 [?e ?a ?v]]
               db id-a id-v)]
    (if is-ref?
      (let [curr-set (when e
                       (into #{} (d/q '[:find [(pull ?v [*]) ...]
                                        :in $ ?e ?a
                                        :where
                                        [?e ?a ?v]]
                                      db e attr)))
            curr-set-without-eid (into #{} (mapv (fn [ent]
                                                   (with-meta
                                                     (dissoc ent :db/id)
                                                     {:db/id (:db/id ent)}))
                                                 curr-set))
            to-remove (->> (set/difference curr-set-without-eid value)
                           (mapv (fn [e] (:db/id (meta e))))
                           (into #{}))
            to-add (->> (set/difference value (set/intersection curr-set-without-eid value))
                        (mapv (fn [e] (with-meta e {:tempid (rand-id)})))
                        (sort-by (fn [e] (pr-str (into (sorted-map) e)))))
            tx (vec (concat
                      [{id-a id-v :db/id dbid}]
                      (mapv (fn [rm] [:db/retract dbid attr rm]) to-remove)
                      (mapv (fn [add] (merge {:db/id (->> add (meta) :tempid)} add)) to-add)
                      (mapv (fn [add] [:db/add dbid attr (->> add (meta) :tempid)]) to-add)))]
        tx)
      (let [curr-set (when e
                       (into #{} (d/q '[:find [?v ...]
                                        :in $ ?e ?a
                                        :where
                                        [?e ?a ?v]]
                                      db e attr)))
            to-remove (set/difference curr-set value)
            to-add (set/difference value (set/intersection curr-set value))
            tx (vec (concat
                      [{id-a id-v :db/id dbid}]
                      (vec (sort (mapv (fn [rm] [:db/retract dbid attr rm]) to-remove)))
                      (vec (sort (mapv (fn [add] [:db/add dbid attr add]) to-add)))))]
        tx))))

(def datomic-fn-def
  (clojure.edn/read-string
    {:readers {'db/id  datomic.db/id-literal
               'db/fn  datomic.function/construct
               'base64 datomic.codec/base-64-literal}}
    "{:db/ident :set/intersection\n :db/fn #db/fn \n{:lang \"clojure\", :requires [[datomic.api :as d] [clojure.set :as set] [clojure.walk :as walk]], :imports [(java.util UUID HashSet List) (datomic Database)], :params [db lookup-ref attr value], :code (letfn [(to-clojure-types [m] (do (walk/prewalk (fn [e] (cond (instance? HashSet e) (into #{} e) (and (instance? List e) (not (vector? e))) (vec e) :else e)) m))) (find-upsert-id [db m] (do (assert (map? m)) (let [id (reduce-kv (fn [_ k v] (when (= :db.unique/identity (d/q (quote [:find ?ident . :in $ ?id :where [?e :db/ident ?id] [?e :db/unique ?u] [?u :db/ident ?ident]]) db k)) (reduced [k v]))) nil m)] (if-not id (throw (ex-info \"could not find :db.unique/identity\" {:m m})) id)))) (rand-id [] (do (str \"id-\" (UUID/randomUUID))))] (do (let [value (to-clojure-types value) lookup-ref (to-clojure-types lookup-ref) _ (assert (vector? lookup-ref)) _ (assert (set? value)) db (if (instance? Database db) db (d/db db)) dbid (rand-id) is-ref? (= :db.type/ref (d/q (quote [:find ?type . :in $ ?attr :where [?attr :db/valueType ?t] [?t :db/ident ?type]]) db attr)) [id-a id-v] lookup-ref e (d/q (quote [:find ?e . :in $ ?a ?v :where [?e ?a ?v]]) db id-a id-v)] (if is-ref? (let [curr-set (when e (into #{} (d/q (quote [:find [(pull ?v [*]) ...] :in $ ?e ?a :where [?e ?a ?v]]) db e attr))) curr-set-without-eid (into #{} (mapv (fn [ent] (with-meta (dissoc ent :db/id) #:db{:id (:db/id ent)})) curr-set)) to-remove (->> (set/difference curr-set-without-eid value) (mapv (fn [e] (:db/id (meta e)))) (into #{})) to-add (->> (set/difference value (set/intersection curr-set-without-eid value)) (mapv (fn [e] (with-meta e {:tempid (rand-id)}))) (sort-by (fn [e] (pr-str (into (sorted-map) e))))) tx (vec (concat [{id-a id-v, :db/id dbid}] (mapv (fn [rm] [:db/retract dbid attr rm]) to-remove) (mapv (fn [add] (merge #:db{:id (->> add (meta) :tempid)} add)) to-add) (mapv (fn [add] [:db/add dbid attr (->> add (meta) :tempid)]) to-add)))] tx) (let [curr-set (when e (into #{} (d/q (quote [:find [?v ...] :in $ ?e ?a :where [?e ?a ?v]]) db e attr))) to-remove (set/difference curr-set value) to-add (set/difference value (set/intersection curr-set value)) tx (vec (concat [{id-a id-v, :db/id dbid}] (vec (sort (mapv (fn [rm] [:db/retract dbid attr rm]) to-remove))) (vec (sort (mapv (fn [add] [:db/add dbid attr add]) to-add)))))] tx)))))}\n}"))

