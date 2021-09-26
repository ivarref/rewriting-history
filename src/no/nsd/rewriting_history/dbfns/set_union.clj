(ns no.nsd.rewriting-history.dbfns.set-union
  (:require [clojure.walk :as walk]
            [datomic.api :as d]
            [clojure.set :as set])
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

(defn rand-id []
  (str "id-" (UUID/randomUUID)))

(defn set-union-ref [db [id-a id-v] attr values]
  (let [e (d/q '[:find ?e .
                 :in $ ?a ?v
                 :where
                 [?e ?a ?v]]
               db id-a id-v)
        curr-set (when e
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
        dbid (rand-id)
        to-add (->> (set/difference values curr-set-without-eid)
                    (mapv (fn [e] (with-meta e {:tempid
                                                (or
                                                  (:db/id e)
                                                  (rand-id))})))
                    (sort-by (fn [e] (pr-str (into (sorted-map) e)))))
        tx (vec (concat
                  [{id-a id-v :db/id dbid}]
                  (mapv (fn [add] (merge {:db/id (->> add (meta) :tempid)} add)) to-add)
                  (mapv (fn [add] [:db/add dbid attr (->> add (meta) :tempid)]) to-add)))]
    tx))

(defn set-union-primitives [db [id-a id-v] attr values]
  [{id-a id-v
    attr values}])

(defn set-union [db lookup-ref attr values]
  (let [values (to-clojure-types values)
        lookup-ref (to-clojure-types lookup-ref)
        _ (assert (vector? lookup-ref))
        _ (assert (set? values))
        _ (assert (keyword? attr))
        db (if (instance? Database db) db (d/db db))
        _ (assert (= :db.cardinality/many (d/q '[:find ?type .
                                                 :in $ ?attr
                                                 :where
                                                 [?attr :db/cardinality ?c]
                                                 [?c :db/ident ?type]]
                                               db attr))
                  (str "expected attribute to have cardinality :db.cardinality/many"))
        is-ref? (= :db.type/ref (d/q '[:find ?type .
                                       :in $ ?attr
                                       :where
                                       [?attr :db/valueType ?t]
                                       [?t :db/ident ?type]]
                                     db attr))]
    (when is-ref?
      (doseq [v values]
        (assert (map? v) "expected set element to be a map")))
    (doseq [v values]
      (when (and (map? v)
                 (some? (:db/id v))
                 (not (string? (:db/id v))))
        (throw (ex-info "expected :db/id to be a string or not present" {:element v}))))
    (if is-ref?
      (set-union-ref db lookup-ref attr values)
      (set-union-primitives db lookup-ref attr values))))
