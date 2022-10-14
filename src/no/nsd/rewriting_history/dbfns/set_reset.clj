(ns no.nsd.rewriting-history.dbfns.set-reset
  (:require [clojure.set :as set]
            [clojure.walk :as walk]
            [datomic.api :as d])
  (:import (datomic Database)
           (java.util HashSet List UUID)))

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

(defn set-reset [db lookup-ref attr values]
  (let [values (let [values (to-clojure-types values)]
                 (if (vector? values)
                   (into #{} values)
                   values))
        lookup-ref (to-clojure-types lookup-ref)
        _ (assert (vector? lookup-ref))
        _ (assert (set? values))
        db (if (instance? Database db) db (d/db db))
        dbid (rand-id)
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
                                     db attr))
        unique-type (d/q '[:find ?t .
                           :in $ ?attr
                           :where
                           [?e :db/ident ?attr]
                           [?e :db/unique ?type]
                           [?type :db/ident ?t]]
                         db (first lookup-ref))
        is-component? (and is-ref?
                           (true? (d/q '[:find ?comp .
                                         :in $ ?attr
                                         :where
                                         [?attr :db/isComponent ?comp]]
                                       db attr)))
        [id-a id-v] lookup-ref
        e (d/q '[:find ?e .
                 :in $ ?a ?v
                 :where
                 [?e ?a ?v]]
               db id-a id-v)]
    (doseq [v values]
      (when (and (map? v)
                 (some? (:db/id v))
                 (not (string? (:db/id v))))
        (throw (ex-info "expected :db/id to be a string or not present" {:element v}))))
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
            to-remove (->> (set/difference curr-set-without-eid values)
                           (mapv (fn [e] (:db/id (meta e))))
                           (into #{}))
            to-add (->> (set/difference values (set/intersection curr-set-without-eid values))
                        (mapv (fn [e] (with-meta e {:tempid
                                                    (or
                                                      (:db/id e)
                                                      (rand-id))})))
                        (sort-by (fn [e] (pr-str (into (sorted-map) e)))))
            tx (vec (concat
                      [{id-a id-v :db/id dbid}]
                      (mapv (fn [rm] (if is-component?
                                       [:db/retractEntity rm]
                                       [:db/retract dbid attr rm]))
                            to-remove)
                      (mapv (fn [add] (merge {:db/id (->> add (meta) :tempid)} add)) to-add)
                      (mapv (fn [add] [:db/add dbid attr (->> add (meta) :tempid)]) to-add)))]
        tx)
      (let [curr-set (when e
                       (into #{} (d/q '[:find [?v ...]
                                        :in $ ?e ?a
                                        :where
                                        [?e ?a ?v]]
                                      db e attr)))
            to-remove (set/difference curr-set values)
            to-add (set/difference values (set/intersection curr-set values))
            the-ref (if (and e (= :db.unique/value unique-type))
                      e
                      dbid)
            tx (filterv some?
                        (concat
                          (when (not= e the-ref)
                            [{id-a id-v :db/id dbid}])
                          (vec (sort (mapv (fn [rm] [:db/retract the-ref attr rm]) to-remove)))
                          (vec (sort (mapv (fn [add] [:db/add the-ref attr add]) to-add)))))]
        tx))))
