(ns no.nsd.rewriting-history.dbfns.set-disj-if-empty
  (:require [datomic.api :as d]
            [clojure.walk :as walk])
  (:import (java.util HashSet List)
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

(defn set-disj-if-empty-fn [db lookup-ref attr value if-empty-tx if-some-tx]
  (let [value (to-clojure-types value)
        lookup-ref (to-clojure-types lookup-ref)
        if-empty-tx (to-clojure-types if-empty-tx)
        if-some-tx (to-clojure-types if-some-tx)
        _ (assert (vector? lookup-ref))
        _ (assert (map? value))
        _ (assert (keyword? attr))
        _ (assert (true?
                    (= :db.type/ref (d/q '[:find ?type .
                                           :in $ ?attr
                                           :where
                                           [?attr :db/valueType ?t]
                                           [?t :db/ident ?type]]
                                         db attr)))
                  (str "expected " attr " to be of valueType :db.type/ref"))

        _ (assert (= :db.cardinality/many (d/q '[:find ?type .
                                                 :in $ ?attr
                                                 :where
                                                 [?attr :db/cardinality ?c]
                                                 [?c :db/ident ?type]]
                                               db attr))
                  (str "expected attribute to have cardinality :db.cardinality/many"))

        db (if (instance? Database db) db (d/db db))
        [id-a id-v] lookup-ref
        e (d/q '[:find ?e .
                 :in $ ?a ?v
                 :where
                 [?e ?a ?v]]
               db id-a id-v)]
    (if-not e
      if-empty-tx
      (let [curr-set (into #{} (d/q '[:find [(pull ?v [*]) ...]
                                      :in $ ?e ?a
                                      :where
                                      [?e ?a ?v]]
                                    db e attr))
            found-eid (reduce (fn [_ cand]
                                (when (= (dissoc cand :db/id) value)
                                  (reduced (:db/id cand))))
                              nil
                              curr-set)
            is-empty? (or (and found-eid (= 1 (count curr-set)))
                          (empty? curr-set))]
        (reduce into []
                [(when found-eid [[:db/retract lookup-ref attr found-eid]])
                 (if is-empty? if-empty-tx if-some-tx)])))))

(defn set-disj-if-empty [lookup-ref attr value if-empty-tx if-some-tx]
  [:set/disj-if-empty lookup-ref attr value if-empty-tx if-some-tx])
