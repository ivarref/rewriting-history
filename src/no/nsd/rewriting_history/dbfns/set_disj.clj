(ns no.nsd.rewriting-history.dbfns.set-disj
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

(defn set-disj [db lookup-ref attr value]
  (let [value (to-clojure-types value)
        lookup-ref (to-clojure-types lookup-ref)
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
        db (if (instance? Database db) db (d/db db))
        [id-a id-v] lookup-ref
        e (d/q '[:find ?e .
                 :in $ ?a ?v
                 :where
                 [?e ?a ?v]]
               db id-a id-v)]
    (when e
      (let [curr-set (into #{} (d/q '[:find [(pull ?v [*]) ...]
                                      :in $ ?e ?a
                                      :where
                                      [?e ?a ?v]]
                                    db e attr))
            found-eid (reduce (fn [_ cand]
                                (when (= (dissoc cand :db/id) value)
                                  (reduced (:db/id cand))))
                              nil
                              curr-set)]
        (when found-eid
          [[:db/retract lookup-ref attr found-eid]])))))
