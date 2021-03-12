(ns no.nsd.rewriting-history.dbfns.some-retract
  (:require [clojure.walk :as walk]
            [datomic.api :as d])
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

(defn some-retract [db lookup-ref attr]
  (let [db (if (instance? Database db) db (d/db db))
        lookup-ref (to-clojure-types lookup-ref)
        _ (assert (vector? lookup-ref))
        _ (assert (keyword? attr))
        [id-a id-v] lookup-ref]
    (when-let [e (d/q '[:find ?e .
                        :in $ ?a ?v
                        :where
                        [?e ?a ?v]]
                      db id-a id-v)]
      (let [v (d/q '[:find ?v .
                     :in $ ?e ?a
                     :where
                     [?e ?a ?v]]
                   db e attr)]
        (when (some? v)
          [[:db/retract lookup-ref attr v]])))))
