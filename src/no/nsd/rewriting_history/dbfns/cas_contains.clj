(ns no.nsd.rewriting-history.dbfns.cas-contains
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

(defn primitive? [v]
  (or (keyword? v)
      (number? v)
      (string? v)))

; [:cas/contains [:rh/lookup-ref id] :state #{:scheduled :done nil} :scheduled]

(defn cas-contains-inner [db [id-a id-v] attr coll key]
  (assert (keyword? attr))
  (assert (set? coll))
  (assert (some? key))
  (assert (primitive? key))
  (assert (not-empty coll)
          "expected coll to be non-empty")
  (assert (= :db.cardinality/one (d/q '[:find ?type .
                                        :in $ ?attr
                                        :where
                                        [?attr :db/cardinality ?c]
                                        [?c :db/ident ?type]]
                                      db attr))
          (str "expected attribute to have cardinality :db.cardinality/one"))
  (let [ok-types (sorted-set :db.type/keyword)]
    (assert (contains?
              ok-types
              (d/q '[:find ?type .
                     :in $ ?attr
                     :where
                     [?attr :db/valueType ?t]
                     [?t :db/ident ?type]]
                   db attr))
            (str "expected attribute to be of type " ok-types)))
  (let [curr-value (d/q '[:find ?curr-value .
                          :in $ ?a ?v ?attr
                          :where
                          [?e ?a ?v]
                          [?e ?attr ?curr-value]]
                        db id-a id-v attr)]
    (assert (contains? coll curr-value)
            (str "expected key " (pr-str curr-value)
                 " to be found in coll " (pr-str coll)))
    [{id-a id-v
      attr key}]))

(defn cas-contains [db lookup-ref attr coll key]
  (cas-contains-inner
    (if (instance? Database db) db (d/db db))
    (to-clojure-types lookup-ref)
    (to-clojure-types attr)
    (to-clojure-types coll)
    (to-clojure-types key)))
