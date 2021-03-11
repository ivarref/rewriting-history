(ns no.nsd.rewriting-history.db-set-union-fn
  (:require [clojure.walk :as walk]
            [clojure.tools.logging :as log]
            [datomic.api :as d])
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

(defn set-union-ref [db lookup-ref attr values])

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
    (doseq [v values]
      (when (and (map? v)
                 (some? (:db/id v))
                 (not (string? (:db/id v))))
        (throw (ex-info "expected :db/id to be a string or not present" {:element v}))))
    (if is-ref?
      (set-union-ref db lookup-ref attr values)
      (set-union-primitives db lookup-ref attr values))))
