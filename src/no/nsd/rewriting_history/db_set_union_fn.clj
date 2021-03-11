(ns no.nsd.rewriting-history.db-set-union-fn
  (:require [clojure.walk :as walk])
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

(defn set-union [db lookup-ref attr values])