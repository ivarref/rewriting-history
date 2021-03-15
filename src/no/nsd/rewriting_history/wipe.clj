(ns no.nsd.rewriting-history.wipe
  (:require [no.nsd.rewriting-history.impl :as impl])
  (:import (datomic Connection)))

(defn wipe-rewrite-job! [conn lookup-ref]
  (assert (vector? lookup-ref))
  (assert (instance? Connection conn))
  (let [history (impl/pull-flat-history conn [:rh/lookup-ref (pr-str lookup-ref)])]
    (->> history
         (filter (fn [[_e a _v _t _o]] (= "rh" (namespace a))))
         (map first)
         (into #{}))))

