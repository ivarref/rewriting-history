(ns no.nsd.rewriting-history.wipe
  (:require [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]
            [clojure.tools.logging :as log])
  (:import (datomic Connection)))

(defn wipe-rewrite-job! [conn lookup-ref]
  (assert (vector? lookup-ref))
  (assert (instance? Connection conn))
  (when (impl/resolve-lookup-ref conn [:rh/lookup-ref (pr-str lookup-ref)])
    (let [history (impl/pull-flat-history conn [:rh/lookup-ref (pr-str lookup-ref)])
          eids (->> history
                    (filter (fn [[_e a _v _t _o]] (= "rh" (namespace a))))
                    (map first)
                    (into #{}))
          tx (mapv (fn [eid] {:db/excise eid})
                   eids)]
      (log/debug "excising" (count tx) "entities ...")
      (let [{:keys [db-after]} @(d/transact conn tx)]
        @(d/sync-excise conn (d/basis-t db-after))))))
