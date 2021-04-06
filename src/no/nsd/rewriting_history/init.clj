(ns no.nsd.rewriting-history.init
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]
            [no.nsd.rewriting-history.impl :as impl]
            [clojure.set :as set]))

(defn excise-op [eid]
  {:db/excise eid})

(def chunk-size 10)

(defn job-init! [conn lookup-ref]
  (assert (vector? lookup-ref))
  (let [eids-to-excise (->> (d/q '[:find [?eid ...]
                                   :in $ ?lookup-ref
                                   :where
                                   [?e :rh/lookup-ref ?lookup-ref]
                                   [?e :rh/eid ?eid]]
                                 (d/db conn)
                                 (pr-str lookup-ref))
                            (into #{}))
        already-excised (->> (d/q '[:find [?eid ...]
                                    :in $ ?lookup-ref
                                    :where
                                    [?e :rh/lookup-ref ?lookup-ref]
                                    [?e :rh/excised-eid ?eid]]
                                  (d/db conn)
                                  (pr-str lookup-ref))
                             (into #{}))
        job-ref [:rh/lookup-ref (pr-str lookup-ref)]]
    (let [chunks (partition-all chunk-size (into [] (set/difference eids-to-excise already-excised)))]
      (doseq [[idx chunk] (map-indexed vector chunks)]
        (let [{:keys [db-after]} @(d/transact conn (reduce into []
                                                           [(mapv excise-op chunk)
                                                            [[:cas/contains job-ref :rh/state #{:init} :init]]
                                                            [[:set/union job-ref :rh/excised-eid (into #{} chunk)]]]))]
          @(d/sync-excise conn (d/basis-t db-after)))))
    @(d/transact conn [[:db/cas [:rh/lookup-ref (pr-str lookup-ref)] :rh/state :init :rewrite-history]])
    (impl/log-state-change :rewrite-history lookup-ref)))
