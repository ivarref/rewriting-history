(ns no.nsd.rewriting-history.init
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]))

(defn job-init! [conn lookup-ref]
  (assert (vector? lookup-ref))
  (let [eids-to-excise (d/q '[:find [?eid ...]
                              :in $ ?lookup-ref
                              :where
                              [?e :rh/lookup-ref ?lookup-ref]
                              [?e :rh/eid ?eid]]
                            (d/db conn)
                            (pr-str lookup-ref))
        tx (->> (concat
                  [[:db/cas [:rh/lookup-ref (pr-str lookup-ref)] :rh/state :init :rewrite-history]]
                  (mapv (fn [eid] {:db/excise eid}) eids-to-excise))
                vec)]
    (log/debug "deleting initial eids:" eids-to-excise)
    (let [{:keys [db-after]} @(d/transact conn tx)]
      @(d/sync-excise conn (d/basis-t db-after)))))