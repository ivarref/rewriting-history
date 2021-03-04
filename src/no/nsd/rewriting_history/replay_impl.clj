(ns no.nsd.rewriting-history.replay-impl
  (:require [datomic.api :as d]))

(defn history->set [hist]
  (->> hist
       (map #(mapv pr-str %))
       (mapv (partial zipmap [:rh/e :rh/a :rh/v :rh/t :rh/o]))
       (into #{})))

(defn add-rewrite-job! [conn job-id org-history new-history]
  (assert (string? job-id))
  (assert (vector? org-history))
  (assert (vector? new-history))
  (let [tx [{:rh/id          job-id
             :rh/state       :init
             :rh/tx-index    0
             :rh/eid         (into #{} (-> org-history meta :original-eids))
             :rh/org-history (history->set org-history)
             :rh/new-history (history->set new-history)}]]
    @(d/transact conn tx)))