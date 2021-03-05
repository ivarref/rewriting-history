(ns no.nsd.rewriting-history.replay-impl
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [no.nsd.rewriting-history.impl :as impl])
  (:import (java.util Date)))

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

(defn get-new-history [conn job-id]
  (->> (d/q '[:find ?e ?a ?v ?t ?o
              :in $ ?ee
              :where
              [?ee :rh/new-history ?n]
              [?n :rh/e ?e]
              [?n :rh/a ?a]
              [?n :rh/v ?v]
              [?n :rh/t ?t]
              [?n :rh/o ?o]]
            (d/db conn)
            (impl/resolve-lookup-ref (d/db conn) [:rh/id job-id]))
       (vec)
       (mapv (partial mapv read-string))
       (sort-by (fn [[e a v t o]] [t e a o v]))
       (vec)))

(defn verify-history! [conn job-id]
  (log/info "verifying history ...")
  (let [lookup-ref (->> (d/q '[:find ?lookup-ref .
                               :in $ ?job-id
                               :where
                               [?e :rh/id ?job-id]
                               [?e :rh/lookup-ref ?lookup-ref]]
                             (d/db conn)
                             job-id)
                        (edn/read-string))
        expected-history (get-new-history conn job-id)
        current-history (impl/pull-flat-history-simple (d/db conn) lookup-ref)
        ok-replay? (= expected-history current-history)
        tx (if ok-replay?
             [[:db/cas [:rh/id job-id] :rh/state :verify :done]
              {:db/id [:rh/id job-id] :rh/done (Date.)}]
             [[:db/cas [:rh/id job-id] :rh/state :verify :error]
              {:db/id [:rh/id job-id] :rh/error (Date.)}])]
    (if ok-replay?
      (do
        @(d/transact conn tx))
      (do
        (log/error "replay of history for lookup ref" lookup-ref "got something wrong")
        (log/error "expected history:" expected-history)
        @(d/transact conn tx)))))

