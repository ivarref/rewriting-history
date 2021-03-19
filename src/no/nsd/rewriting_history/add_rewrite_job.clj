(ns no.nsd.rewriting-history.add-rewrite-job
  (:require [clojure.tools.logging :as log]
            [datomic.api :as d]
            [no.nsd.rewriting-history.impl :as impl]))

(def chunk-size 10)

(defn put-chunks! [conn job-ref expected-state attr items]
  (log/info "saving attribute" attr "for" job-ref "of size" (count items) "...")
  (let [chunks (partition-all chunk-size (into [] items))]
    (doseq [[idx chunk] (map-indexed vector chunks)]
      #_(log/info "process chunk" (inc idx) "of" (count chunks))
      @(d/transact conn [[:cas/contains job-ref :rh/state #{expected-state} expected-state]
                         [:set/union job-ref attr (into #{} chunk)]])))
  #_(log/info "saving attribute" attr "for" job-ref "of size" (count items) "... OK!"))

(defn add-job! [conn lookup-ref from-state to-pending-state new-history]
  (assert (vector? lookup-ref))
  (assert (vector? new-history)
          (str "expected new-history to be vector, was: " (pr-str new-history)))
  (let [id (pr-str lookup-ref)
        job-ref [:rh/lookup-ref id]
        org-history (impl/pull-flat-history-simple conn lookup-ref)
        tx [[:db/cas job-ref :rh/state from-state to-pending-state]
            [:set/reset job-ref :rh/eid #{}]
            [:set/reset job-ref :rh/org-history #{}]
            [:set/reset job-ref :rh/new-history #{}]
            [:set/reset job-ref :rh/replace #{}]
            [:set/reset job-ref :rh/tempids #{}]
            [:some/retract job-ref :rh/done]
            [:some/retract job-ref :rh/error]
            {:rh/lookup-ref id
             :rh/tx-index 0}]]
    @(d/transact conn tx)
    (impl/log-state-change to-pending-state lookup-ref)
    (put-chunks! conn job-ref :pending-init :rh/eid (into #{} (-> org-history meta :original-eids)))
    (put-chunks! conn job-ref :pending-init :rh/org-history (impl/history->set org-history))
    (put-chunks! conn job-ref :pending-init :rh/new-history (impl/history->set new-history))
    @(d/transact conn [[:db/cas job-ref :rh/state to-pending-state :init]])
    (impl/log-state-change :init lookup-ref)))
