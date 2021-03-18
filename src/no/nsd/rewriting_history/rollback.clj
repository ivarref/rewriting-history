(ns no.nsd.rewriting-history.rollback
  (:require [datomic.api :as d]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.schedule-init :as schedule-init]
            [clojure.tools.logging :as log])
  (:import (datomic Connection)))


(defn rollback! [conn lookup-ref t]
  (assert (instance? Connection conn))
  (assert (some? (impl/resolve-lookup-ref conn [:rh/lookup-ref (pr-str lookup-ref)])))
  (assert (pos-int? t))
  (log/info "checking t ...")
  (let [lookup-ref-str (pr-str lookup-ref)
        job-ref [:rh/lookup-ref lookup-ref-str]]
    (assert (= t (d/q '[:find ?t .
                        :in $ ?lookup-ref ?t
                        :where
                        [?e :rh/lookup-ref ?lookup-ref]
                        [?e :rh/state :init ?t true]]
                      (impl/history conn)
                      lookup-ref-str
                      t))
            "Expected to find t in history with :rh/state set to :init")
    (log/info "checking t ... OK")
    (let [history (impl/pull-flat-history-simple (impl/as-of conn t) lookup-ref)]
      (log/info "setting state to :pending-rollback and resetting state ...")
      @(d/transact conn [[:db/cas job-ref :rh/state :ok-rollback :pending-rollback]
                         [:set/reset job-ref :rh/eid #{}]
                         [:set/reset job-ref :rh/org-history #{}]
                         [:set/reset job-ref :rh/new-history #{}]
                         [:set/reset job-ref :rh/replace #{}]
                         [:set/reset job-ref :rh/tempids #{}]
                         [:some/retract job-ref :rh/done]
                         [:some/retract job-ref :rh/error]
                         {:rh/lookup-ref lookup-ref-str
                          :rh/tx-index   0}])
      (impl/log-state-change :pending-rollback lookup-ref)
      (schedule-init/put-chunks! conn job-ref :pending-rollback :rh/eid (into #{} (-> history meta :original-eids)))
      (schedule-init/put-chunks! conn job-ref :pending-rollback :rh/org-history (impl/history->set history))
      (schedule-init/put-chunks! conn job-ref :pending-rollback :rh/new-history (impl/history->set history))
      (log/info "saved chunks OK!")
      @(d/transact conn [[:db/cas job-ref :rh/state :pending-rollback :init]])
      (impl/log-state-change :init lookup-ref))))








