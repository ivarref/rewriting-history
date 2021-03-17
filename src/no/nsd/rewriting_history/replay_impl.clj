(ns no.nsd.rewriting-history.replay-impl
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.init :as init]
            [no.nsd.rewriting-history.rewrite :as rewrite]
            [no.nsd.rewriting-history.verify :as verify]
            [no.nsd.rewriting-history.schedule-init :as schedule-init]
            [clojure.pprint :as pprint]))

(defn job-state [conn lookup-ref]
  (assert (vector? lookup-ref))
  (d/q '[:find ?state .
         :in $ ?lookup-ref
         :where
         [?e :rh/lookup-ref ?lookup-ref]
         [?e :rh/state ?state]]
       (d/db conn)
       (pr-str lookup-ref)))

(defn process-job-step! [conn lookup-ref]
  (let [state (job-state conn lookup-ref)]
    (log/info "processing state" state "for lookup-ref" lookup-ref "...")
    (cond

      (= :scheduled state)
      (schedule-init/process-single-schedule! conn lookup-ref)

      (= :init state)
      (init/job-init! conn lookup-ref)

      (= :rewrite-history state)
      (rewrite/rewrite-history! conn lookup-ref)

      (= :verify state)
      (verify/verify-history! conn lookup-ref)

      :else
      (do
        (log/error "unhandled job state:" state)
        nil #_(throw (ex-info "unhandled job state" {:state state}))))))

(defn process-until-state [conn lookup-ref desired-state]
  (loop []
    (process-job-step! conn lookup-ref)
    (let [new-state (job-state conn lookup-ref)]
      (cond (= new-state :error)
            :error

            (not= desired-state new-state)
            (recur)

            :else
            new-state))))

(defn process-state! [conn state]
  (when-let [lookup-ref (some-> (d/q '[:find ?lookup-ref .
                                       :in $ ?state
                                       :where
                                       [?e :rh/state ?state]
                                       [?e :rh/lookup-ref ?lookup-ref]]
                                     (d/db conn)
                                     state)
                                (edn/read-string))]
    (process-until-state conn lookup-ref :done)
    (recur conn state)))

(defn add-rewrite-job! [conn lookup-ref org-history new-history]
  (assert (vector? lookup-ref))
  (assert (vector? org-history))
  (assert (vector? new-history))
  (let [tx [{:rh/lookup-ref  (pr-str lookup-ref)
             :rh/state       :init
             :rh/tx-index    0
             :rh/eid         (into #{} (-> org-history meta :original-eids))
             :rh/org-history (impl/history->set org-history)
             :rh/new-history (impl/history->set new-history)}]]
    @(d/transact conn tx)))