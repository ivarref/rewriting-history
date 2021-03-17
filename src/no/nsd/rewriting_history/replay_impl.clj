(ns no.nsd.rewriting-history.replay-impl
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.schedule-impl :as schedule]
            [no.nsd.rewriting-history.init :as init]
            [no.nsd.rewriting-history.rewrite :as rewrite]
            [clojure.pprint :as pprint])
  (:import (java.util Date)))

(comment
  (history-take-tx
    [[1 :tx/txInstant #inst"1974-01-01T00:00:00.000-00:00" 1 true]
     [4 :m/id "id" 1 true]
     [4 :m/info "original-data" 1 true]
     [2 :tx/txInstant #inst"1975-01-01T00:00:00.000-00:00" 2 true]
     [4 :m/info "original-data" 2 false]
     [4 :m/info "bad-data" 2 true]
     [3 :tx/txInstant #inst"1976-01-01T00:00:00.000-00:00" 3 true]
     [4 :m/info "bad-data" 3 false]
     [4 :m/info "good-data" 3 true]]
    2))

(defn job-state [conn lookup-ref]
  (assert (vector? lookup-ref))
  (d/q '[:find ?state .
         :in $ ?lookup-ref
         :where
         [?e :rh/lookup-ref ?lookup-ref]
         [?e :rh/state ?state]]
       (d/db conn)
       (pr-str lookup-ref)))

(defn verify-history! [conn lookup-ref]
  (let [expected-history (impl/get-new-history conn lookup-ref)
        current-history (some->>
                          (impl/pull-flat-history-simple (d/db conn) lookup-ref)
                          (take (count expected-history))
                          (vec)
                          (impl/simplify-eavtos conn lookup-ref))
        ok-replay? (= expected-history current-history)
        db-id [:rh/lookup-ref (pr-str lookup-ref)]
        tx (if ok-replay?
             [[:db/cas db-id :rh/state :verify :done]
              {:db/id db-id :rh/done (Date.)}]
             [[:db/cas db-id :rh/state :verify :error]
              {:db/id db-id :rh/error (Date.)}])]
    (if ok-replay?
      (do
        @(d/transact conn tx))
      (do
        (log/error "replay of history for lookup ref" lookup-ref "got something wrong")
        (log/error "expected history:" expected-history)
        @(d/transact conn tx)))))

(defn process-job-step! [conn lookup-ref]
  (let [state (job-state conn lookup-ref)]
    (log/info "processing state" state "for lookup-ref" lookup-ref "...")
    (cond

      (= :scheduled state)
      (schedule/process-single-schedule! conn lookup-ref)

      (= :init state)
      (init/job-init! conn lookup-ref)

      (= :rewrite-history state)
      (rewrite/rewrite-history! conn lookup-ref)

      (= :verify state)
      (verify-history! conn lookup-ref)

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