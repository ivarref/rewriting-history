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

(defn job-state [conn job-id]
  (d/q '[:find ?state .
         :in $ ?job-id
         :where
         [?e :rh/id ?job-id]
         [?e :rh/state ?state]]
       (d/db conn)
       job-id))

(defn job-init! [conn job-id]
  (let [eids-to-excise (d/q '[:find [?eid ...]
                              :in $ ?job-id
                              :where
                              [?e :rh/id ?job-id]
                              [?e :rh/eid ?eid]]
                            (d/db conn)
                            job-id)
        tx (->> (concat
                  [[:db/cas [:rh/id job-id] :rh/state :init :rewrite-history]]
                  (mapv (fn [eid] {:db/excise eid}) eids-to-excise))
                vec)]
    (log/debug "deleting initial eids:" eids-to-excise)
    (let [{:keys [db-after]} @(d/transact conn tx)]
      @(d/sync-excise conn (d/basis-t db-after)))))

(defn save-tempids-metadata [tx]
  (->> tx
       (map second)
       (filter string?)
       (map (fn [tempid] {:rh/tempid-str tempid
                          :rh/tempid-ref tempid}))
       (into #{})))

(defn rewrite-history! [conn job-id]
  (let [new-history (get-new-history conn job-id)
        txes (impl/history->transactions conn new-history)
        tx-index (d/q '[:find ?tx-index .
                        :in $ ?job-id
                        :where
                        [?e :rh/id ?job-id]
                        [?e :rh/tx-index ?tx-index]]
                      (d/db conn)
                      job-id)
        lookup-tempid (fn [[o [tempid tempid-str] a v]]
                        (assert (and (string? tempid-str) (= :tempid tempid)))
                        [o (d/q '[:find ?tempid-ref .
                                  :in $ ?job-id ?tempid-str
                                  :where
                                  [?e :rh/id ?job-id]
                                  [?e :rh/tempids ?tmpid]
                                  [?tmpid :rh/tempid-str ?tempid-str]
                                  [?tmpid :rh/tempid-ref ?tempid-ref]]
                                (d/db conn)
                                job-id
                                tempid-str)
                         a v])
        new-hist-tx (->> (nth txes tx-index)
                         (mapv (fn [[o e a v :as oeav]]
                                 (if (vector? e)
                                   (lookup-tempid oeav)
                                   oeav))))
        save-tempids (save-tempids-metadata new-hist-tx)
        tx-done? (= (inc tx-index) (count txes))
        tx (->> (concat [[:db/cas [:rh/id job-id] :rh/tx-index tx-index (inc tx-index)]
                         {:db/id [:rh/id job-id] :rh/tempids save-tempids}]
                        (when tx-done?
                          [[:db/cas [:rh/id job-id] :rh/state :rewrite-history :verify]
                           #_{:db/id [:rh/id job-id] :rh/done (Date.)}])
                        new-hist-tx)
                vec)]
    (log/debug "applying transaction" (inc tx-index) "of total" (count txes) "transactions ...")
    @(d/transact conn tx)))


(defn verify-history! [conn job-id]
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

(defn process-job-step! [conn job-id]
  (let [state (job-state conn "job")]
    (cond
      (= :init state)
      (job-init! conn job-id)

      (= :rewrite-history state)
      (rewrite-history! conn job-id)

      (= :verify state)
      (verify-history! conn job-id)

      :else
      (do
        (log/error "unhandled job state:" state)
        nil #_(throw (ex-info "unhandled job state" {:state state}))))))

(defn process-until-state [conn job-id desired-state]
  (loop []
    (process-job-step! conn job-id)
    (when (not= desired-state (job-state conn job-id))
      (recur))))

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
             :rh/lookup-ref  (pr-str (-> org-history meta :lookup-ref))
             :rh/org-history (history->set org-history)
             :rh/new-history (history->set new-history)}]]
    @(d/transact conn tx)))