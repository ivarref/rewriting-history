(ns no.nsd.rewriting-history.add-rewrite-job
  (:require [clojure.tools.logging :as log]
            [datomic.api :as d]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.dbfns.set-reset :as sr]))

(def chunk-size 100)

(defn put-chunks! [conn job-ref expected-state attr items]
  (log/debug "saving attribute" attr "for" job-ref "of size" (count items) "...")
  (let [chunks (partition-all chunk-size (into [] items))]
    (doseq [[idx chunk] (map-indexed vector chunks)]
      @(d/transact conn [[:cas/contains job-ref :rh/state #{expected-state} expected-state]
                         [:set/union job-ref attr (into #{} chunk)]])))
  #_(log/info "saving attribute" attr "for" job-ref "of size" (count items) "... OK!"))

(defn add-job! [conn lookup-ref from-state pending-state new-history]
  (assert (vector? lookup-ref))
  (assert (vector? new-history)
          (str "expected new-history to be vector, was: " (pr-str new-history)))
  (let [id (pr-str lookup-ref)
        job-ref [:rh/lookup-ref id]
        org-history (impl/pull-flat-history-simple conn lookup-ref)
        attrs [:rh/eid :rh/excised-eid :rh/org-history :rh/new-history :rh/replace :rh/tempids]]
    @(d/transact conn [[:cas/contains job-ref :rh/state (conj from-state pending-state) pending-state]])
    (doseq [attr attrs]
      (let [[fst & to-reset] (sr/set-reset conn job-ref attr #{})]
        (doseq [chunk (partition-all chunk-size to-reset)]
          @(d/transact conn (conj (vec chunk)
                                  [:cas/contains job-ref :rh/state #{pending-state} pending-state]
                                  fst))))
      (assert (= 1 (count (sr/set-reset conn job-ref attr #{})))))
    @(d/transact conn [[:cas/contains job-ref :rh/state #{pending-state} pending-state]
                       [:some/retract job-ref :rh/done]
                       [:some/retract job-ref :rh/error]
                       {:rh/lookup-ref id
                        :rh/tx-index   0}])
    (impl/log-state-change pending-state lookup-ref)
    (put-chunks! conn job-ref pending-state :rh/eid (into #{} (-> org-history meta :original-eids)))
    (put-chunks! conn job-ref pending-state :rh/org-history (impl/history->set org-history))
    (put-chunks! conn job-ref pending-state :rh/new-history (impl/history->set new-history))
    @(d/transact conn [[:db/cas job-ref :rh/state pending-state :init]])
    (impl/log-state-change :init lookup-ref)))
