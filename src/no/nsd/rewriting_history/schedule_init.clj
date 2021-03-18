(ns no.nsd.rewriting-history.schedule-init
  (:require [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))

(defn maybe-replace [o {:keys [match replacement]}]
  (cond (and (string? o)
             (string? match)
             (str/includes? o match))
        (str/replace o match replacement)

        :else o))

(defn replace-eavto [replacements [e a v t o :as eavto]]
  [e
   a
   (reduce maybe-replace v replacements)
   t
   o])

(def chunk-size 10)

(defn put-chunks! [conn job-ref expected-state attr items]
  (log/info "saving attribute" attr "for" job-ref "of size" (count items) "...")
  (let [chunks (partition-all chunk-size (into [] items))]
    (doseq [[idx chunk] (map-indexed vector chunks)]
      #_(log/info "process chunk" (inc idx) "of" (count chunks))
      @(d/transact conn [[:cas/contains job-ref :rh/state #{expected-state} expected-state]
                         [:set/union job-ref attr (into #{} chunk)]])))
  #_(log/info "saving attribute" attr "for" job-ref "of size" (count items) "... OK!"))

(defn process-single-schedule! [conn lookup-ref]
  (let [id (pr-str lookup-ref)
        job-ref [:rh/lookup-ref id]
        replacements (->> (d/q '[:find ?r ?match ?replacement
                                 :in $ ?lookup-ref
                                 :where
                                 [?e :rh/lookup-ref ?lookup-ref]
                                 [?e :rh/replace ?r]
                                 [?r :rh/match ?match]
                                 [?r :rh/replacement ?replacement]]
                               (d/db conn)
                               id)
                          (mapv (fn [[eid m r]] [eid (edn/read-string m) (edn/read-string r)]))
                          (mapv (partial zipmap [:eid :match :replacement])))
        org-history (impl/pull-flat-history-simple conn lookup-ref)
        new-history (mapv (partial replace-eavto replacements) org-history)
        tx [[:db/cas job-ref :rh/state :scheduled :pending-init]
            [:set/reset job-ref :rh/eid #{}]
            [:set/reset job-ref :rh/org-history #{}]
            [:set/reset job-ref :rh/new-history #{}]
            [:set/reset job-ref :rh/replace #{}]
            [:set/reset job-ref :rh/tempids #{}]
            [:some/retract job-ref :rh/done]
            [:some/retract job-ref :rh/error]
            {:rh/lookup-ref id
             :rh/tx-index 0}]]
    (log/info "setting state to :pending-init and resetting state ...")
    @(d/transact conn tx)
    (put-chunks! conn job-ref :pending-init :rh/eid (into #{} (-> org-history meta :original-eids)))
    (put-chunks! conn job-ref :pending-init :rh/org-history (impl/history->set org-history))
    (put-chunks! conn job-ref :pending-init :rh/new-history (impl/history->set new-history))
    (log/info "saved chunks OK!")
    @(d/transact conn [[:db/cas job-ref :rh/state :pending-init :init]])
    (log/info "state is now :init for" lookup-ref)))