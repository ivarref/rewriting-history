(ns no.nsd.rewriting-history.schedule-init
  (:require [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]
            [clojure.edn :as edn]
            [clojure.string :as str]))

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

(defn process-single-schedule! [conn lookup-ref]
  (let [id (pr-str lookup-ref)
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
        tx [[:db/cas [:rh/lookup-ref id] :rh/state :scheduled :init]
            [:set/reset [:rh/lookup-ref id] :rh/eid (into #{} (-> org-history meta :original-eids))]
            [:set/reset [:rh/lookup-ref id] :rh/org-history (impl/history->set org-history)]
            [:set/reset [:rh/lookup-ref id] :rh/new-history (impl/history->set new-history)]
            [:set/reset [:rh/lookup-ref id] :rh/replace #{}]
            [:set/reset [:rh/lookup-ref id] :rh/tempids #{}]
            [:some/retract [:rh/lookup-ref id] :rh/done]
            [:some/retract [:rh/lookup-ref id] :rh/error]
            {:rh/lookup-ref id
             :rh/tx-index 0}]]
    @(d/transact conn tx)))