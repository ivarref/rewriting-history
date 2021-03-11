(ns no.nsd.rewriting-history.schedule-impl
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay]
            [clojure.edn :as edn]
            [clojure.pprint :as pprint]
            [clojure.string :as str]))

(defn schedule-replacement! [conn lookup-ref match replacement]
  (when (some? (d/q '[:find ?error .
                      :in $ ?lookup-ref
                      :where
                      [?e :rh/lookup-ref ?lookup-ref]
                      [?e :rh/error ?error]]
                    (d/db conn)
                    (pr-str lookup-ref)))
    (log/error "cannot schedule replacement on entity that has failed!")
    (throw (ex-info "cannot schedule replacement on entity that has failed!"
                    {:lookup-ref lookup-ref})))
  (let [id (pr-str lookup-ref)
        replace {:rh/match       (pr-str match)
                 :rh/replacement (pr-str replacement)}]
    @(d/transact conn
                 [{:rh/lookup-ref id
                   :rh/state      :scheduled}
                  [:set/union [:rh/lookup-ref id] :rh/replace #{replace}]])))

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
            [:set/reset [:rh/lookup-ref id] :rh/org-history (replay/history->set org-history)]
            [:set/reset [:rh/lookup-ref id] :rh/new-history (replay/history->set new-history)]
            [:set/reset [:rh/lookup-ref id] :rh/replace #{}]
            [:set/reset [:rh/lookup-ref id] :rh/tempids #{}]
            [:some/retract [:rh/lookup-ref id] :rh/done]
            [:some/retract [:rh/lookup-ref id] :rh/error]
            {:rh/lookup-ref id
             :rh/tx-index 0}]]
    ;(log/info "lookup-ref:" lookup-ref)
    ;(log/info "replacements:" replacements)
    ;(log/info "new-history:\n" (with-out-str (pprint/pprint new-history)))
    ;(log/info "tx:\n" (with-out-str (pprint/pprint tx)))
    @(d/transact conn tx)))


(defn process-scheduled! [conn]
  (when-let [lookup-ref (d/q '[:find ?lookup-ref .
                               :in $
                               :where
                               [?e :rh/state :scheduled]
                               [?e :rh/lookup-ref ?lookup-ref]]
                             (d/db conn))]
    (process-single-schedule! conn (edn/read-string lookup-ref))))