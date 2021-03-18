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

(defn put-chunks! [conn chunk-size db-id attr items]
  (log/info "saving attribute" attr "for" db-id "of size" (count items) "...")
  (let [chunks (partition-all chunk-size (into [] items))]
    (doseq [[idx chunk] (map-indexed vector chunks)]
      #_(log/info "process chunk" (inc idx) "of" (count chunks))
      @(d/transact conn [[:set/union db-id attr (into #{} chunk)]])))
  #_(log/info "saving attribute" attr "for" db-id "of size" (count items) "... OK!"))

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
        tx [[:db/cas [:rh/lookup-ref id] :rh/state :scheduled :pending-init]
            [:set/reset [:rh/lookup-ref id] :rh/eid #{}]
            [:set/reset [:rh/lookup-ref id] :rh/org-history #{}]
            [:set/reset [:rh/lookup-ref id] :rh/new-history #{}]
            [:set/reset [:rh/lookup-ref id] :rh/replace #{}]
            [:set/reset [:rh/lookup-ref id] :rh/tempids #{}]
            [:some/retract [:rh/lookup-ref id] :rh/done]
            [:some/retract [:rh/lookup-ref id] :rh/error]
            {:rh/lookup-ref id
             :rh/tx-index 0}]
        chunk-size 10]
    @(d/transact conn tx)
    (put-chunks! conn chunk-size [:rh/lookup-ref id] :rh/eid (into #{} (-> org-history meta :original-eids)))
    (put-chunks! conn chunk-size [:rh/lookup-ref id] :rh/org-history (impl/history->set org-history))
    (put-chunks! conn chunk-size [:rh/lookup-ref id] :rh/new-history (impl/history->set new-history))
    @(d/transact conn [[:db/cas [:rh/lookup-ref id] :rh/state :pending-init :init]])))