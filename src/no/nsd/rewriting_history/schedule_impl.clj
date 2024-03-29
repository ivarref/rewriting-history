(ns no.nsd.rewriting-history.schedule-impl
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]
            [no.nsd.rewriting-history.impl :as impl]
            [clojure.edn :as edn]))

(defn format-pending-replacements [items]
  (->> items
       (shuffle)
       (map (partial mapv edn/read-string))
       (map (partial zipmap [:lookup-ref :match :replacement]))
       (map #(-> %
                 (assoc :attr (first (:lookup-ref %)))
                 (assoc :ref (second (:lookup-ref %)))
                 (dissoc :lookup-ref)))
       (map (partial into (sorted-map)))
       (sort-by (juxt :attr :ref :match :replacement))
       (vec)))

(defn pending-replacements [db lookup-ref]
  (->> (d/q '[:find ?lookup-ref ?match ?replacement
              :in $ ?lookup-ref
              :where
              [?e :rh/lookup-ref ?lookup-ref]
              [?e :rh/replace ?r]
              [?r :rh/match ?match]
              [?r :rh/replacement ?replacement]]
            (impl/to-db db)
            (pr-str lookup-ref))
       (format-pending-replacements)))

(defn all-pending-replacements [db]
  (->> (d/q '[:find ?lookup-ref ?match ?replacement
              :where
              [?e :rh/lookup-ref ?lookup-ref]
              [?e :rh/replace ?r]
              [?r :rh/match ?match]
              [?r :rh/replacement ?replacement]]
            (impl/to-db db))
       (format-pending-replacements)))

(defn schedule-replacement! [conn lookup-ref match replacement]
  (assert (vector? lookup-ref))
  (assert (some? (impl/resolve-lookup-ref conn lookup-ref))
          (str "Expected to find lookup-ref " lookup-ref))
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
                 :rh/replacement (pr-str replacement)}
        res (-> @(d/transact conn
                             [[:cas/contains [:rh/lookup-ref id] :rh/state #{:scheduled :done nil} :scheduled]
                              [:set/union [:rh/lookup-ref id] :rh/replace #{replace}]])
                :db-after
                (pending-replacements lookup-ref))]
    (impl/log-state-change :scheduled lookup-ref)
    res))

(defn cancel-replacement! [conn lookup-ref match replacement]
  (assert (some? (impl/resolve-lookup-ref conn lookup-ref))
          (str "Expected to find lookup-ref " lookup-ref))
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
        {:keys [db-after]} @(d/transact
                              conn
                              [[:set/disj-if-empty [:rh/lookup-ref id]
                                :rh/replace {:rh/match       (pr-str match)
                                             :rh/replacement (pr-str replacement)}
                                [[:some/retract [:rh/lookup-ref id] :rh/state]]
                                [[:cas/contains [:rh/lookup-ref id] :rh/state #{:scheduled :done nil} :scheduled]]]])]
    (impl/log-state-change (impl/job-state db-after lookup-ref) lookup-ref)
    (pending-replacements db-after lookup-ref)))
