(ns no.nsd.rewriting-history.patch
  (:require [no.nsd.rewriting-history.schedule-init :as schedule-init]
            [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]
            [clojure.set :as set]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]))

(defn get-tags [conn lookup-ref]
  (->> (d/q '[:find [?p ...]
              :in $ ?lookup-ref
              :where
              [?e :rh/lookup-ref ?lookup-ref]
              [?e :rh/patch ?p]]
            (impl/to-db conn)
            (pr-str lookup-ref))
       (d/q '[:find [?tag ...]
              :in $ [?eid ...]
              :where
              [?e :rh/patch ?eid ?tx true]
              [?tx :rh/tag ?tag ?tx true]]
            (impl/history conn))
       (distinct)
       (sort)
       (vec)))

(defn get-res [conn lookup-ref]
  (let [get-attr (fn [patch-op]
                   (->> (schedule-init/get-patch (d/db conn) lookup-ref patch-op)
                        (sort-by (fn [[e a v t o]] [t e a o v]))
                        (map (partial mapv pr-str))
                        (map (partial zipmap [:e :a :v :t :o]))
                        (map #(assoc % :ref (pr-str lookup-ref)))
                        (vec)))]
    (sorted-map
      :add (get-attr true)
      :remove (get-attr false)
      :tags (get-tags conn lookup-ref))))

(defn schedule-patch!
  [conn lookup-ref tag org-history new-history]
  (assert (vector? lookup-ref))
  (assert (vector? org-history))
  (assert (vector? new-history))
  (assert (some? (impl/resolve-lookup-ref conn lookup-ref))
          (str "Expected to find lookup-ref " lookup-ref))
  (let [old-patch-values (get-res conn lookup-ref)
        new-history (into #{} new-history)
        org-history (into #{} org-history)
        to-add (set/difference new-history org-history)
        to-remove (set/difference org-history new-history)
        id (pr-str lookup-ref)
        doseq! (fn [patch-op s]
                 (doseq [[e a v t o] s]
                   (let [m #:rh{:e        (pr-str e)
                                :a        (pr-str a)
                                :v        (pr-str v)
                                :t        (pr-str t)
                                :o        (pr-str o)
                                :patch-op patch-op}]
                     @(d/transact conn [[:cas/contains [:rh/lookup-ref id] :rh/state #{:scheduled :done nil} :scheduled]
                                        [:set/union [:rh/lookup-ref id] :rh/patch #{m}]
                                        {:db/id  "datomic.tx"
                                         :rh/tag (pr-str tag)}]))))
        _ (doseq! true to-add)
        _ (doseq! false to-remove)
        new-patch-values (get-res conn lookup-ref)]
    (impl/log-state-change (impl/job-state (d/db conn) lookup-ref) lookup-ref)
    (merge-with into
                (sorted-map
                  :status (if (= new-patch-values old-patch-values)
                            "No changes"
                            "Scheduled patch")
                  :tags []
                  :add []
                  :remove [])
                new-patch-values)))

(defn cancel-patch!
  [conn lookup-ref org-history new-history]
  (assert (vector? lookup-ref))
  (assert (vector? org-history))
  (assert (vector? new-history))
  (assert (some? (impl/resolve-lookup-ref conn lookup-ref))
          (str "Expected to find lookup-ref " lookup-ref))
  (let [old-patch-values (get-res conn lookup-ref)
        new-history (into #{} new-history)
        org-history (into #{} org-history)
        to-add (set/difference new-history org-history)
        to-remove (set/difference org-history new-history)
        id (pr-str lookup-ref)
        doseq! (fn [patch-op s]
                 (doseq [[e a v t o] s]
                   (let [m #:rh{:e        (pr-str e)
                                :a        (pr-str a)
                                :v        (pr-str v)
                                :t        (pr-str t)
                                :o        (pr-str o)
                                :patch-op patch-op}]
                     @(d/transact conn [[:set/disj-if-empty [:rh/lookup-ref id]
                                         :rh/patch m
                                         [[:some/retract [:rh/lookup-ref id] :rh/state]]
                                         [[:cas/contains [:rh/lookup-ref id] :rh/state #{:scheduled :done nil} :scheduled]]]]))))
        _ (doseq! true to-add)
        _ (doseq! false to-remove)
        new-patch-values (get-res conn lookup-ref)]
    (impl/log-state-change (impl/job-state (d/db conn) lookup-ref) lookup-ref)
    (merge-with into
                (sorted-map
                  :status (if (= new-patch-values old-patch-values)
                            "No changes"
                            "Cancelled patch")
                  :add []
                  :remove []
                  :tags [])
                new-patch-values)))

(defn cancel-patch-tag!
  [conn tag]
  (let [db (impl/to-db conn)
        eids (->> (d/q '[:find [?p ...]
                         :in $
                         :where
                         [?e :rh/patch ?p]]
                       db)
                  (d/q '[:find [?eid ...]
                         :in $ ?tag [?eid ...]
                         :where
                         [_ :rh/patch ?eid ?tx true]
                         [?tx :rh/tag ?tag ?tx true]]
                       (impl/history db)
                       (pr-str tag))
                  (distinct)
                  (sort)
                  (vec))
        eid->lookup-ref (->> (d/q '[:find ?p ?lref
                                    :in $ [?p ...]
                                    :where
                                    [?e :rh/patch ?p]
                                    [?e :rh/lookup-ref ?lref]]
                                  db
                                  eids)
                             (into {}))
        dest-lrefs (distinct (vals eid->lookup-ref))
        id (some->> eid->lookup-ref
                    not-empty
                    vals
                    first)]
    (if (not-empty eids)
      (do
        (assert (= 1 (count dest-lrefs))
                (str "Expected a single destination lookup-ref for tag " tag))
        (let [old-patch-values (get-res conn (edn/read-string id))]
          (doseq [eid eids]
            @(d/transact conn [[:set/disj-if-empty [:rh/lookup-ref id]
                                :rh/patch (dissoc (d/pull db '[:*] eid) :db/id)
                                [[:some/retract [:rh/lookup-ref id] :rh/state]]
                                [[:cas/contains [:rh/lookup-ref id] :rh/state #{:scheduled :done nil} :scheduled]]]]))
          (let [new-patch-values (get-res conn (edn/read-string id))]
            (merge-with into
                        (sorted-map
                          :status (if (= new-patch-values old-patch-values)
                                    "No changes"
                                    "Cancelled patch")
                          :add []
                          :remove []
                          :tags [])
                        new-patch-values))))
      (sorted-map
        :status "No changes"
        :tags []
        :add []
        :remove []))))

(defn all-pending-patches [conn]
  (let [db (impl/to-db conn)]
    (->> (d/q '[:find [?lref ...]
                :in $
                :where
                [?e :rh/state :scheduled]
                [?e :rh/lookup-ref ?lref]
                [?e :rh/patch _]]
              db)
         (mapv edn/read-string)
         (sort)
         (map (partial get-res conn))
         (reduce (partial merge-with into) (sorted-map
                                             :status "List"
                                             :add []
                                             :remove []
                                             :tags [])))))