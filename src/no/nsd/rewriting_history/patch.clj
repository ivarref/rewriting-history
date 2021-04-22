(ns no.nsd.rewriting-history.patch
  (:require [no.nsd.rewriting-history.schedule-init :as schedule-init]
            [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]
            [clojure.set :as set]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]))

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
      :remove (get-attr false))))

(defn schedule-patch!
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
                   (let [m #:rh{:e (pr-str e)
                                :a (pr-str a)
                                :v (pr-str v)
                                :t (pr-str t)
                                :o (pr-str o)
                                :patch-op patch-op}]
                     @(d/transact conn [[:cas/contains [:rh/lookup-ref id] :rh/state #{:scheduled :done nil} :scheduled]
                                        [:set/union [:rh/lookup-ref id] :rh/patch #{m}]]))))
        _ (doseq! true to-add)
        _ (doseq! false to-remove)
        new-patch-values (get-res conn lookup-ref)]
    (impl/log-state-change (impl/job-state (d/db conn) lookup-ref) lookup-ref)
    (merge-with into
                (sorted-map
                  :status (if (= new-patch-values old-patch-values)
                            "No changes"
                            "Scheduled patch")
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
                   (let [m #:rh{:e (pr-str e)
                                :a (pr-str a)
                                :v (pr-str v)
                                :t (pr-str t)
                                :o (pr-str o)
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
                  :remove [])
                new-patch-values)))

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
         (reduce (partial merge-with into) (sorted-map :status "List" :add [] :remove [])))))