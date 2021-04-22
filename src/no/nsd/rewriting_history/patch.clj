(ns no.nsd.rewriting-history.patch
  (:require [no.nsd.rewriting-history.schedule-init :as schedule-init]
            [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]
            [clojure.set :as set]
            [clojure.edn :as edn]))

(defn get-res [conn lookup-ref]
  (let [get-attr (fn [attr]
                   (->> (schedule-init/get-patch (d/db conn) lookup-ref attr)
                        (sort-by (fn [[e a v t o]] [t e a o v]))
                        (map (partial mapv pr-str))
                        (map (partial zipmap [:e :a :v :t :o]))
                        (map #(assoc % :ref (pr-str lookup-ref)))
                        (vec)))]
    (sorted-map
      :add (get-attr :rh/patch-add)
      :remove (get-attr :rh/patch-remove))))

(defn schedule-patch!
  [conn lookup-ref org-history new-history]
  (assert (vector? lookup-ref))
  (assert (vector? org-history))
  (assert (vector? new-history))
  (assert (some? (impl/resolve-lookup-ref conn lookup-ref))
          (str "Expected to find lookup-ref " lookup-ref))
  (when (not= org-history new-history)
    (let [new-history (into #{} new-history)
          org-history (into #{} org-history)
          to-add (set/difference new-history org-history)
          to-remove (set/difference org-history new-history)
          id (pr-str lookup-ref)
          doseq! (fn [attr s]
                   (doseq [[e a v t o] s]
                     (let [m #:rh{:e (pr-str e)
                                  :a (pr-str a)
                                  :v (pr-str v)
                                  :t (pr-str t)
                                  :o (pr-str o)}]
                       @(d/transact conn [[:cas/contains [:rh/lookup-ref id] :rh/state #{:scheduled :done nil} :scheduled]
                                          [:set/union [:rh/lookup-ref id] attr #{m}]]))))
          _ (doseq! :rh/patch-add to-add)
          _ (doseq! :rh/patch-remove to-remove)]))
  (get-res conn lookup-ref))

(defn all-pending-patches [conn]
  (let [db (impl/to-db conn)]
    (->> (d/q '[:find [?lref ...]
                :in $
                :where
                [?e :rh/state :scheduled]
                [?e :rh/lookup-ref ?lref]
                [?e :rh/patch-add _]]
              db)
         (mapv edn/read-string)
         (sort)
         (map (partial get-res conn))
         (reduce (partial merge-with into) (sorted-map :add [] :remove [])))))