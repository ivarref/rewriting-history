(ns no.nsd.rewriting-history.schedule-init
  (:require [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [no.nsd.rewriting-history.add-rewrite-job :as add-job]))

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

(defn get-patch [db lookup-ref patch-op]
  (->> (d/q '[:find ?ee ?a ?v ?t ?o
              :in $ ?lookup-ref ?patch-op
              :where
              [?e :rh/lookup-ref ?lookup-ref]
              [?e :rh/patch ?p]
              [?p :rh/patch-op ?patch-op]
              [?p :rh/e ?ee]
              [?p :rh/a ?a]
              [?p :rh/v ?v]
              [?p :rh/t ?t]
              [?p :rh/o ?o]]
            db
            (pr-str lookup-ref)
            patch-op)
       (mapv (partial mapv edn/read-string))))

(defn process-single-schedule! [conn lookup-ref]
  (let [replacements (->> (d/q '[:find ?r ?match ?replacement
                                 :in $ ?lookup-ref
                                 :where
                                 [?e :rh/lookup-ref ?lookup-ref]
                                 [?e :rh/replace ?r]
                                 [?r :rh/match ?match]
                                 [?r :rh/replacement ?replacement]]
                               (d/db conn)
                               (pr-str lookup-ref))
                          (mapv (fn [[eid m r]] [eid (edn/read-string m) (edn/read-string r)]))
                          (mapv (partial zipmap [:eid :match :replacement])))
        patch-add (into [] (get-patch (d/db conn) lookup-ref true))
        patch-remove (into #{} (get-patch (d/db conn) lookup-ref false))
        curr-history (impl/pull-flat-history-simple conn lookup-ref)
        new-history (->> curr-history
                         (into patch-add)
                         (mapv (partial replace-eavto replacements))
                         (remove #(contains? patch-remove %))
                         (sort-by impl/sort-eavto)
                         (vec))]
    (assert (= (count new-history)
               (+ (count curr-history)
                  (count patch-add)
                  (* -1 (count patch-remove))))
            "Expected all patch remove to be found in existing history")
    (add-job/add-job! conn lookup-ref #{:scheduled} :pending-init new-history)))