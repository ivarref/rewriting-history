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

(defn get-patch [db lookup-ref attr]
  (->> (d/q '[:find ?ee ?a ?v ?t ?o
              :in $ ?lookup-ref ?attr
              :where
              [?e :rh/lookup-ref ?lookup-ref]
              [?e ?attr ?p]
              [?p :rh/e ?ee]
              [?p :rh/a ?a]
              [?p :rh/v ?v]
              [?p :rh/t ?t]
              [?p :rh/o ?o]]
            db
            (pr-str lookup-ref)
            attr)
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
        patch-add (into [] (get-patch (d/db conn) lookup-ref :rh/patch-add))
        patch-remove (into #{} (get-patch (d/db conn) lookup-ref :rh/patch-remove))
        new-history (->> (impl/pull-flat-history-simple conn lookup-ref)
                         (into patch-add)
                         (mapv (partial replace-eavto replacements))
                         (remove #(contains? patch-remove %))
                         (sort-by (fn [[e a v t o]] [t e a o v]))
                         (vec))]
    (add-job/add-job! conn lookup-ref #{:scheduled} :pending-init new-history)))