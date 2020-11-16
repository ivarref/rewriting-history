(ns no.nsd.rewriting-history.impl
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]))

; private API, subject to change

(defn resolve-lookup-ref
  "Returns eid of lookup ref"
  [db lookup-ref-or-eid]
  (if (vector? lookup-ref-or-eid)
    (if-let [eid (d/q '[:find ?e .
                        :in $ ?a ?v
                        :where [?e ?a ?v]]
                      db (first lookup-ref-or-eid) (second lookup-ref-or-eid))]
      eid
      (do (log/error "Could not find lookup ref" lookup-ref-or-eid)
          nil))
    lookup-ref-or-eid))

(declare eid->eavto-set)

(defn single-tx-range-contains? [tx-range tx]
  (reduce (fn [_ [mn mx]]
            (if (and (>= tx mn)
                     (<= tx mx))
              (reduced true)
              false))
          false
          tx-range))

(defn tx-range-contains? [tx-ranges tx]
  (every? (fn [tx-range] (single-tx-range-contains? tx-range tx))
          tx-ranges))

(defn ref->tx-ranges [db [e a v t o :as eavto]]
  (let [starts (d/q '[:find [?t ...]
                      :in $ ?e ?a ?v
                      :where
                      [?e ?a ?v ?t true]]
                    (d/history db)
                    e a v)
        stops (into (d/q '[:find [?t ...]
                           :in $ ?e ?a ?v
                           :where
                           [?e ?a ?v ?t false]]
                         (d/history db)
                         e a v)
                    [Long/MAX_VALUE])]
    (->> (interleave (sort starts) (sort stops))
         (partition 2)
         (mapv vec))))

(defn is-db-ident? [db eid]
  (d/q '[:find ?ident .
         :in $ ?e
         :where
         [?e :db/ident ?ident]]
       db eid))

(defn expand-refs [db tx-ranges [e a v t o :as eavto]]
  (if (= :db.type/ref (d/q '[:find ?type .
                             :in $ ?attr
                             :where
                             [?attr :db/valueType ?t]
                             [?t :db/ident ?type]]
                           db a))
    (if-let [ident (is-db-ident? db v)]
      [[e a ident t o]]
      (if o
        (into [eavto] (eid->eavto-set db (into tx-ranges [(ref->tx-ranges db eavto)]) v))
        [eavto]))
    [eavto]))

(defn eid->eavto-set [db tx-ranges eid]
  (let [eavtos (->> (d/q '[:find ?e ?a ?v ?t ?o
                           :in $ ?e
                           :where
                           [?e ?a ?v ?t ?o]]
                           ;[?aa :db/ident ?a]]
                         (d/history db)
                         eid)
                    (filter (fn [[e a v t o]] (tx-range-contains? tx-ranges t))))
        expanded (->> eavtos
                      (mapcat (partial expand-refs db tx-ranges))
                      (into #{}))]
    (if (empty? eavtos)
      (do (log/warn "Could not find eid" eid)
          #{})
      expanded)))

(defn pull-flat-history [db [a v :as lookup-ref]]
  (let [eid-long (resolve-lookup-ref db lookup-ref)
        tx-range (ref->tx-ranges db [eid-long a v 0 0])]
    (->> (eid->eavto-set db [tx-range] eid-long)
         (into [])
         (sort-by (fn [[e a v t o]] [t o e a v]))
         (vec))))
