(ns no.nsd.rewriting-history.impl
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.pprint :as pprint]))

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
          (throw (ex-info "Could not find lookup ref" {:ref lookup-ref-or-eid}))))
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
                           [?e ?aid ?v ?t ?o]
                           [?aid :db/ident ?a]]
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
         (sort-by (fn [[e a v t o]] [t e a o v]))
         (vec))))

(defn is-regular-ref? [db a v]
  (and (= :db.type/ref (d/q '[:find ?type .
                              :in $ ?attr
                              :where
                              [?attr :db/valueType ?t]
                              [?t :db/ident ?type]]
                            db a))
       (not (keyword? v))))

(def get-t #(nth % 3))

(defn simplify-eavtos [db eavtos]
  (let [eids (reduce into
                     []
                     [(map first eavtos) (map get-t eavtos)])
        eid-map (zipmap (vec (distinct (sort eids))) (iterate inc 1))]
    (->> eavtos
         (mapv (fn [[e a v t o]]
                 [(get eid-map e)
                  a
                  (if (is-regular-ref? db a v)
                    (get eid-map v)
                    v)
                  (get eid-map t)
                  o])))))

(defn pull-flat-history-simple [db lookup-ref]
  (->> (pull-flat-history db lookup-ref)
       (simplify-eavtos db)))

(defn eavto->oeav-tx
  [db tempids [e a v t o :as eavto]]
  (let [ent-id (if (contains? tempids (str e))
                 [:tempid (str e)]
                 (str e))
        value (if (is-regular-ref? db a v)
                (if (contains? tempids (str v))
                  [:tempid (str v)]
                  (str v))
                v)
        op (if o :db/add :db/retract)]
    [op ent-id a value]))

(defn eavtos->transaction
  [db [txout tempids] eavtos]
  (let [oeavs (mapv (partial eavto->oeav-tx db tempids) eavtos)
        self-tempids (->> oeavs
                          (map second)
                          (filter string?)
                          (into #{}))]
    [(conj txout oeavs)
     (into (sorted-set) (set/union tempids self-tempids))]))

(defn history->transactions
  [db eavtos]
  (let [txes (partition-by (fn [[e a v t o]] t) eavtos)]
    (first
      (reduce (partial eavtos->transaction db) [[] #{}] txes))))

(defn maybe-resolve [tempids v]
  (if (and (vector? v)
           (= 2 (count v))
           (= :tempid (first v)))
    (let [new-value (get tempids (second v))]
      new-value)
    v))

(defn resolve-tempid [tempids [op e a v]]
  [op
   (maybe-resolve tempids e)
   a
   (maybe-resolve tempids v)])

(defn apply-txes! [conn txes]
  (reduce
    (fn [prev-tempids tx]
      (let [new-txes (mapv (partial resolve-tempid prev-tempids) tx)]
        (let [{:keys [tempids]} @(d/transact conn new-txes)]
          (merge prev-tempids tempids))))
    {}
    txes))

(defn rewrite-history! [conn db old-history new-history])
