(ns no.nsd.rewriting-history.impl
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.pprint :as pprint])
  (:import (datomic Database)))

; private API, subject to change

(defn to-db [db-or-conn]
  (if (instance? Database db-or-conn)
    db-or-conn
    (d/db db-or-conn)))

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

(defn db-ident [db eid]
  (d/q '[:find ?ident .
         :in $ ?e
         :where
         [?e :db/ident ?ident]]
       db eid))

(defn is-ref? [db eid]
  (= :db.type/ref
     (d/q '[:find ?type .
            :in $ ?e
            :where
            [?e :db/valueType ?t]
            [?t :db/ident ?type]]
          db eid)))

(defn expand-refs [seen db [e a v t o :as eavto]]
  (if (@seen eavto)
    [eavto]
    (do
      (swap! seen conj eavto)
      (cond (and (is-ref? db a) (db-ident db v))
            [[e a (db-ident db v) t o]]

            (and o (is-ref? db a))
            (into [eavto] (eid->eavto-set seen db v))

            :else
            [eavto]))))

(defn eid->eavto-set [seen db eid]
  (let [eavtos (d/q '[:find ?e ?a ?v ?t ?o
                      :in $ ?e
                      :where
                      [?e ?aid ?v ?t ?o]
                      [?aid :db/ident ?a]]
                    (d/history db)
                    eid)
        expanded (->> eavtos
                      (mapcat (partial expand-refs seen db))
                      (into #{}))]
    (if (empty? eavtos)
      (do (log/warn "Could not find eid" eid)
          #{})
      expanded)))

(def get-e #(nth % 0))
(def get-a #(nth % 1))
(def get-v #(nth % 2))
(def get-t #(nth % 3))

(defn only-txInstant2-eavtos [eavtos]
  (let [attrs (into #{} (map get-a eavtos))]
    (if (contains? attrs :db/txInstant2)
      eavtos
      (map (fn [[e a v t o :as eavto]]
             (if (= a :db/txInstant)
               [e :db/txInstant2 v t o]
               eavto))
           eavtos))))

(defn only-txInstant2 [eavtos]
  (let [txes (partition-by get-t eavtos)]
    (reduce (fn [o tx] (set/union o (into #{} (only-txInstant2-eavtos tx))))
            #{}
            txes)))

(defn pull-flat-history [db [a v :as lookup-ref]]
  (let [seen (atom #{})
        db (to-db db)
        eid-long (resolve-lookup-ref db lookup-ref)
        eavtos (eid->eavto-set seen db eid-long)
        tx-ids (into #{} (map get-t eavtos))
        tx-meta-eavtos (reduce (fn [o tx-id]
                                 (set/union o (only-txInstant2 (eid->eavto-set seen db tx-id))))
                               #{}
                               tx-ids)]
    (->> (set/union tx-meta-eavtos eavtos)
         (remove #(= :db/txInstant (get-a %)))
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

(defn simplify-eavtos [db eavtos]
  (let [eids (->> (reduce into
                          []
                          [(map first eavtos) (map get-t eavtos)])
                  (sort)
                  (distinct)
                  (vec))
        eid-map (zipmap eids (iterate inc 1))]
    (with-meta
      (->> eavtos
           (mapv (fn [[e a v t o]]
                   [(get eid-map e)
                    a
                    (if (is-regular-ref? db a v)
                      (get eid-map v)
                      v)
                    (get eid-map t)
                    o])))
      {:original-eids eids})))

(defn pull-flat-history-simple [db lookup-ref]
  (let [db (to-db db)]
    (->> (pull-flat-history db lookup-ref)
         (simplify-eavtos db))))

(defn eavto->oeav-tx
  [db tempids [e a v t o :as eavto]]
  (let [op (if o :db/add :db/retract)

        ent-id (cond (= e t)
                     "datomic.tx"

                     (contains? tempids (str e))
                     [:tempid (str e)]

                     :else
                     (str e))

        value (cond (not (is-regular-ref? db a v))
                    v

                    (= v t)
                    "datomic.tx"

                    (contains? tempids (str v))
                    [:tempid (str v)]

                    :else
                    (str v))]
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
  "history->transactions creates transactions based on eavtos and a database.

  It is only dependent on the database as far as looking up schema definitions,
  thus it does not matter if this function is called before or after initial excision."
  [db eavtos]
  (let [db (to-db db)
        txes (partition-by get-t eavtos)]
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

(defn rewrite-history! [conn old-history new-history])

(defn history->set [hist]
  (->> hist
       (map #(mapv pr-str %))
       (mapv (partial zipmap [:rh/e :rh/a :rh/v :rh/t :rh/o]))
       (into #{})))

(defn add-rewrite-job! [conn job-id org-history new-history]
  (assert (string? job-id))
  (assert (vector? org-history))
  (assert (vector? new-history))
  (let [tx [{:rh/id          job-id
             :rh/state       :init
             :rh/tx-index    0
             :rh/eid         (into #{} (-> org-history meta :original-eids))
             :rh/org-history (history->set org-history)
             :rh/new-history (history->set new-history)}]]
    @(d/transact conn tx)))