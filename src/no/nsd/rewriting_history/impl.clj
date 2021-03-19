(ns no.nsd.rewriting-history.impl
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.pprint :as pprint]
            [no.nsd.rewriting-history.dbfns.schema :as fn-schema])
  (:import (datomic Database)))

; private API, subject to change

(defn to-db [db-or-conn]
  (if (instance? Database db-or-conn)
    db-or-conn
    (d/db db-or-conn)))

(defn history [db-or-conn]
  (d/history (to-db db-or-conn)))

(defn as-of [conn t]
  (d/as-of (to-db conn) t))

(defn resolve-lookup-ref
  "Returns eid of lookup ref or nil if lookup ref is not found"
  [db lookup-ref-or-eid]
  (assert (vector? lookup-ref-or-eid))
  (d/q '[:find ?e .
         :in $ ?a ?v
         :where [?e ?a ?v]]
       (to-db db)
       (first lookup-ref-or-eid) (second lookup-ref-or-eid)))

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

(defn only-txInstant-eavtos [eavtos]
  (let [attrs (into #{} (map get-a eavtos))]
    (if (contains? attrs :tx/txInstant)
      eavtos
      (map (fn [[e a v t o :as eavto]]
             (if (= a :db/txInstant)
               [e :tx/txInstant v t o]
               eavto))
           eavtos))))

(defn only-txInstant [eavtos]
  (let [txes (partition-by get-t eavtos)]
    (reduce (fn [o tx] (set/union o (into #{} (only-txInstant-eavtos tx))))
            #{}
            txes)))

(defn pull-flat-history [db [a v :as lookup-ref]]
  (when-let [eid-long (resolve-lookup-ref db lookup-ref)]
    (let [seen (atom #{})
          db (to-db db)
          eavtos (eid->eavto-set seen db eid-long)
          tx-ids (into #{} (map get-t eavtos))
          tx-meta-eavtos (reduce (fn [o tx-id]
                                   (set/union o (only-txInstant (eid->eavto-set seen db tx-id))))
                                 #{}
                                 tx-ids)]
      (->> (set/union tx-meta-eavtos eavtos)
           (remove #(= :db/txInstant (get-a %)))
           (into [])
           (sort-by (fn [[e a v t o]] [t e a o v]))
           (vec)))))

(defn is-regular-ref? [db a v]
  (and (= :db.type/ref (d/q '[:find ?type .
                              :in $ ?attr
                              :where
                              [?attr :db/valueType ?t]
                              [?t :db/ident ?type]]
                            db a))
       (not (keyword? v))))

(defn simplify-eavtos [db lookup-ref eavtos]
  (let [tx-eids (into #{} (distinct (map get-t eavtos)))
        reg-eids (set/difference (into #{} (map first eavtos))
                                 tx-eids)
        eid-map (merge
                  (zipmap (sort tx-eids) (map (partial str "t_") (iterate inc 1)))
                  (zipmap (sort reg-eids) (map (partial str "e_") (iterate inc 1))))
        eids (->> (reduce into
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
                    (if (is-regular-ref? (to-db db) a v)
                      (get eid-map v)
                      v)
                    (get eid-map t)
                    o])))
      {:original-eids (vec (sort (vec (set/union tx-eids reg-eids))))
       :lookup-ref    lookup-ref})))

(defn pull-flat-history-simple [db lookup-ref]
  (let [db (to-db db)]
    (some->> (pull-flat-history db lookup-ref)
             (simplify-eavtos db lookup-ref))))

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

(defn history->set [hist]
  (->> hist
       (map #(mapv pr-str %))
       (mapv (partial zipmap [:rh/e :rh/a :rh/v :rh/t :rh/o]))
       (into #{})))

(def schema
  (into [#:db{:ident :rh/lookup-ref :cardinality :db.cardinality/one :valueType :db.type/string :unique :db.unique/identity}
         #:db{:ident :tx/txInstant :cardinality :db.cardinality/one :valueType :db.type/instant}
         #:db{:ident :rh/replace :cardinality :db.cardinality/many :valueType :db.type/ref :isComponent true}
         #:db{:ident :rh/match :cardinality :db.cardinality/one :valueType :db.type/string}
         #:db{:ident :rh/replacement :cardinality :db.cardinality/one :valueType :db.type/string}
         #:db{:ident :rh/eid :cardinality :db.cardinality/many :valueType :db.type/long}
         #:db{:ident :rh/org-history :cardinality :db.cardinality/many :valueType :db.type/ref :isComponent true}
         #:db{:ident :rh/new-history :cardinality :db.cardinality/many :valueType :db.type/ref :isComponent true}
         #:db{:ident :rh/state :cardinality :db.cardinality/one :valueType :db.type/keyword :index true}
         #:db{:ident :rh/done :cardinality :db.cardinality/one :valueType :db.type/instant}
         #:db{:ident :rh/error :cardinality :db.cardinality/one :valueType :db.type/instant}
         #:db{:ident :rh/tx-index :cardinality :db.cardinality/one :valueType :db.type/long}
         #:db{:ident :rh/tempids :cardinality :db.cardinality/many :valueType :db.type/ref :isComponent true}
         #:db{:ident :rh/tempid-str :cardinality :db.cardinality/one :valueType :db.type/string}
         #:db{:ident :rh/tempid-ref :cardinality :db.cardinality/one :valueType :db.type/ref}
         #:db{:ident :rh/e :cardinality :db.cardinality/one :valueType :db.type/string}
         #:db{:ident :rh/a :cardinality :db.cardinality/one :valueType :db.type/string}
         #:db{:ident :rh/v :cardinality :db.cardinality/one :valueType :db.type/string}
         #:db{:ident :rh/t :cardinality :db.cardinality/one :valueType :db.type/string}
         #:db{:ident :rh/o :cardinality :db.cardinality/one :valueType :db.type/string}]
        fn-schema/schema))

(defn init-schema! [conn]
  ; Setup schema for persisting of history-rewrites:
  @(d/transact conn schema))

(defn get-history [conn attr lookup-ref]
  (let [db (to-db conn)]
    (some->> (resolve-lookup-ref db [:rh/lookup-ref (pr-str lookup-ref)])
             (d/q '[:find ?e ?a ?v ?t ?o
                    :in $ ?attr ?eid
                    :where
                    [?eid ?attr ?n]
                    [?n :rh/e ?e]
                    [?n :rh/a ?a]
                    [?n :rh/v ?v]
                    [?n :rh/t ?t]
                    [?n :rh/o ?o]]
                  db
                  attr)
             (vec)
             (mapv (partial mapv read-string))
             (sort-by (fn [[e a v t o]] [t e a o v]))
             (vec))))

(defn get-new-history [conn lookup-ref]
  (get-history conn :rh/new-history lookup-ref))

(defn get-org-history [conn lookup-ref]
  (get-history conn :rh/org-history lookup-ref))

(defn log-state-change [state lookup-ref]
  (log/info "state is now" state "for" lookup-ref))

(comment
  (def conn (-> reloaded.repl/system
                :datomic
                :conn)))

(comment
  (def first-t (->> (d/q '[:find [?t ...]
                           :in $ ?ref
                           :where
                           [?e :rh/lookup-ref ?ref]
                           [?e :rh/state :init ?t true]]
                         (d/history (d/db conn))
                         (pr-str [:Meldeskjema/ref "995695"]))
                    (sort)
                    (first))))

(comment
  (get-org-history (d/as-of (d/db conn) first-t)
                   [:Meldeskjema/ref "995695"]))