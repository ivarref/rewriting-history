(ns no.nsd.rewriting-history.tx
  (:require [no.nsd.rewriting-history.impl :as impl]
            [clojure.tools.logging :as log]
            [clojure.set :as set]))

; Generate transactions from a given history

(defn eavto->oeav-tx
  [db prev-tempids [e a v t o :as eavto]]
  (let [op (if o :db/add :db/retract)

        ent-id (cond (= e t)
                     "datomic.tx"

                     (contains? prev-tempids (str e))
                     (with-meta [:tempid (str e)] {:rewrite true})

                     :else
                     (str e))

        value (cond (not (impl/is-regular-ref? db a v))
                    v

                    (= v t)
                    "datomic.tx"

                    (contains? prev-tempids (str v))
                    (with-meta [:tempid (str v)] {:rewrite true})

                    :else
                    (str v))]
    [op ent-id a value]))

(defn eavtos->transaction
  [db lookup-ref tx-count prev-tempids [tx-index eavtos]]
  (let [p-tempids @prev-tempids
        self-tempids (->> eavtos
                          (map first)
                          (map str)
                          (into (sorted-set)))
        new-tempids (->> (set/difference self-tempids p-tempids)
                         (mapv (fn [tmpid]
                                 {:rh/tempid-str tmpid
                                  :rh/tempid-ref (let [[e _a _v t _o] (->> eavtos
                                                                           (filter #(= tmpid (str (first %))))
                                                                           (first))]
                                                   (if (= e t)
                                                     "datomic.tx"
                                                     (str e)))}))
                         (into #{}))
        db-id [:rh/lookup-ref (pr-str lookup-ref)]
        tx-done? (= (inc tx-index) tx-count)]
    (swap! prev-tempids set/union self-tempids)
    (reduce into
            []
            [(when-not tx-done?
               [[:db/cas db-id :rh/tx-index tx-index (inc tx-index)]])
             (mapv (partial eavto->oeav-tx db p-tempids) eavtos)
             [[:set/union db-id :rh/tempids new-tempids]]
             (if tx-done?
               [[:db/cas db-id :rh/state :rewrite-history :verify]]
               [[:cas/contains db-id :rh/state #{:rewrite-history} :rewrite-history]])])))

(defn generate-tx
  [db lookup-ref eavtos]
  (assert (vector? lookup-ref))
  (let [db (impl/to-db db)
        txes (partition-by impl/get-t eavtos)
        tempids (atom {})]
    (mapv (partial eavtos->transaction db lookup-ref (count txes) tempids)
          (map-indexed vector txes))))
