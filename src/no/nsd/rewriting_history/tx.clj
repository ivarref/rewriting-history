(ns no.nsd.rewriting-history.tx
  (:require [no.nsd.rewriting-history.impl :as impl]
            [clojure.tools.logging :as log]
            [clojure.set :as set]))

; Generate transactions from a given history

(defn save-tempids-metadata [tx]
  (->> tx
       (map second)
       (filter string?)
       (remove #(= "datomic.tx" %))
       (map (fn [tempid] {:rh/tempid-str tempid
                          :rh/tempid-ref tempid}))
       (into #{})))

(defn tx-max [history tx-index]
  (->> history
       (mapv (fn [[e a v t o]] t))
       (distinct)
       (sort)
       (take tx-index)
       (last)))

(defn history-take-tx [history tmax]
  (->> history
       (take-while (fn [[e a v t o]] (<= t tmax)))
       (vec)))

(defn eavto->oeav-tx
  [db prev-tempids [e a v t o :as eavto]]
  (let [op (if o :db/add :db/retract)

        ent-id (cond (= e t)
                     "datomic.tx"

                     (contains? prev-tempids (str e))
                     [:tempid (str e)]

                     :else
                     (str e))

        value (cond (not (impl/is-regular-ref? db a v))
                    v

                    (= v t)
                    "datomic.tx"

                    (contains? prev-tempids (str v))
                    [:tempid (str v)]

                    :else
                    (do (log/info "could not find tempid" v)
                        (log/info "eavto" eavto)
                        (str v)))]
    [op ent-id a value]))

(defn eavtos->transaction
  [db lookup-ref prev-tempids eavtos]
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
                         (into #{}))]

    (swap! prev-tempids set/union self-tempids)
    (conj (mapv (partial eavto->oeav-tx db p-tempids) eavtos)
          [:set/union [:rh/lookup-ref (pr-str lookup-ref)] :rh/tempids new-tempids])))

(defn generate-tx
  "history->transactions creates transactions based on eavtos and a database.

  Returns a vector of OEAVs.

  It is only dependent on the database as far as looking up schema definitions,
  thus it does not matter if this function is called before or after initial excision."
  [db lookup-ref eavtos]
  (assert (vector? lookup-ref))
  (let [db (impl/to-db db)
        txes (partition-by impl/get-t eavtos)
        tempids (atom {})]
    (mapv (partial eavtos->transaction db lookup-ref tempids) txes)))

