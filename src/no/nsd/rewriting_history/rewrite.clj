(ns no.nsd.rewriting-history.rewrite
  (:require [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pprint])
  (:import (java.util Date)))

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

(defn get-tempids [conn lookup-ref]
  (->> (d/q '[:find ?s ?r
              :in $ ?ref
              :where
              [?e :rh/lookup-ref ?ref]
              [?e :rh/tempids ?tid]
              [?tid :rh/tempid-str ?s]
              [?tid :rh/tempid-ref ?r]]
            (impl/to-db conn)
            (pr-str lookup-ref))
       (into {})))

(comment
  (get-tempids c [:m/id "id"]))

(defn rewrite-history-get-tx [conn lookup-ref]
  (assert (vector? lookup-ref))
  (let [new-history (impl/get-new-history conn lookup-ref)
        txes (impl/history->transactions conn new-history)
        tx-index (d/q '[:find ?tx-index .
                        :in $ ?ref
                        :where
                        [?e :rh/lookup-ref ?ref]
                        [?e :rh/tx-index ?tx-index]]
                      (d/db conn)
                      (pr-str lookup-ref))
        expected-history (some->>
                           (tx-max new-history tx-index)
                           (history-take-tx new-history)
                           (impl/simplify-eavtos conn lookup-ref))
        actual-history (impl/pull-flat-history-simple conn lookup-ref)
        tempids (get-tempids conn lookup-ref)
        new-hist-tx (->> (nth txes tx-index)
                         (mapv (partial impl/resolve-tempid tempids)))
        save-tempids (conj (save-tempids-metadata new-hist-tx)
                           {:rh/tempid-str (str (tx-max new-history (inc tx-index)))
                            :rh/tempid-ref "datomic.tx"})
        tx-done? (= (inc tx-index) (count txes))
        db-id [:rh/lookup-ref (pr-str lookup-ref)]
        new-state (if tx-done? :verify :rewrite-history)
        tx (->> (concat (->> new-hist-tx
                             (map second)
                             (filter string?)
                             (distinct)
                             (mapv (fn [tempid] {:db/id tempid})))
                        [[:db/cas db-id :rh/tx-index tx-index (inc tx-index)]
                         {:db/id db-id :rh/tempids save-tempids}]
                        (if tx-done?
                          [[:db/cas db-id :rh/state :rewrite-history new-state]]
                          [[:cas/contains db-id :rh/state #{:rewrite-history} new-state]])
                        new-hist-tx)
                vec)]
    {:tx tx
     :tx-index tx-index
     :new-history new-history
     :new-hist-tx new-hist-tx
     :new-state new-state
     :expected-history expected-history
     :actual-history actual-history}))

(defn rewrite-history! [conn lookup-ref]
  (assert (vector? lookup-ref))
  (let [{:keys [tx
                expected-history
                actual-history
                new-state
                new-history
                tx-index]}
        (rewrite-history-get-tx conn lookup-ref)]
    (log/debug "expected-history:" expected-history)
    (if (= expected-history actual-history)
      (do
        (log/debug "tx:\n" (with-out-str (binding [*print-length* 120]
                                           (pprint/pprint tx))))
        (let [res @(d/transact conn tx)]
          (impl/log-state-change new-state lookup-ref)
          res))
      (do
        (log/error "expected history differs from actual history so far:")
        (log/error "expected history:\n" (with-out-str (pprint/pprint expected-history)))
        (log/error "actual history:" (with-out-str (pprint/pprint actual-history)))
        @(d/transact conn [[:db/cas [:rh/lookup-ref (pr-str lookup-ref)] :rh/state :rewrite-history :error]
                           {:db/id [:rh/lookup-ref (pr-str lookup-ref)] :rh/error (Date.)}])
        (impl/log-state-change :error lookup-ref)
        {:expected-history (history-take-tx new-history tx-index)}))))