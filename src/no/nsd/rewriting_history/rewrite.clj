(ns no.nsd.rewriting-history.rewrite
  (:require [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [no.nsd.rewriting-history.tx :as tx]
            [clojure.walk :as walk]
            [clojure.pprint :as pprint])
  (:import (datomic Connection)
           (java.util Date)))

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

(defn resolve-tempids [tx tempids]
  (walk/postwalk
    (fn [v]
      (if (and (vector? v)
               (:rewrite (meta v)))
        (if-let [eid (get tempids (second v))]
          eid
          (do
            (log/error "Could not resolve tempid" (second v))
            (throw (ex-info (str "Could not resolve tempid " (second v))
                            {:tempid (second v)}))))
        v))
    tx))

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

(defn rewrite-history! [conn lookup-ref]
  (assert (vector? lookup-ref))
  (assert (instance? Connection conn))
  (let [tx-index (d/q '[:find ?tx-index .
                        :in $ ?ref
                        :where
                        [?e :rh/lookup-ref ?ref]
                        [?e :rh/tx-index ?tx-index]]
                      (d/db conn)
                      (pr-str lookup-ref))
        history (impl/get-new-history conn lookup-ref)
        txes (tx/generate-tx conn lookup-ref history)
        tx (nth txes tx-index)
        tx-resolved (resolve-tempids tx (get-tempids conn lookup-ref))
        expected-history (some->>
                           (tx-max history tx-index)
                           (history-take-tx history)
                           (impl/simplify-eavtos conn lookup-ref))
        actual-history (impl/pull-flat-history-simple conn lookup-ref)]
    (log/info "rewrite history tx" (inc tx-index) "of total" (count txes) "transactions ...")
    (if (= expected-history actual-history)
      @(d/transact conn tx-resolved)
      (do
        (log/error "expected history differs from actual history so far:")
        (log/error "expected history:\n" (with-out-str (pprint/pprint expected-history)))
        (log/error "actual history:" (with-out-str (pprint/pprint actual-history)))
        @(d/transact conn [[:db/cas [:rh/lookup-ref (pr-str lookup-ref)] :rh/state :rewrite-history :error]
                           {:db/id [:rh/lookup-ref (pr-str lookup-ref)] :rh/error (Date.)}])
        (impl/log-state-change :error lookup-ref)
        {:expected-history (history-take-tx history tx-index)}))))

(def ^:dynamic *loop* true)

(defn rewrite-history-loop! [conn lookup-ref]
  (assert (vector? lookup-ref))
  (assert (instance? Connection conn))
  (let [tx-index (d/q '[:find ?tx-index .
                        :in $ ?ref
                        :where
                        [?e :rh/lookup-ref ?ref]
                        [?e :rh/tx-index ?tx-index]]
                      (d/db conn)
                      (pr-str lookup-ref))
        history (impl/get-new-history conn lookup-ref)
        txes (vec (drop tx-index (tx/generate-tx conn lookup-ref history)))
        tempids (get-tempids conn lookup-ref)
        start-time (System/currentTimeMillis)]
    (loop [[[idx tx] & rest] (map-indexed vector txes)
           tempids tempids]
      (let [{:keys [db-after]} @(d/transact conn (resolve-tempids tx tempids))]
        (when (and (not-empty rest) *loop*)
          (recur rest (get-tempids db-after lookup-ref)))))
    (let [spent-time (- (System/currentTimeMillis) start-time)]
      (log/info "spent" spent-time "ms on reapplying transactions"))
    (impl/log-state-change :verify lookup-ref)))