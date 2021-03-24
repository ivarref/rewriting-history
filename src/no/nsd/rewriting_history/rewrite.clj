(ns no.nsd.rewriting-history.rewrite
  (:require [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [no.nsd.rewriting-history.tx :as tx]
            [clojure.walk :as walk])
  (:import (datomic Connection)))

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
        tx-resolved (resolve-tempids tx (get-tempids conn lookup-ref))]
    (log/info "rewrite history tx" (inc tx-index) "of total" (count txes) "transactions ...")
    @(d/transact conn tx-resolved)))