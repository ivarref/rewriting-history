(ns no.nsd.rewriting-history.rollback
  (:require [datomic.api :as d]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.add-rewrite-job :as add-job]
            [clojure.tools.logging :as log])
  (:import (datomic Connection)
           (java.util Date)))

(defn rollback! [conn lookup-ref t]
  (assert (instance? Connection conn))
  (assert (some? (impl/resolve-lookup-ref conn [:rh/lookup-ref (pr-str lookup-ref)])))
  (assert (or (instance? Date t) (pos-int? t)))
  (if (instance? Date t)
    (do
      (log/info "resolving t ...")
      (rollback! conn lookup-ref
                 (d/q '[:find ?t .
                        :in $ ?lookup-ref ?inst
                        :where
                        [?e :rh/lookup-ref ?lookup-ref]
                        [?e :rh/state :init ?t true]
                        [?t :db/txInstant ?inst]]
                      (impl/history conn)
                      (pr-str lookup-ref)
                      t)))
    (do
      (assert (= t (d/q '[:find ?t .
                          :in $ ?lookup-ref ?t
                          :where
                          [?e :rh/lookup-ref ?lookup-ref]
                          [?e :rh/state :init ?t true]]
                        (impl/history conn)
                        (pr-str lookup-ref)
                        t))
              "Expected to find t in history with :rh/state set to :init")
      (log/info "checking t ... OK")
      (let [new-history (impl/get-org-history (impl/as-of conn t) lookup-ref)]
        (log/info "adding rollback job...")
        (add-job/add-job! conn lookup-ref #{:ok-rollback :done} :pending-rollback new-history)
        (log/info "adding rollback job... OK!")))))

(defn available-times [conn lookup-ref]
  (assert (vector? lookup-ref))
  (let [db (impl/history conn)
        times (->> (d/q '[:find ?inst ?t
                          :in $ ?lookup-ref
                          :where
                          [?e :rh/lookup-ref ?lookup-ref]
                          [?e :rh/state :init ?t true]
                          [?t :db/txInstant ?inst]]
                        db
                        (pr-str lookup-ref))
                   (map (fn [[inst t]] {inst [t]}))
                   (reduce (partial merge-with into) {})
                   (vec))]
    (if (every? #(= 1 (count (second %))) times)
      (into (sorted-set) (mapv first times))
      (do
        (log/error "Did not get a single distinctive transaction id for :db/txInstant, aborting!")
        (throw (ex-info "Did not get a single distinctive transaction id for :db/txInstant"
                        {:times times}))))))

(comment
  (available-times c [:m/id "id"]))