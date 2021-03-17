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
       (map (fn [tempid] {:rh/tempid-str tempid
                          :rh/tempid-ref tempid}))
       (into #{})))

(defn resolve-tempid [conn lookup-ref [o [tempid tempid-str] a v]]
  (assert (and (string? tempid-str) (= :tempid tempid)))
  [o
   (d/q '[:find ?tempid-ref .
          :in $ ?lookup-ref ?tempid-str
          :where
          [?e :rh/lookup-ref ?lookup-ref]
          [?e :rh/tempids ?tmpid]
          [?tmpid :rh/tempid-str ?tempid-str]
          [?tmpid :rh/tempid-ref ?tempid-ref]]
        (d/db conn)
        (pr-str lookup-ref)
        tempid-str)
   a v])

(defn history-take-tx [history tx]
  (let [tx-max (->> history
                    (mapv (fn [[e a v t o]] t))
                    (distinct)
                    (sort)
                    (take tx)
                    (last))]
    (when tx-max
      (->> history
           (take-while (fn [[e a v t o]] (<= t tx-max)))
           (vec)))))

(defn rewrite-history! [conn lookup-ref]
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
                           (history-take-tx new-history tx-index)
                           (impl/simplify-eavtos conn lookup-ref))
        actual-history (impl/pull-flat-history-simple conn lookup-ref)
        new-hist-tx (->> (nth txes tx-index)
                         (mapv (fn [[o e a v :as oeav]]
                                 (if (vector? e)
                                   (resolve-tempid conn lookup-ref oeav)
                                   oeav))))
        save-tempids (save-tempids-metadata new-hist-tx)
        tx-done? (= (inc tx-index) (count txes))
        db-id [:rh/lookup-ref (pr-str lookup-ref)]
        tx (->> (concat [[:db/cas db-id :rh/tx-index tx-index (inc tx-index)]
                         {:db/id db-id :rh/tempids save-tempids}]
                        (if tx-done?
                          [[:db/cas db-id :rh/state :rewrite-history :verify]]
                          [[:cas/contains db-id :rh/state #{:rewrite-history} :rewrite-history]])
                        new-hist-tx)
                vec)]
    (log/debug "expected-history:" expected-history)
    (if (= expected-history actual-history)
      (do
        (log/info "applying transaction" (inc tx-index) "of total" (count txes) "transactions ...")
        @(d/transact conn tx))
      (do
        (log/error "expected history differs from actual history so far:")
        (log/error "expected history:\n" (with-out-str (pprint/pprint expected-history)))
        (log/error "actual history:" (with-out-str (pprint/pprint actual-history)))
        @(d/transact conn [[:db/cas db-id :rh/state :rewrite-history :error]
                           {:db/id db-id :rh/error (Date.)}])
        {:expected-history (history-take-tx new-history tx-index)}))))