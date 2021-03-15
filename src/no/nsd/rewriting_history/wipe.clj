(ns no.nsd.rewriting-history.wipe
  (:require [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn])
  (:import (datomic Connection)
           (java.util Date)
           (java.time ZonedDateTime ZoneId)))

(defn wipe-rewrite-job! [conn lookup-ref]
  (assert (vector? lookup-ref))
  (assert (instance? Connection conn))
  (when (impl/resolve-lookup-ref conn [:rh/lookup-ref (pr-str lookup-ref)])
    (let [history (impl/pull-flat-history conn [:rh/lookup-ref (pr-str lookup-ref)])
          eids (->> history
                    (filter (fn [[_e a _v _t _o]] (= "rh" (namespace a))))
                    (map first)
                    (into #{}))
          tx (mapv (fn [eid] {:db/excise eid})
                   eids)]
      (log/debug "excising" (count tx) "entities ...")
      (let [{:keys [db-after]} @(d/transact conn tx)]
        @(d/sync-excise conn (d/basis-t db-after))))))

(defn wipe-old-rewrite-jobs! [conn ^Date now older-than-days]
  (assert (instance? Connection conn))
  (assert (instance? Date now))
  (assert (pos-int? older-than-days))
  (let [end (-> (ZonedDateTime/ofInstant
                  (.toInstant now)
                  (ZoneId/of "UTC"))
                (.minusDays older-than-days)
                (.toInstant)
                (Date/from))
        wipe-jobs (d/q '[:find [?lref ...]
                         :in $ ?end
                         :where
                         [?e :rh/state :done]
                         [?e :rh/done ?done]
                         [(.before ^java.util.Date ?done ?end)]
                         [?e :rh/lookup-ref ?lref]]
                       (d/db conn)
                       end)]
    (doseq [lookup-ref (mapv edn/read-string wipe-jobs)]
      (wipe-rewrite-job! conn lookup-ref))
    (count wipe-jobs)))
