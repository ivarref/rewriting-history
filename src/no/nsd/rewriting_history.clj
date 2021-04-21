(ns no.nsd.rewriting-history
  (:require [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay]
            [no.nsd.rewriting-history.schedule-impl :as schedule]
            [no.nsd.rewriting-history.wipe :as wipe]
            [no.nsd.rewriting-history.rollback :as rollback]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.set :as set]
            [datomic.api :as d])
  (:import (java.util Date)))

; public API

(def schema impl/schema)

(declare pull-flat-history)
(declare pending-replacements)

(defn schedule-replacement!
  "Schedule a replacement of ^String match with ^String replacement
  for lookup-ref.
  Creates or updates a rewrite-job."
  [conn lookup-ref match replacement]
  (assert (string? match))
  (assert (string? replacement))
  (assert (some? (impl/resolve-lookup-ref conn lookup-ref))
          (str "Expected to find lookup-ref " lookup-ref))
  (let [curr-history (pull-flat-history conn lookup-ref)
        found-match-in-history? (->> curr-history
                                     (map (fn [[e a v t o]] v))
                                     (filter string?)
                                     (into #{})
                                     (reduce (fn [_ v]
                                               (when (str/includes? v match)
                                                 (reduced true)))
                                             false))]
    (cond (and (empty? match)
               (not-empty replacement))
          (do (log/warn "empty match given and non-empty replacement given, ignoring")
              (pending-replacements conn lookup-ref))
          found-match-in-history? (schedule/schedule-replacement! conn lookup-ref match replacement)
          :else (pending-replacements conn lookup-ref))))

(defn schedule-patch!
  [conn lookup-ref org-history new-history]
  (assert (vector? lookup-ref))
  (assert (vector? org-history))
  (assert (vector? new-history))
  (assert (some? (impl/resolve-lookup-ref conn lookup-ref))
          (str "Expected to find lookup-ref " lookup-ref))
  (if (= org-history new-history)
    nil
    (let [new-history (into #{} new-history)
          org-history (into #{} org-history)
          to-add (set/difference new-history org-history)
          to-remove (set/difference org-history new-history)
          id (pr-str lookup-ref)
          doseq! (fn [attr s]
                   (doseq [[e a v t o] s]
                     (let [m #:rh{:e (pr-str e)
                                  :a (pr-str a)
                                  :v (pr-str v)
                                  :t (pr-str t)
                                  :o (pr-str o)}]
                       @(d/transact conn [[:cas/contains [:rh/lookup-ref id] :rh/state #{:scheduled :done nil} :scheduled]
                                          [:set/union [:rh/lookup-ref id] attr #{m}]]))))]
      (doseq! :rh/patch-add to-add)
      (doseq! :rh/patch-remove to-remove))))

(defn cancel-replacement! [conn lookup-ref match replacement]
  "Cancel a scheduled replacement for lookup-ref. Updates an existing rewrite-job."
  (schedule/cancel-replacement! conn lookup-ref match replacement))

(defn pending-replacements [conn lookup-ref]
  "Get pending replacements for lookup-ref."
  (schedule/pending-replacements conn lookup-ref))

(defn all-pending-replacements [conn]
  "Get all pending replacements in the form of
   [{:attr        attr
     :ref         ref
     :match       ...
     :replacement ...}
    ...]"
  (schedule/all-pending-replacements conn))

(defn rewrite-scheduled! [conn]
  "Process all rewrite-jobs that are scheduled."
  (replay/process-state! conn :scheduled))

(defn excise-old-rewrite-jobs! [conn older-than-days]
  "Excise all rewrite-job metadata that are older than older-than-days days."
  (wipe/wipe-old-rewrite-jobs! conn (Date.) older-than-days))

(defn available-rollback-times [conn lookup-ref]
  "Get available rollback times for a lookup ref. Returns a set of java.util.Dates."
  (rollback/available-times conn lookup-ref))

(defn rollback! [conn lookup-ref ^Date t]
  "Rollback lookup-ref to a previous ^java.util.Date t."
  (rollback/rollback! conn lookup-ref t))

(defn assoc-lookup-ref [eavtos [attr idval] & kvs]
  (assert (vector? eavtos))
  (let [new-vals (into {} (mapv vec (partition 2 kvs)))]
    (if-let [eid (->> eavtos
                      (filter (fn [[e a v t o]]
                                (and (= a attr)
                                     (= v idval))))
                      (ffirst))]
      (->> eavtos
           (mapv (fn [[e a v t o :as eavto]]
                   (if (= e eid)
                     [e a (get new-vals a v) t o]
                     eavto))))
      eavtos)))

; convenience methods

(defn pull-flat-history
  "Returns a vector of all the EAVTOs that are reachable from lookup-ref,
  i.e. the full history of lookup-ref, with normalized values for E and T."
  [db lookup-ref]
  (impl/pull-flat-history-simple db lookup-ref))

(defn get-org-history [db lookup-ref]
  "Get the original flat history of lookup-ref as it was before rewriting started."
  (impl/get-org-history db lookup-ref))

(defn get-job-state [conn lookup-ref]
  (impl/job-state conn lookup-ref))