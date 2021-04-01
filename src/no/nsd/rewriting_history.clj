(ns no.nsd.rewriting-history
  (:require [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay]
            [no.nsd.rewriting-history.schedule-impl :as schedule]
            [no.nsd.rewriting-history.wipe :as wipe]
            [no.nsd.rewriting-history.rollback :as rollback]
            [datomic.api :as d])
  (:import (java.util Date)))

; public API

(def schema impl/schema)

(defn init-schema! [conn]
  (impl/init-schema! conn))

(defn schedule-replacement!
  "Schedule a replacement of ^String match with ^String replacement
  for lookup-ref.
  This will create or update a rewrite-job that will need to be triggered
  at a later stage using process-scheduled!"
  [conn lookup-ref match replacement]
  (schedule/schedule-replacement! conn lookup-ref match replacement))

(defn cancel-replacement! [conn lookup-ref match replacement]
  "Cancel a scheduled replacement for lookup-ref."
  (schedule/cancel-replacement! conn lookup-ref match replacement))

(defn pending-replacements [conn lookup-ref]
  (schedule/pending-replacements (d/db conn) lookup-ref))

(defn rewrite-scheduled! [conn]
  (replay/process-state! conn :scheduled))

(defn excise-old-rewrite-jobs! [conn older-than-days]
  (wipe/wipe-old-rewrite-jobs! conn (Date.) older-than-days))

(defn available-rollback-times [conn lookup-ref]
  (rollback/available-times conn lookup-ref))

(defn rollback! [conn lookup-ref t]
  (rollback/rollback! conn lookup-ref t))

; convenience methods

(defn pull-flat-history
  "Returns a vector of all the EAVTOs that are reachable from lookup-ref,
  i.e. the full history of lookup-ref, with normalized values for E and T."
  [db lookup-ref]
  (impl/pull-flat-history-simple db lookup-ref))

(defn get-org-history [db lookup-ref]
  (impl/get-org-history db lookup-ref))
