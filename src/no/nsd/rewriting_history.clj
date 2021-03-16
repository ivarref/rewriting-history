(ns no.nsd.rewriting-history
  (:require [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay]
            [no.nsd.rewriting-history.schedule-impl :as schedule]
            [no.nsd.rewriting-history.wipe :as wipe])
  (:import (java.util Date)))

; public API

(defn init-schema! [conn]
  (impl/init-schema! conn))

(defn schedule-replacement! [conn lookup-ref match replacement]
  (schedule/schedule-replacement! conn lookup-ref match replacement))

(defn cancel-replacement! [conn lookup-ref match replacement]
  (schedule/cancel-replacement! conn lookup-ref match replacement))

(defn process-scheduled! [conn]
  (replay/process-state! conn :scheduled))

(defn wipe-old-rewrite-jobs! [conn older-than-days]
  (wipe/wipe-old-rewrite-jobs! conn (Date.) older-than-days))

; convenience methods
(defn pull-flat-history [db [a v :as lookup-ref]]
  (impl/pull-flat-history-simple db lookup-ref))

(defn rewrite-history! [conn old-history new-history]
  (impl/rewrite-history! conn old-history new-history))

(defn verify-history! [conn job-id]
  (replay/verify-history! conn job-id))
