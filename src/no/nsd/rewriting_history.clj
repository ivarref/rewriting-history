(ns no.nsd.rewriting-history
  (:require [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay-impl]
            [no.nsd.rewriting-history.schedule-impl :as schedule]))

; public API

(defn init-schema! [conn]
  (impl/init-schema! conn))

(defn pull-flat-history [db [a v :as lookup-ref]]
  (impl/pull-flat-history-simple db lookup-ref))

(defn rewrite-history! [conn old-history new-history]
  (impl/rewrite-history! conn old-history new-history))

(defn verify-history! [conn job-id]
  (replay-impl/verify-history! conn job-id))

(defn schedule-replacement! [conn lookup-ref match replacement]
  (schedule/schedule-replacement! conn lookup-ref match replacement))

(defn cancel-replacement! [conn lookup-ref match replacement]
  (schedule/cancel-replacement! conn lookup-ref match replacement))

(defn process-scheduled! [conn]
  (schedule/process-scheduled! conn))