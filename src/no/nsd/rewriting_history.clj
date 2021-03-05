(ns no.nsd.rewriting-history
  (:require [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay-impl]))

; public API

(defn init-schema! [conn]
  (impl/init-schema! conn))

(defn pull-flat-history [db [a v :as lookup-ref]]
  (impl/pull-flat-history-simple db lookup-ref))

(defn schedule-string-replace! [conn lookup-ref needle replacement])

(defn rewrite-history! [conn old-history new-history]
  (impl/rewrite-history! conn old-history new-history))

(defn verify-history! [conn job-id]
  (replay-impl/verify-history! conn job-id))