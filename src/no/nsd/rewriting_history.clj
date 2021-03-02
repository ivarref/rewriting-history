(ns no.nsd.rewriting-history
  (:require [no.nsd.rewriting-history.impl :as impl]))

; public API

(defn pull-flat-history [db [a v :as lookup-ref]]
  (impl/pull-flat-history-simple db lookup-ref))

(defn rewrite-history! [conn db old-history new-history]
  (impl/rewrite-history! conn db old-history new-history))