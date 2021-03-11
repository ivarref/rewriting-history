(ns no.nsd.rewriting-history.db-fns-schema
  (:require [no.nsd.rewriting-history.db-set-reset-fn-generated :as set-reset]
            [no.nsd.rewriting-history.db-set-union-fn-generated :as set-union]))

(def schema
  [set-reset/datomic-fn-def
   set-union/datomic-fn-def])