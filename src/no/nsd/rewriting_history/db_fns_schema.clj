(ns no.nsd.rewriting-history.db-fns-schema
  (:require [no.nsd.rewriting-history.db-set-reset-fn-generated :as set-reset]
            [no.nsd.rewriting-history.db-set-union-fn-generated :as set-union]
            [no.nsd.rewriting-history.db-some-retract-fn-generated :as some-retract]))

(def schema
  [set-reset/datomic-fn-def
   set-union/datomic-fn-def
   some-retract/datomic-fn-def])