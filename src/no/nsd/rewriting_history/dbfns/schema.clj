(ns no.nsd.rewriting-history.dbfns.schema
  (:require [datomic.api :as d]
            [no.nsd.rewriting-history.dbfns.generated :as fns]))

(defn read-dbfn [s]
  (clojure.edn/read-string
    {:readers {'db/id  datomic.db/id-literal
               'db/fn  datomic.function/construct
               'base64 datomic.codec/base-64-literal}}
    s))

(def schema
  (mapv read-dbfn [fns/set-disj
                   fns/set-reset
                   fns/set-union
                   fns/some-retract
                   fns/cas-contains]))