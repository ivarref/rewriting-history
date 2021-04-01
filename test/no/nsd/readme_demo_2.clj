(ns no.nsd.readme-demo-2
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [datomic.api :as d]))

(require '[no.nsd.rewriting-history :as rh])

(def conn (u/empty-stage-conn))

@(d/transact conn rh/schema)
@(d/transact conn [#:db{:ident :m/id   :cardinality :db.cardinality/one :valueType :db.type/string :unique :db.unique/identity}
                   #:db{:ident :m/info :cardinality :db.cardinality/one :valueType :db.type/string}])

@(d/transact conn [{:m/id "id" :m/info "initial-data"}])
@(d/transact conn [{:m/id "id" :m/info "sensitive-data"}])
@(d/transact conn [{:m/id "id" :m/info "good-data"}])

(rh/pull-flat-history conn [:m/id "id"])

(rh/schedule-replacement! conn [:m/id "id"] "sensitive" "censored")

; Process scheduled replacements
(rh/rewrite-scheduled! conn)

(rh/schedule-replacement! conn [:m/id "id"] "a" "b")
(rh/cancel-replacement! conn [:m/id "id"] "a" "b")
(rh/pending-replacements conn [:m/id "id"])
(rh/rollback! conn [:m/id "id"] #inst"1981")
(rh/available-rollback-times conn [:m/id "id"])

(rh/excise-old-rewrite-jobs! conn 90)
(u/pprint (rh/available-rollback-times conn [:m/id "id"]))
(u/pprint (rh/pull-flat-history conn [:m/id "id"]))