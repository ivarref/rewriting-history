# Rewriting history

[![Clojars Project](https://img.shields.io/clojars/v/no.nsd/rewriting-history.svg)](https://clojars.org/no.nsd/rewriting-history)

A library to rewrite Datomic history.

![Shoe-banging incident](Khruschev_shoe_fake.jpg "Shoe-banging incident")

This library can rewrite the history of top level entities.
A top level entity is an entity that is not referred by any other external entities.
That is to say: no other entities is dependent on its existence.
This is important because the rewriting will cause the entity ID of the top level
entity and its children to change.

## 1 minute example

```clojure
(require '[no.nsd.rewriting-history :as rh])
(require '[datomic.api :as d])

; Create a new datomic connection.
; This needs to be a real database as in memory datomic does not have excision support
(def conn (let [uri "datomic:sql://rh-demo?REAL_JDBC_URL_HERE"]
            (d/delete-database uri)
            (d/create-database uri)
            (d/connect uri)))

; Init rewriting-history schema
(rh/init-schema! conn)

; Init demo schema
@(d/transact conn [#:db{:ident :m/id   :cardinality :db.cardinality/one :valueType :db.type/string :unique :db.unique/identity}
                   #:db{:ident :m/info :cardinality :db.cardinality/one :valueType :db.type/string}])

; Add initial data
@(d/transact conn [{:m/id "id" :m/info "initial-data"}])

; Mistakingly add sensitive data that we will want to censor 
@(d/transact conn [{:m/id "id" :m/info "sensitive-data"}])

; Add more data
@(d/transact conn [{:m/id "id" :m/info "good-data"}])

; Schedule a replacement
(rh/schedule-replacement! conn [:m/id "id"] "sensitive-data" "censored-data")

; Process scheduled replacements
(rh/process-scheduled! conn)

; Verify that the string "sensitive-data" is gone from the history of the database:
(assert (= #{"initial-data" "censored-data" "good-data"}
           (into #{} (d/q '[:find [?v ...]
                            :in $
                            :where
                            [?e :m/id "id"]
                            [?e :m/info ?v]]
                          (d/history (d/db conn))))))
```

## Rationale

Regular Datomic excision does not remove retractions of non-existent entities
that are part of the first transaction in the new history.
This means that sensitive data can be left in the history database as retractions.

See [rationale_test.clj](test/no/nsd/rationale_test.clj) for a demonstration
of this problem.

This library aims to solve this problem, and leave the user free to rewrite the history
as she/he likes.

## Features

* Graph / loop structures is supported.

* Regular and component references is supported.

* :db/idents will not be excised during history rewriting and is thus considered permanent.

## Limitations
 
* Entity and transactions IDs will be replaced during rewriting of history. 
  If external systems depend on or worse stores these values, things will break.

* `db/txInstant` of `datomic.tx` will have new values. 
Thus using `(d/as-of db #inst"2021-...")` does not make sense.  
The original value is stored in `tx/txInstant`.

* Potential source of incorrect history:
  Re-playing the transaction history must use several transactions. It's not possible to
  both excise and re-play the entire history in one go. Thus this is a source
  of bugs if ordinary writes happen to the same entity as it is being rewritten.
  It will however be detected at the end of replaying of the history.
 