# Rewriting history

[![Clojars Project](https://img.shields.io/clojars/v/no.nsd/rewriting-history.svg)](https://clojars.org/no.nsd/rewriting-history)

A library to rewrite Datomic history. On-prem only.

![Shoe-banging incident](Khruschev_shoe_fake.jpg "Shoe-banging incident")

This library can rewrite the history of top level entities.
A top level entity is an entity that is not referred by any other external entities.
That is to say: no other entities is dependent on its existence.

## 1-minute example

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

; Rewrite scheduled replacements
(rh/rewrite-scheduled! conn)

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

Regular Datomic excision is also fairly coarse-grained, not allowing to make
tiny edits to the history.

This library aims to solve this problem, and leave the user free to rewrite the history
as she/he likes.

## Features

* Replace a substring with another substring in all historical values. 

* Arbitrary nested structures is supported.

* Regular and component references is supported.

* :db/idents will not be excised during history rewriting and is thus considered permanent.

## Basic usage and background

A Datomic database is a set of datoms.
A datom contains five values:

* E: entity ID
* A: attribute
* V: value
* T: transaction id
* O: transaction operation, true meaning add and false retract.

Consider the data from the above example:

```clojure
(def conn ,,,)
@(d/transact conn [{:m/id "id" :m/info "initial-data"}])
@(d/transact conn [{:m/id "id" :m/info "sensitive-data"}])
@(d/transact conn [{:m/id "id" :m/info "good-data"}])
```

What does the history of `[:m/id "id"]` look like?

```clojure
(require '[no.nsd.rewriting-history :as rh])
(rh/pull-flat-history conn [:m/id "id"])
=>
[; First transaction:
 [1 :tx/txInstant #inst"1973" 1 true]
 [4 :m/id "id" 1 true]
 [4 :m/info "initial-data" 1 true]
 
 ; Second transaction:
 [2 :tx/txInstant #inst"1974" 2 true]
 [4 :m/info "initial-data" 2 false]
 [4 :m/info "sensitive-data" 2 true] ; << sensitive data!
 
 ; Third transaction:
 [3 :tx/txInstant #inst"1975" 3 true]
 [4 :m/info "sensitive-data" 3 false] ; << sensitive data!
 [4 :m/info "good-data" 3 true]]
```

`pull-flat-history` returns a vector of EAVTOs with normalized values for E and T.
This is the history, as seen by rewriting-history, that will be rewritten.

After scheduling a replacement and processing scheduled jobs, 
the new history will look like the following:

```clojure
(rh/schedule-replacement! conn [:m/id "id"] "sensitive" "censored")
(rh/rewrite-scheduled! conn)
(rh/pull-flat-history conn [:m/id "id"])
=>
[[1 :tx/txInstant #inst"1973" 1 true]
 [4 :m/id "id" 1 true]
 [4 :m/info "initial-data" 1 true]
 
 [2 :tx/txInstant #inst"1974" 2 true]
 [4 :m/info "initial-data" 2 false]
 [4 :m/info "censored-data" 2 true] ; << fixed!
 
 [3 :tx/txInstant #inst"1975" 3 true]
 [4 :m/info "censored-data" 3 false] ; << fixed!
 [4 :m/info "good-data" 3 true]]
```

`schedule-replacement!` adds a pending replacement of `sensitive`
with `censored` for lookup-ref `[:m/id "id"]`. 
`rewrite-scheduled!` triggers actual rewriting of all scheduled jobs.

### Cancelling pending changes

It's possible to cancel a pending replacement:
```clojure
(rh/schedule-replacement! conn [:m/id "id"] "a" "b")
=> [{:match "a", :replacement "b"}]
(rh/cancel-replacement! conn [:m/id "id"] "a" "b")
=> []
```

### Rolling back to previous states

rewriting-history stores both the original history and the new history
in the database before rewriting takes place.
Thus it is possible to rollback to earlier states as long as that data
has not been excised:

```clojure
(rh/available-rollback-times conn [:m/id "id"])
=> #{#inst"1981"}
(rh/rollback! conn [:m/id "id"] #inst"1981")
(rh/pull-flat-history conn [:m/id "id"])
=>
[[1 :tx/txInstant #inst"1973" 1 true]
 [4 :m/id "id" 1 true]
 [4 :m/info "initial-data" 1 true]
 [2 :tx/txInstant #inst"1974" 2 true]
 [4 :m/info "initial-data" 2 false]
 [4 :m/info "sensitive-data" 2 true] ; << sensitive data is back
 [3 :tx/txInstant #inst"1975" 3 true]
 [4 :m/info "sensitive-data" 3 false] ; << sensitive data is back
 [4 :m/info "good-data" 3 true]]
```

### Cleaning up

It's obviously not good if sensitive data remains in the rewriting job data.
Here is how to excise old rewrite-jobs that are older than 90 days:

```clojure
(rh/excise-old-rewrite-jobs! conn 90)
(rh/available-rollback-times conn [:m/id "id"])
=> #{}
```

## Warning

`rewriting-history` does not, and does not try to, detect whether or not
the given top level entities are in fact top level entities that
are independent of everything else. If they are not truly independent,
you will break your database. Proceed with caution!

## Limitations and shortcomings
 
* Entity and transaction IDs will be replaced during rewriting of history. 
  If external systems directly depend on, or worse, store, these values, things will break.

* `db/txInstant` of `datomic.tx` will have new values.
Thus using `(d/as-of db #inst"<some date>")` does not make sense.
The original value is stored in `tx/txInstant`.

* Potential source of incorrect history:
  Re-playing the transaction history must use several transactions. It's not possible to
  both excise and re-play the entire history in one go. Thus this is a source
  of bugs if ordinary writes happen to the same entity as it is being rewritten.
  It will however be detected at the end of replaying of the history.

## Alleviating the shortcomings

### db/txInstant

If your datomic find queries uses `db/txInstant`, you will need to update
them to support `tx/txInstant`. If for example your query looks like
`(d/q '[... [?tx :db/txInstant ?inst]`, it should be updated to:

```
[?tx :db/txInstant ?inst-default]
[(get-else $ ?tx :tx/txInstant ?inst-default) ?inst]
```

See [get_else_tx_instant_test.clj](test/no/nsd/get_else_tx_instant_test.clj) for a demonstration.

### Potential source of incorrect history: Concurrent ordinary writes during history rewriting

One option is to run `rh/process-scheduled!` at a time
when ordinary writes is unlikely.
You may use the [recurring-cup](https://github.com/ivarref/recurring-cup) scheduler 
like the following:

```clojure
(require '[ivarref.recurring-cup :as cup])
(require '[no.nsd.rewriting-history :as rh])
(def conn "...")
(cup/start!)
(cup/schedule!
  ::rewrite-history
  (cup/daily {:hour 3 :timezone "Europe/Oslo"})
  (fn []
    (rh/process-scheduled! conn)))
```

Disclaimer: I'm the author of `recurring-cup`.

TODO document assert strategy.

## API

The public API can be found in [src/no/nsd/rewriting_history.clj](src/no/nsd/rewriting_history.clj).

## License

Copyright © 2021 Ivar Refsdal

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.