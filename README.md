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

You can assert that your entities are in state
`#{:scheduled :done nil}`. You will need to apply
this to all of your write transactions.

Another option is to run `rh/process-scheduled!` at a time
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

## API

The public API can be found in [src/no/nsd/rewriting_history.clj](src/no/nsd/rewriting_history.clj).

