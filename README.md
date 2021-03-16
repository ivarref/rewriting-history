# Rewriting history

[![Clojars Project](https://img.shields.io/clojars/v/no.nsd/rewriting-history.svg)](https://clojars.org/no.nsd/rewriting-history)

A library to rewrite Datomic history.

![Shoe-banging incident](Khruschev_shoe_fake.jpg "Shoe-banging incident")

This library can rewrite the history of top level entities.
A top level entity is an entity that is not referred by any other external entities.
That is to say: no other entities is dependent on its existence.
This is important because the rewriting will cause the entity ID of the top level
entity and its children to change.

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

Re-playing the transaction history must use several transactions. It's not possible to
both excise and re-play the entire history in one go. Thus this is a potential source
of bugs.