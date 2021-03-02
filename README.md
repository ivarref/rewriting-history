# Rewriting history

A library to rewrite Datomic history.

This library can rewrite the history of top level datoms.
A top level datom is a datom that is not referred by any other datom.
That is to say: no other datom is dependent on its existence.

This is important because the rewriting will cause the entity ID of the top level
datom and its children to change.