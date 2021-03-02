# Rewriting history

A library to rewrite Datomic history.

This library can rewrite the history of top level datoms.
A top level datom is a datom that is not referred by any other datom.
That is to say: no other datom is dependent on its existence.

This is important because the rewriting will cause the entity ID of the top level
datom and its children to change.

The top level datom and its children may only have references to `:db/idents`
or component entities. No regular references to external entities is allowed.
Thus the top level datom (and its children) should be entirely self contained.

Graph structures or loops is not supported. The entity must be a tree.