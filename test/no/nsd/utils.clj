(ns no.nsd.utils
  (:require [datomic.api :as d]
            [datomic-schema.core]))

(defn empty-conn []
  (let [uri "datomic:mem://hello-world"]
    (d/delete-database uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      conn)))
