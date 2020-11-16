(ns no.nsd.utils
  (:require [no.nsd.rewriting-history.impl :as impl]
            [datomic.api :as d]))

(defn empty-conn []
  (let [uri "datomic:mem://hello-world"]
    (d/delete-database uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      conn)))

(def tx #(nth % 3))

(defn is-regular-ref? [db a v]
  (and (= :db.type/ref (d/q '[:find ?type .
                              :in $ ?attr
                              :where
                              [?attr :db/valueType ?t]
                              [?t :db/ident ?type]]
                            db a))
       (nil? (impl/is-db-ident? db v))))

(defn simplify-eavtos [db eavtos]
  (let [eid-map (zipmap (distinct (sort (map first eavtos))) (iterate inc 1))
        tx-map (zipmap (distinct (sort (map tx eavtos))) (iterate inc 1))]
    (->> eavtos
         (mapv (fn [[e a v t o]]
                 [(get eid-map e)
                  a
                  (if (is-regular-ref? db a v)
                    (get eid-map v)
                    v)
                  (get tx-map t)
                  o])))))
