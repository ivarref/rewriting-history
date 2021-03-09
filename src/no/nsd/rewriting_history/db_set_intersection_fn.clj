(ns no.nsd.rewriting-history.db-set-intersection-fn
  (:require [datomic.api :as d]
            [clojure.string :as str]))

(defn set--intersection
  [db lookup-ref-or-eid doc]
  (let [resolved-e (cond (string? lookup-ref-or-eid)
                         lookup-ref-or-eid

                         (vector? lookup-ref-or-eid)
                         (let [[id v] lookup-ref-or-eid]
                           (d/q '[:find ?e .
                                  :in $ ?id ?v
                                  :where
                                  [?e ?id ?v]]
                                db
                                id
                                v))

                         :else
                         lookup-ref-or-eid)]
    [[:db/add resolved-e :db/doc doc]]))

(def schema
  (clojure.edn/read-string
    {:readers {'db/id  datomic.db/id-literal
               'db/fn  datomic.function/construct
               'base64 datomic.codec/base-64-literal}}
    (slurp "stage-schema.edn")))

(comment
  (do
    (require '[rewrite-clj.zip :as z])
    (require '[clojure.edn :as edn])
    (let [reqs (-> (z/of-file *file*)
                   (z/find-value z/next 'ns)
                   (z/find-value z/next :require)
                   (z/up)
                   (z/child-sexprs))
          ident (-> (z/of-file *file*)
                    (z/find-value z/next 'defn)
                    (z/right)
                    (z/sexpr)
                    (str)
                    (str/replace "--" "/")
                    (keyword))
          params (-> (z/of-file *file*)
                     (z/find-value z/next 'defn)
                     (z/right)
                     (z/right)
                     (z/sexpr))
          code (-> (z/of-file *file*)
                   (z/find-value z/next 'defn)
                   (z/right)
                   (z/right)
                   (z/right)
                   (z/sexpr))
          out-file (str/replace *file* ".clj" ".edn")
          out-str (str "{:db/ident " ident "\n"
                       " :db/fn #db/fn \n"
                       (pr-str {:lang     "clojure"
                                :params   params
                                :requires (vec (drop 1 reqs))
                                :code     code})
                       "\n"
                       "}")]
      (-> (z/of-file *file*)
          (z/find-value z/next 'clojure.edn/read-string)
          (z/right)
          (z/right)
          (z/replace out-str))
      #_(clojure.edn/read-string
          {:readers {'db/id  datomic.db/id-literal
                     'db/fn  datomic.function/construct
                     'base64 datomic.codec/base-64-literal}}
          out-str)
      #_(binding [*data-readers*
                  (merge *data-readers*)]

          (edn/read-string {:readers {'db/id  datomic.db/id-literal
                                      'db/fn  datomic.function/construct
                                      'base64 datomic.codec/base-64-literal}}
                           "{:db/id #db/id [:db.part/user]}"))
      #_(delay {:db/ident ident
                :db/fn    {:lang     "clojure"
                           :params   params
                           :requires (vec (drop 1 reqs))
                           :code     code}}))))


