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

(def datomic-fn-def
  (clojure.edn/read-string
    {:readers {'db/id  datomic.db/id-literal
               'db/fn  datomic.function/construct
               'base64 datomic.codec/base-64-literal}}
    "{:db/ident :set/intersection\n :db/fn #db/fn \n{:lang \"clojure\", :params [db lookup-ref-or-eid doc], :requires [[datomic.api :as d] [clojure.string :as str]], :code (let [resolved-e (cond (string? lookup-ref-or-eid) lookup-ref-or-eid (vector? lookup-ref-or-eid) (let [[id v] lookup-ref-or-eid] (d/q (quote [:find ?e . :in $ ?id ?v :where [?e ?id ?v]]) db id v)) :else lookup-ref-or-eid)] [[:db/add resolved-e :db/doc doc]])}\n}"))


(comment
  (let [uri "datomic:mem://pet-store"
        _ (d/delete-database uri)
        _ (d/create-database uri)
        conn (d/connect uri)]
    (require '[datomic-schema.core])
    @(d/transact conn [(generate-function false)])
    conn))


(comment
  (do
    (require '[rewrite-clj.zip :as z])
    (require '[clojure.edn :as edn])
    (require '[rewrite-clj.node :as n])
    (defn generate-function [write-to-file]
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
        (when write-to-file
          (spit *file* (-> (z/of-file *file*)
                           (z/find-value z/next 'clojure.edn/read-string)
                           (z/right)
                           (z/right)
                           (z/replace out-str)
                           (z/root)
                           (n/string))))
        (clojure.edn/read-string
          {:readers {'db/id  datomic.db/id-literal
                     'db/fn  datomic.function/construct
                     'base64 datomic.codec/base-64-literal}}
          out-str)))
    (generate-function true)))


