(ns no.nsd.rewriting-history.db-set-intersection-fn
  (:require [datomic.api :as d]
            [clojure.string :as str]
            [clojure.pprint :as pprint]
            [clojure.set :as set]))

(defn set--intersection
  [db lookup-ref-or-eid a new-set]
  (let [is-ref? (= :db.type/ref (d/q '[:find ?type .
                                       :in $ ?attr
                                       :where
                                       [?attr :db/valueType ?t]
                                       [?t :db/ident ?type]]
                                     db a))
        e (cond (string? lookup-ref-or-eid)
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
    (cond
      (string? e)
      (let [retval (mapv (fn [set-entry] {:db/id e a set-entry}) new-set)]
        retval)

      (false? is-ref?)
      (let [curr-set (into #{} (d/q '[:find [?v ...]
                                      :in $ ?e ?a
                                      :where
                                      [?e ?a ?v]]
                                    db e a))
            to-remove (set/difference curr-set new-set)
            to-add (set/difference new-set (set/intersection curr-set new-set))
            tx (reduce
                 into
                 []
                 [(mapv (fn [rm] [:db/retract e a rm]) to-remove)
                  (mapv (fn [add] [:db/add e a add]) to-add)])]
        tx)

      is-ref?
      nil
      #_(let [existing-set (d/q '[:find ?v
                                  :in $ ?e ?a
                                  :where
                                  [?e ?a ?v]]
                                db e a)]
          (println "existing-set:\n" (with-out-str (pprint/pprint existing-set)))
          nil))))

(comment
  (let [uri "datomic:mem://pet-store"
        _ (d/delete-database uri)
        _ (d/create-database uri)
        conn (d/connect uri)]
    (require '[datomic-schema.core])
    @(d/transact conn [(generate-function false)])
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/desc :one :string]
                                [:m/info :many :string]
                                [:c/a :one :string]])

    (let [{:keys [tempids]} @(d/transact conn [{:db/id  "id"
                                                :m/id   "id"
                                                :m/desc "description"}
                                               [:set/intersection "id" :m/info #{"a" "b"}]])
          a-id (get tempids "a")]
      @(d/transact conn [[:set/intersection [:m/id "id"] :m/info #{"b" "c"}]])
      (->> (d/pull (d/db conn) '[:*] [:m/id "id"])
           :m/info))))

(def datomic-fn-def
  (clojure.edn/read-string
    {:readers {'db/id  datomic.db/id-literal
               'db/fn  datomic.function/construct
               'base64 datomic.codec/base-64-literal}}
    "{:db/ident :set/intersection\n :db/fn #db/fn \n{:lang \"clojure\", :params [db lookup-ref-or-eid doc], :requires [[datomic.api :as d] [clojure.string :as str]], :code (let [resolved-e (cond (string? lookup-ref-or-eid) lookup-ref-or-eid (vector? lookup-ref-or-eid) (let [[id v] lookup-ref-or-eid] (d/q (quote [:find ?e . :in $ ?id ?v :where [?e ?id ?v]]) db id v)) :else lookup-ref-or-eid)] [[:db/add resolved-e :db/doc doc]])}\n}"))

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


