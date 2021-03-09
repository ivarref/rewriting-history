(ns no.nsd.rewriting-history.db-set-intersection-fn
  (:require [datomic.api :as d]
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

(def datomic-fn-def
  (clojure.edn/read-string
    {:readers {'db/id  datomic.db/id-literal
               'db/fn  datomic.function/construct
               'base64 datomic.codec/base-64-literal}}
    "{:db/ident :set/intersection\n :db/fn #db/fn \n{:lang \"clojure\", :params [db lookup-ref-or-eid a new-set], :requires [[datomic.api :as d] [clojure.string :as str] [clojure.pprint :as pprint] [clojure.set :as set] [clojure.java.io :as io]], :code (let [is-ref? (= :db.type/ref (d/q (quote [:find ?type . :in $ ?attr :where [?attr :db/valueType ?t] [?t :db/ident ?type]]) db a)) e (cond (string? lookup-ref-or-eid) lookup-ref-or-eid (vector? lookup-ref-or-eid) (let [[id v] lookup-ref-or-eid] (d/q (quote [:find ?e . :in $ ?id ?v :where [?e ?id ?v]]) db id v)) :else lookup-ref-or-eid)] (cond (string? e) (let [retval (mapv (fn [set-entry] {a set-entry, :db/id e}) new-set)] retval) (false? is-ref?) (let [curr-set (into #{} (d/q (quote [:find [?v ...] :in $ ?e ?a :where [?e ?a ?v]]) db e a)) to-remove (set/difference curr-set new-set) to-add (set/difference new-set (set/intersection curr-set new-set)) tx (reduce into [] [(mapv (fn [rm] [:db/retract e a rm]) to-remove) (mapv (fn [add] [:db/add e a add]) to-add)])] tx) is-ref? nil))}\n}"))

