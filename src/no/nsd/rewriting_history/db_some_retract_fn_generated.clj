(ns no.nsd.rewriting-history.db-some-retract-fn-generated)

(def datomic-fn-def
  (clojure.edn/read-string
    {:readers {'db/id  datomic.db/id-literal
               'db/fn  datomic.function/construct
               'base64 datomic.codec/base-64-literal}}
    "{:db/ident :some/retract\n :db/fn #db/fn \n{:lang \"clojure\", :requires [[clojure.walk :as walk] [datomic.api :as d]], :imports [(java.util HashSet List) (datomic Database)], :params [db lookup-ref attr], :code (letfn [(to-clojure-types [m] (do (walk/prewalk (fn [e] (cond (instance? HashSet e) (into #{} e) (and (instance? List e) (not (vector? e))) (vec e) :else e)) m)))] (do (let [db (if (instance? Database db) db (d/db db)) lookup-ref (to-clojure-types lookup-ref) _ (assert (vector? lookup-ref)) _ (assert (keyword? attr)) [id-a id-v] lookup-ref] (when-let [e (d/q (quote [:find ?e . :in $ ?a ?v :where [?e ?a ?v]]) db id-a id-v)] (let [v (d/q (quote [:find ?v . :in $ ?e ?a :where [?e ?a ?v]]) db e attr)] (when (some? v) [[:db/retract lookup-ref attr v]]))))))}\n}"))