(ns no.nsd.rewriting-history.db-fns-generated)

(def set-disj "{:db/ident :set/disj\n :db/fn #db/fn \n{:lang \"clojure\", :requires [[datomic.api :as d] [clojure.walk :as walk]], :imports [(java.util HashSet List) (datomic Database)], :params [db lookup-ref attr value], :code (letfn [(to-clojure-types [m] (do (walk/prewalk (fn [e] (cond (instance? HashSet e) (into #{} e) (and (instance? List e) (not (vector? e))) (vec e) :else e)) m)))] (do (let [value (to-clojure-types value) lookup-ref (to-clojure-types lookup-ref) _ (assert (vector? lookup-ref)) _ (assert (map? value)) _ (assert (keyword? attr)) _ (assert (true? (= :db.type/ref (d/q (quote [:find ?type . :in $ ?attr :where [?attr :db/valueType ?t] [?t :db/ident ?type]]) db attr))) (str \"expected \" attr \" to be of valueType :db.type/ref\")) db (if (instance? Database db) db (d/db db)) [id-a id-v] lookup-ref e (d/q (quote [:find ?e . :in $ ?a ?v :where [?e ?a ?v]]) db id-a id-v)] (when e (let [curr-set (into #{} (d/q (quote [:find [(pull ?v [*]) ...] :in $ ?e ?a :where [?e ?a ?v]]) db e attr)) found-eid (reduce (fn [_ cand] (when (= (dissoc cand :db/id) value) (reduced (:db/id cand)))) nil curr-set)] (when found-eid [[:db/retract lookup-ref attr found-eid]]))))))}\n}")
(def set-reset "{:db/ident :set/reset\n :db/fn #db/fn \n{:lang \"clojure\", :requires [[datomic.api :as d] [clojure.set :as set] [clojure.walk :as walk]], :imports [(java.util UUID HashSet List) (datomic Database)], :params [db lookup-ref attr values], :code (letfn [(to-clojure-types [m] (do (walk/prewalk (fn [e] (cond (instance? HashSet e) (into #{} e) (and (instance? List e) (not (vector? e))) (vec e) :else e)) m))) (rand-id [] (do (str \"id-\" (UUID/randomUUID))))] (do (let [values (to-clojure-types values) lookup-ref (to-clojure-types lookup-ref) _ (assert (vector? lookup-ref)) _ (assert (set? values)) db (if (instance? Database db) db (d/db db)) dbid (rand-id) is-ref? (= :db.type/ref (d/q (quote [:find ?type . :in $ ?attr :where [?attr :db/valueType ?t] [?t :db/ident ?type]]) db attr)) is-component? (and is-ref? (true? (d/q (quote [:find ?comp . :in $ ?attr :where [?attr :db/isComponent ?comp]]) db attr))) [id-a id-v] lookup-ref e (d/q (quote [:find ?e . :in $ ?a ?v :where [?e ?a ?v]]) db id-a id-v)] (doseq [v values] (when (and (map? v) (some? (:db/id v)) (not (string? (:db/id v)))) (throw (ex-info \"expected :db/id to be a string or not present\" {:element v})))) (if is-ref? (let [curr-set (when e (into #{} (d/q (quote [:find [(pull ?v [*]) ...] :in $ ?e ?a :where [?e ?a ?v]]) db e attr))) curr-set-without-eid (into #{} (mapv (fn [ent] (with-meta (dissoc ent :db/id) #:db{:id (:db/id ent)})) curr-set)) to-remove (->> (set/difference curr-set-without-eid values) (mapv (fn [e] (:db/id (meta e)))) (into #{})) to-add (->> (set/difference values (set/intersection curr-set-without-eid values)) (mapv (fn [e] (with-meta e {:tempid (or (:db/id e) (rand-id))}))) (sort-by (fn [e] (pr-str (into (sorted-map) e))))) tx (vec (concat [{id-a id-v, :db/id dbid}] (mapv (fn [rm] (if is-component? [:db/retractEntity rm] [:db/retract dbid attr rm])) to-remove) (mapv (fn [add] (merge #:db{:id (->> add (meta) :tempid)} add)) to-add) (mapv (fn [add] [:db/add dbid attr (->> add (meta) :tempid)]) to-add)))] tx) (let [curr-set (when e (into #{} (d/q (quote [:find [?v ...] :in $ ?e ?a :where [?e ?a ?v]]) db e attr))) to-remove (set/difference curr-set values) to-add (set/difference values (set/intersection curr-set values)) tx (vec (concat [{id-a id-v, :db/id dbid}] (vec (sort (mapv (fn [rm] [:db/retract dbid attr rm]) to-remove))) (vec (sort (mapv (fn [add] [:db/add dbid attr add]) to-add)))))] tx)))))}\n}")
(def set-union "{:db/ident :set/union\n :db/fn #db/fn \n{:lang \"clojure\", :requires [[clojure.walk :as walk] [clojure.tools.logging :as log] [datomic.api :as d] [clojure.set :as set]], :imports [(java.util UUID HashSet List) (datomic Database)], :params [db lookup-ref attr values], :code (letfn [(to-clojure-types [m] (do (walk/prewalk (fn [e] (cond (instance? HashSet e) (into #{} e) (and (instance? List e) (not (vector? e))) (vec e) :else e)) m))) (rand-id [] (do (str \"id-\" (UUID/randomUUID)))) (set-union-ref [db [id-a id-v] attr values] (do (let [e (d/q (quote [:find ?e . :in $ ?a ?v :where [?e ?a ?v]]) db id-a id-v) curr-set (when e (into #{} (d/q (quote [:find [(pull ?v [*]) ...] :in $ ?e ?a :where [?e ?a ?v]]) db e attr))) curr-set-without-eid (into #{} (mapv (fn [ent] (with-meta (dissoc ent :db/id) #:db{:id (:db/id ent)})) curr-set)) dbid (rand-id) to-add (->> (set/difference values curr-set-without-eid) (mapv (fn [e] (with-meta e {:tempid (or (:db/id e) (rand-id))}))) (sort-by (fn [e] (pr-str (into (sorted-map) e))))) tx (vec (concat [{id-a id-v, :db/id dbid}] (mapv (fn [add] (merge #:db{:id (->> add (meta) :tempid)} add)) to-add) (mapv (fn [add] [:db/add dbid attr (->> add (meta) :tempid)]) to-add)))] tx))) (set-union-primitives [db [id-a id-v] attr values] (do [{id-a id-v, attr values}]))] (do (let [values (to-clojure-types values) lookup-ref (to-clojure-types lookup-ref) _ (assert (vector? lookup-ref)) _ (assert (set? values)) _ (assert (keyword? attr)) db (if (instance? Database db) db (d/db db)) is-ref? (= :db.type/ref (d/q (quote [:find ?type . :in $ ?attr :where [?attr :db/valueType ?t] [?t :db/ident ?type]]) db attr))] (when is-ref? (doseq [v values] (assert (map? v) \"expected set element to be a map\"))) (doseq [v values] (when (and (map? v) (some? (:db/id v)) (not (string? (:db/id v)))) (throw (ex-info \"expected :db/id to be a string or not present\" {:element v})))) (if is-ref? (set-union-ref db lookup-ref attr values) (set-union-primitives db lookup-ref attr values)))))}\n}")
(def some-retract "{:db/ident :some/retract\n :db/fn #db/fn \n{:lang \"clojure\", :requires [[clojure.walk :as walk] [datomic.api :as d]], :imports [(java.util HashSet List) (datomic Database)], :params [db lookup-ref attr], :code (letfn [(to-clojure-types [m] (do (walk/prewalk (fn [e] (cond (instance? HashSet e) (into #{} e) (and (instance? List e) (not (vector? e))) (vec e) :else e)) m)))] (do (let [db (if (instance? Database db) db (d/db db)) lookup-ref (to-clojure-types lookup-ref) _ (assert (vector? lookup-ref)) _ (assert (keyword? attr)) [id-a id-v] lookup-ref] (when-let [e (d/q (quote [:find ?e . :in $ ?a ?v :where [?e ?a ?v]]) db id-a id-v)] (let [v (d/q (quote [:find ?v . :in $ ?e ?a :where [?e ?a ?v]]) db e attr)] (when (some? v) [[:db/retract lookup-ref attr v]]))))))}\n}")
