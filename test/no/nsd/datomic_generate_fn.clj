(ns no.nsd.datomic-generate-fn
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [rewrite-clj.zip :as z]
            [rewrite-clj.node :as n]))

(defn fqn->fil [fqn]
  (str
    "src/"
    (-> fqn
        (str)
        (str/split #"/")
        (first)
        (str/replace "." "/")
        (str/replace "-" "_")
        (str ".clj"))))

(defn fqn->fn [fqn]
  (-> fqn
      (str)
      (str/split #"/")
      (last)
      (symbol)))

(defn find-fns [start-pos]
  (loop [pos start-pos
         fns []]
    (if-let [next-fn (some-> pos
                             (z/find-value z/next 'defn)
                             (z/up))]
      (recur (z/right next-fn)
             (conj fns (z/sexpr next-fn))),
      fns)))

(defn generate-function [fqn db-name write-to-file]
  (let [fil (fqn->fil fqn)
        reqs (-> (z/of-file fil)
                 (z/find-value z/next 'ns)
                 (z/find-value z/next :require)
                 (z/up)
                 (z/child-sexprs))
        impos (-> (z/of-file fil)
                  (z/find-value z/next 'ns)
                  (z/find-value z/next :import)
                  (z/up)
                  (z/child-sexprs))
        ident (fqn->fn fqn)
        fndef (-> (z/of-file fil)
                  (z/find-value z/next ident)
                  (z/up)
                  (z/sexpr))
        other-defns (->> (find-fns (z/of-file fil))
                         (remove #(= ident (second %)))
                         (mapv (fn [[_defn id args & body]]
                                 (list id args (apply list 'do body)))))
        params (some-> (z/of-file fil)
                       (z/find-value z/next ident)
                       (z/right)
                       (z/sexpr))
        code (some-> (z/of-file fil)
                     (z/find-value z/next ident)
                     (z/remove)
                     (z/remove)
                     (z/down)
                     (z/remove)
                     (z/edit (fn [n]
                               (list 'letfn other-defns
                                     (apply list 'do n))))
                     (z/sexpr))
        db-fn {:lang     "clojure"
               :requires (vec (drop 1 reqs))
               :imports (vec (drop 1 impos))
               :params   params
               :code     code}
        out-str (str "{:db/ident " db-name "\n"
                     " :db/fn #db/fn \n"
                     (pr-str db-fn)
                     "\n"
                     "}")]
    (when write-to-file
      (spit fil (-> (z/of-file fil)
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