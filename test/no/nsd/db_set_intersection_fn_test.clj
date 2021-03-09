(ns no.nsd.db-set-intersection-fn-test
  (:require [clojure.test :refer :all]
            [rewrite-clj.zip :as z]
            [rewrite-clj.node :as n]
            [no.nsd.log-init]
            [no.nsd.shorter-stacktrace]
            [clojure.string :as str]
            [datomic-schema.core]
            [datomic.api :as d]))

(defn generate-function [write-to-file]
  (let [fil (str "src/no/nsd/rewriting_history/db_set_intersection_fn.clj")
        reqs (-> (z/of-file fil)
                 (z/find-value z/next 'ns)
                 (z/find-value z/next :require)
                 (z/up)
                 (z/child-sexprs))
        ident (-> (z/of-file fil)
                  (z/find-value z/next 'defn)
                  (z/right)
                  (z/sexpr)
                  (str)
                  (str/replace "--" "/")
                  (keyword))
        params (-> (z/of-file fil)
                   (z/find-value z/next 'defn)
                   (z/right)
                   (z/right)
                   (z/sexpr))
        code (-> (z/of-file fil)
                 (z/find-value z/next 'defn)
                 (z/right)
                 (z/right)
                 (z/right)
                 (z/sexpr))
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

(deftest dev-test
  (let [uri "datomic:mem://pet-store"
        _ (d/delete-database uri)
        _ (d/create-database uri)
        conn (d/connect uri)]
    @(d/transact conn [(generate-function false)])
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/desc :one :string]
                                [:m/info :many :string]
                                [:c/a :one :string]])

    (let [{:keys [tempids]} @(d/transact conn [{:db/id  "id"
                                                :m/id   "id"
                                                :m/desc "description"}
                                               [:set/intersection "id" :m/info #{"a" "b"}]])]
      @(d/transact conn [[:set/intersection [:m/id "id"] :m/info #{"b" "c"}]])
      (->> (d/pull (d/db conn) '[:*] [:m/id "id"]) :m/info (into #{})))))

(comment
  (dev-test))