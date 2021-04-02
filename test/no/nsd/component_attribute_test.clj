(ns no.nsd.component-attribute-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [datomic-schema.core]
            [datomic.api :as d]
            [no.nsd.rewriting-history :as rh]))

(deftest component-attribute-test
  (testing "Verify component attribute gets new entity id if we do not specify :db/id"
    (let [conn (u/empty-conn)]
      @(d/transact rh/schema conn)
      @(d/transact conn #d/schema[[:m/id :one :string :id]
                                  [:m/address :one :ref :component]
                                  [:m/country :one :ref :component]
                                  [:country/name :one :string :id]])
      @(d/transact conn [{:m/id      "id"
                          :m/address {:m/country {:country/name "Norway"}}}])
      @(d/transact conn [{:m/id      "id"
                          :m/address {:m/country {:country/name "Norway"}}}])
      (let [fh (rh/pull-flat-history conn [:m/id "id"])]
        (is (= (->> fh
                    (filterv #(= :m/address (second %)))
                    (mapv last))
               [true false true]))
        (u/rewrite-noop! conn [:m/id "id"])
        (is (= fh (rh/pull-flat-history conn [:m/id "id"])))))))
