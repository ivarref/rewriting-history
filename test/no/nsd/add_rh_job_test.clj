(ns no.nsd.add-rh-job-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.shorter-stacktrace]
            [no.nsd.log-init]
            [datomic-schema.core]
            [no.nsd.rewriting-history :as rh]
            [clojure.tools.logging :as log]
            [datomic.api :as d]
            [clojure.edn :as edn]
            [no.nsd.rewriting-history.impl :as impl]))

(defn setup-schema! [tx!]
  (tx! #d/schema[[:db/txInstant2 :one :instant]])

  ; Setup schema for persisting of history-rewrites:
  (tx! #d/schema[[:rh/id :one :string :id]
                 [:rh/eid :many :long]
                 [:rh/org-history :many :ref :component]
                 [:rh/new-history :many :ref :component]
                 [:rh/tempids :many :string]
                 [:rh/e :one :string]
                 [:rh/a :one :string]
                 [:rh/v :one :string]
                 [:rh/t :one :string]
                 [:rh/o :one :string]])

  ; Setup application schema:
  (tx! #d/schema[[:m/id :one :string :id]
                 [:m/info :one :string]]))

(defn create-job! [conn tx!]
  ; Test case:
  (tx! [{:m/id "id" :m/info "original-data"}])
  (tx! [{:m/id "id" :m/info "bad-data"}])
  (tx! [{:m/id "id" :m/info "good-data"}])

  (let [org-history (rh/pull-flat-history conn [:m/id "id"])
        org-history-set (->> org-history
                             (map #(mapv pr-str %))
                             (mapv (partial zipmap [:rh/e :rh/a :rh/v :rh/t :rh/o]))
                             (into #{}))
        tx (into []
                 [{:rh/id          "job"
                   :rh/eid         (into #{} (-> org-history meta :original-eids))
                   :rh/org-history org-history-set
                   :rh/new-history org-history-set}])]
    (tx! tx)))

(defn get-new-history [conn lookup-ref]
  (->> (d/q '[:find ?n ?e ?a ?v ?t ?o
              :in $ ?ee
              :where
              [?ee :rh/new-history ?n]
              [?n :rh/e ?e]
              [?n :rh/a ?a]
              [?n :rh/v ?v]
              [?n :rh/t ?t]
              [?n :rh/o ?o]]
            (d/db conn)
            (impl/resolve-lookup-ref (d/db conn) lookup-ref))
       (vec)
       (mapv (fn [[n e a v t o]]
               (with-meta
                 [(read-string e)
                  (read-string a)
                  (read-string v)
                  (read-string t)
                  (read-string o)]
                 {:eid n})))
       (sort-by (fn [[e a v t o]] [t e a o v]))
       (vec)))

(deftest add-rewrite-job-test
  (testing "Store eavtos to a job"
    (let [conn (u/empty-conn)
          tx! (u/tx-fn! conn)]
      (setup-schema! tx!)
      (create-job! conn tx!)

      (is (= (get-new-history conn [:rh/id "job"])
             (rh/pull-flat-history conn [:m/id "id"]))))))


