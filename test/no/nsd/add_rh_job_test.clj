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
                 [:rh/state :one :keyword]
                 [:rh/tx-index :one :long]
                 [:rh/tempids :many :ref :component]
                 [:rh/tempid-str :one :string]
                 [:rh/tempid-ref :one :ref]
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
                   :rh/state       :init
                   :rh/eid         (into #{} (-> org-history meta :original-eids))
                   :rh/org-history org-history-set
                   :rh/new-history org-history-set}])]
    (tx! tx)))

(defn get-new-history [conn job-id]
  (->> (d/q '[:find ?e ?a ?v ?t ?o
              :in $ ?ee
              :where
              [?ee :rh/new-history ?n]
              [?n :rh/e ?e]
              [?n :rh/a ?a]
              [?n :rh/v ?v]
              [?n :rh/t ?t]
              [?n :rh/o ?o]]
            (d/db conn)
            (impl/resolve-lookup-ref (d/db conn) [:rh/id job-id]))
       (vec)
       (mapv (partial mapv read-string))
       (sort-by (fn [[e a v t o]] [t e a o v]))
       (vec)))

(defn job-state [conn job-id]
  (d/q '[:find ?state .
         :in $ ?job-id
         :where
         [?e :rh/id ?job-id]
         [?e :rh/state ?state]]
       (d/db conn)
       job-id))

(defn job-init! [conn job-id]
  (let [eids-to-excise (d/q '[:find [?eid ...]
                              :in $ ?job-id
                              :where
                              [?e :rh/id ?job-id]
                              [?e :rh/eid ?eid]]
                            (d/db conn)
                            job-id)
        tx (->> (concat
                  [[:db/cas [:rh/id job-id] :rh/state :init :rewrite-history]
                   {:db/id [:rh/id job-id] :rh/tx-index 0}]
                  (mapv (fn [eid] {:db/excise eid}) eids-to-excise))
                vec)]
    (log/info "deleting initial eids:" eids-to-excise)
    (let [{:keys [db-after]} @(d/transact conn tx)]
      @(d/sync-excise conn (d/basis-t db-after)))))

(defn save-tempids-metadata [tx]
  (->> tx
       (map second)
       (filter string?)
       (map (fn [tempid] {:rh/tempid-str tempid
                          :rh/tempid-ref tempid}))
       (into #{})))

(defn rewrite-history! [conn job-id]
  (let [new-history (get-new-history conn job-id)
        txes (impl/history->transactions conn new-history)
        tx-index (d/q '[:find ?tx-index .
                        :in $ ?job-id
                        :where
                        [?e :rh/id ?job-id]
                        [?e :rh/tx-index ?tx-index]]
                      (d/db conn)
                      job-id)
        new-hist-tx (nth txes tx-index)
        save-tempids (save-tempids-metadata new-hist-tx)
        tx (->> (concat [[:db/cas [:rh/id job-id] :rh/tx-index tx-index (inc tx-index)]
                         {:db/id [:rh/id job-id] :rh/tempids save-tempids}]
                        new-hist-tx)
                vec)]
    (log/info "applying transaction" (inc tx-index) "of total" (count txes) "transactions ...")
    (u/pprint tx)
    (def t tx)
    @(d/transact conn tx)))



(comment
  (save-tempids-metadata
    [[:db/add
      "datomic.tx"
      :db/txInstant2
      #inst "1974-01-01T00:00:00.000-00:00"]
     [:db/add "4" :m/id "id"]
     [:db/add "4" :m/info "original-data"]]))


(defn process-job-step! [conn job-id]
  (let [state (job-state conn "job")]
    (cond
      (= :init state)
      (job-init! conn job-id)

      (= :rewrite-history state)
      (rewrite-history! conn job-id)

      :else
      (do
        (log/error "unhandled job state:" state)
        nil #_(throw (ex-info "unhandled job state" {:state state}))))))

(deftest add-rewrite-job-test
  (testing "Store eavtos to a job"
    (let [conn (u/empty-stage-conn "add-rewrite-job-test")
          tx! (u/tx-fn! conn)]
      (setup-schema! tx!)
      (create-job! conn tx!)
      (let [org-history (rh/pull-flat-history conn [:m/id "id"])]
        (job-init! conn "job")
        (is (= (get-new-history conn "job") org-history))

        (rewrite-history! conn "job")
        (rewrite-history! conn "job")
        ;(rewrite-history! conn "job")
        #_(is (= :init (process-job-step! conn "job")))
        #_(process-job-step! conn "job")))))



