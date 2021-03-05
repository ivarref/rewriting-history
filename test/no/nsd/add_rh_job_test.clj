(ns no.nsd.add-rh-job-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.shorter-stacktrace]
            [no.nsd.log-init]
            [datomic-schema.core]
            [no.nsd.rewriting-history :as rh]
            [clojure.tools.logging :as log]
            [datomic.api :as d]
            [no.nsd.rewriting-history.impl :as impl]
            [no.nsd.rewriting-history.replay-impl :as replay]))

(defn setup-schema! [tx!]
  (tx! #d/schema[[:db/txInstant2 :one :instant]])

  ; Setup schema for persisting of history-rewrites:
  (tx! #d/schema[[:rh/id :one :string :id]
                 [:rh/lookup-ref :one :string]
                 [:rh/eid :many :long]
                 [:rh/org-history :many :ref :component]
                 [:rh/new-history :many :ref :component]
                 [:rh/state :one :keyword]
                 [:rh/done :one :instant]
                 [:rh/error :one :instant]
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
                  [[:db/cas [:rh/id job-id] :rh/state :init :rewrite-history]]
                  (mapv (fn [eid] {:db/excise eid}) eids-to-excise))
                vec)]
    (log/debug "deleting initial eids:" eids-to-excise)
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
        lookup-tempid (fn [[o [tempid tempid-str] a v]]
                        (assert (and (string? tempid-str) (= :tempid tempid)))
                        [o (d/q '[:find ?tempid-ref .
                                  :in $ ?job-id ?tempid-str
                                  :where
                                  [?e :rh/id ?job-id]
                                  [?e :rh/tempids ?tmpid]
                                  [?tmpid :rh/tempid-str ?tempid-str]
                                  [?tmpid :rh/tempid-ref ?tempid-ref]]
                                (d/db conn)
                                job-id
                                tempid-str)
                         a v])
        new-hist-tx (->> (nth txes tx-index)
                         (mapv (fn [[o e a v :as oeav]]
                                 (if (vector? e)
                                   (lookup-tempid oeav)
                                   oeav))))
        save-tempids (save-tempids-metadata new-hist-tx)
        done? (= (inc tx-index) (count txes))
        tx (->> (concat [[:db/cas [:rh/id job-id] :rh/tx-index tx-index (inc tx-index)]
                         {:db/id [:rh/id job-id] :rh/tempids save-tempids}]
                        (when done?
                          [[:db/cas [:rh/id job-id] :rh/state :rewrite-history :verify]
                           #_{:db/id [:rh/id job-id] :rh/done (Date.)}])
                        new-hist-tx)
                vec)]
    (log/debug "applying transaction" (inc tx-index) "of total" (count txes) "transactions ...")
    @(d/transact conn tx)))

(defn process-job-step! [conn job-id]
  (let [state (job-state conn "job")]
    (cond
      (= :init state)
      (job-init! conn job-id)

      (= :rewrite-history state)
      (rewrite-history! conn job-id)

      (= :verify state)
      (rh/verify-history! conn job-id)

      :else
      (do
        (log/error "unhandled job state:" state)
        nil #_(throw (ex-info "unhandled job state" {:state state}))))))

(deftest replay-history-job-test
  (testing "Replay history job works"
    (let [conn1 (u/empty-conn)
          conn2 (u/empty-conn)
          tx1! (u/tx-fn! conn1)
          tx2! (u/tx-fn! conn2)]
      (setup-schema! tx1!)
      (setup-schema! tx2!)

      (tx1! [{:m/id "id" :m/info "original-data"}])
      (tx1! [{:m/id "id" :m/info "bad-data"}])
      (tx1! [{:m/id "id" :m/info "good-data"}])

      (let [org-history (rh/pull-flat-history conn1 [:m/id "id"])]
        (is (= org-history
               [[1 :db/txInstant2 #inst "1974-01-01T00:00:00.000-00:00" 1 true]
                [4 :m/id "id" 1 true]
                [4 :m/info "original-data" 1 true]
                [2 :db/txInstant2 #inst "1975-01-01T00:00:00.000-00:00" 2 true]
                [4 :m/info "original-data" 2 false]
                [4 :m/info "bad-data" 2 true]
                [3 :db/txInstant2 #inst "1976-01-01T00:00:00.000-00:00" 3 true]
                [4 :m/info "bad-data" 3 false]
                [4 :m/info "good-data" 3 true]]))

        (impl/add-rewrite-job! conn1 "job" org-history org-history)
        (impl/add-rewrite-job! conn2 "job" org-history org-history)

        ; This will wipe the existing data:
        (job-init! conn1 "job")

        ; Fake excision is done for conn2:
        @(d/transact conn2 [{:rh/id "job" :rh/state :rewrite-history}])

        (is (= (replay/get-new-history conn1 "job") org-history))
        (is (= (replay/get-new-history conn2 "job") org-history))

        (while (not= :done (job-state conn2 "job"))
          (process-job-step! conn2 "job"))

        (is (= (rh/pull-flat-history conn2 [:m/id "id"])
               org-history))))))
