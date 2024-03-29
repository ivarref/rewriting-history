(ns no.nsd.patch-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history :as rh]
            [datomic.api :as d]))

(deftest cancel-patch-test
  (let [conn (u/empty-conn-days-txtime)]
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/files :many :ref :component]
                                [:fil/id :one :uuid]
                                [:fil/name :one :string]])
    @(d/transact conn rh/schema)

    (let [fil-eid (get-in @(d/transact conn [{:m/id "id" :m/files
                                                    {:db/id    "fil"
                                                     :fil/id   u/uuid-1
                                                     :fil/name "secret.txt"}}])
                          [:tempids "fil"])
          _ @(d/transact conn [{:db/id    fil-eid
                                :fil/id   u/uuid-2
                                :fil/name "new.txt"}])
          hist (rh/pull-flat-history conn [:m/id "id"])
          new-hist (-> hist
                       (rh/assoc-lookup-ref [:fil/id u/uuid-1]
                                            :fil/id u/uuid-0
                                            :fil/name "deleted.txt"))]
      (is (= [[1 :m/files 2 -1 true]
              [1 :m/id "id" -1 true]
              [2 :fil/id #uuid "00000000-0000-0000-0000-000000000001" -1 true]
              [2 :fil/name "secret.txt" -1 true]
              [2 :fil/id #uuid "00000000-0000-0000-0000-000000000001" -2 false]
              [2 :fil/id #uuid "00000000-0000-0000-0000-000000000002" -2 true]
              [2 :fil/name "secret.txt" -2 false]
              [2 :fil/name "new.txt" -2 true]]
             (u/ignore-txInstant hist)))
      (is (= [[1 :m/files 2 -1 true]
              [1 :m/id "id" -1 true]
              [2 :fil/id #uuid "00000000-0000-0000-0000-000000000000" -1 true]
              [2 :fil/name "deleted.txt" -1 true]
              [2 :fil/id #uuid "00000000-0000-0000-0000-000000000000" -2 false]
              [2 :fil/id #uuid "00000000-0000-0000-0000-000000000002" -2 true]
              [2 :fil/name "deleted.txt" -2 false]
              [2 :fil/name "new.txt" -2 true]]
             (u/ignore-txInstant new-hist)))
      (is (= {:add
                      [{:e "2", :a ":fil/id", :v "uid:0", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "deleted.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/id", :v "uid:0", :t "-2", :o "false", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "deleted.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
              :remove
                      [{:e "2", :a ":fil/id", :v "uid:1", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "secret.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/id", :v "uid:1", :t "-2", :o "false", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "secret.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
              :status "Scheduled patch",
              :tags ["[:delete-file uid:1]"]}
             (u/abbr (rh/schedule-patch! conn [:m/id "id"] [:delete-file u/uuid-1] hist new-hist))))
      (is (= {:add [], :remove [], :status "Cancelled patch", :tags []}
             (rh/cancel-patch! conn [:m/id "id"] [:delete-file u/uuid-1] hist new-hist)))
      (is (= nil (rh/get-job-state conn [:m/id "id"])))
      (is (= {:add
                      [{:e "2", :a ":fil/id", :v "uid:0", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "deleted.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/id", :v "uid:0", :t "-2", :o "false", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "deleted.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
              :remove
                      [{:e "2", :a ":fil/id", :v "uid:1", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "secret.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/id", :v "uid:1", :t "-2", :o "false", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "secret.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
              :status "Scheduled patch",
              :tags ["[:delete-file uid:1]"]}
             (u/abbr (rh/schedule-patch! conn [:m/id "id"] [:delete-file u/uuid-1] hist new-hist))))

      (is (= {:add [], :remove [], :status "Cancelled patch", :tags []}
             (u/abbr (rh/cancel-patch-tag! conn [:delete-file u/uuid-1]))))

      (is (= {:add
                      [{:e "2", :a ":fil/id", :v "uid:0", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "deleted.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/id", :v "uid:0", :t "-2", :o "false", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "deleted.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
              :remove
                      [{:e "2", :a ":fil/id", :v "uid:1", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "secret.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/id", :v "uid:1", :t "-2", :o "false", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "secret.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
              :status "Scheduled patch",
              :tags ["[:delete-file uid:1]"]}
             (u/abbr (rh/schedule-patch! conn [:m/id "id"] [:delete-file u/uuid-1] hist new-hist))))

      @(d/transact conn [{:m/id "i2" :m/files
                                {:db/id    "fil"
                                 :fil/id   u/uuid-3
                                 :fil/name "strengt-hemmeleg.txt"}}])
      (rh/schedule-patch! conn
                          [:m/id "i2"]
                          [:delete-file u/uuid-3]
                          (rh/pull-flat-history conn [:m/id "i2"])
                          (rh/assoc-lookup-ref (rh/pull-flat-history conn [:m/id "i2"])
                                               [:fil/id u/uuid-3]
                                               :fil/id u/uuid-4
                                               :fil/name "deleted2.txt"))
      (is (= {:add
                      [{:e "2", :a ":fil/id", :v "uid:4", :t "-1", :o "true", :ref "[:m/id i2]"}
                       {:e "2", :a ":fil/name", :v "deleted2.txt", :t "-1", :o "true", :ref "[:m/id i2]"}
                       {:e "2", :a ":fil/id", :v "uid:0", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "deleted.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/id", :v "uid:0", :t "-2", :o "false", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "deleted.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
              :remove
                      [{:e "2", :a ":fil/id", :v "uid:3", :t "-1", :o "true", :ref "[:m/id i2]"}
                       {:e "2", :a ":fil/name", :v "strengt-hemmeleg.txt", :t "-1", :o "true", :ref "[:m/id i2]"}
                       {:e "2", :a ":fil/id", :v "uid:1", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "secret.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/id", :v "uid:1", :t "-2", :o "false", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "secret.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
              :status "List",
              :tags ["[:delete-file uid:3]" "[:delete-file uid:1]"]}
             (u/pprint (u/abbr (rh/all-pending-patches conn)))))
      (is (= {:add [], :remove [], :status "Cancelled patch", :tags []}
             (rh/cancel-patch-tag! conn [:delete-file u/uuid-3])))
      (is (= {:add [], :remove [], :status "No changes", :tags []}
             (rh/cancel-patch-tag! conn [:delete-file u/uuid-3])))
      (is (nil? (rh/get-job-state conn [:m/id "i2"])))

      (rh/rewrite-scheduled! conn)
      (is (= new-hist
             (rh/pull-flat-history conn [:m/id "id"]))))))