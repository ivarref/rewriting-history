(ns no.nsd.patch-bug-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history :as rh]
            [datomic.api :as d]))

(deftest patch-test-basic
  (let [conn (u/empty-conn-days-txtime)
        fil-uuid #uuid"00000000-0000-0000-0000-000000000002"]
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/files :many :ref :component]
                                [:m/info :one :string]
                                [:fil/id :one :uuid]
                                [:fil/name :one :string]])
    @(d/transact conn rh/schema)

    (let [fil-eid (get-in @(d/transact conn [{:m/id "id" :m/files
                                                    {:db/id    "fil"
                                                     :fil/id   fil-uuid
                                                     :fil/name "secret.txt"}}])
                          [:tempids "fil"])
          _ @(d/transact conn [{:db/id    fil-eid
                                :fil/id   #uuid"00000000-0000-0000-1111-000000000000"
                                :fil/name "new.txt"}])
          hist (rh/pull-flat-history conn [:m/id "id"])
          new-hist (-> hist
                       (rh/assoc-lookup-ref [:fil/id fil-uuid]
                                            :fil/id #uuid"00000000-0000-0000-0000-000000000000"
                                            :fil/name "deleted.txt"))]
      (is (= [[1 :m/files 2 -1 true]
              [1 :m/id "id" -1 true]
              [2 :fil/id #uuid "00000000-0000-0000-0000-000000000002" -1 true]
              [2 :fil/name "secret.txt" -1 true]
              [2 :fil/id #uuid "00000000-0000-0000-0000-000000000002" -2 false]
              [2 :fil/id #uuid "00000000-0000-0000-1111-000000000000" -2 true]
              [2 :fil/name "secret.txt" -2 false]
              [2 :fil/name "new.txt" -2 true]]
             (u/ignore-txInstant hist)))
      (is (= [[1 :m/files 2 -1 true]
              [1 :m/id "id" -1 true]
              [2 :fil/id #uuid "00000000-0000-0000-0000-000000000000" -1 true]
              [2 :fil/name "deleted.txt" -1 true]
              [2 :fil/id #uuid "00000000-0000-0000-0000-000000000000" -2 false]
              [2 :fil/id #uuid "00000000-0000-0000-1111-000000000000" -2 true]
              [2 :fil/name "deleted.txt" -2 false]
              [2 :fil/name "new.txt" -2 true]]
             (u/ignore-txInstant new-hist)))
      (is (= {:add
                      [{:e "2", :a ":fil/id", :v "uid:0", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "deleted.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/id", :v "uid:0", :t "-2", :o "false", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "deleted.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
              :remove
                      [{:e "2", :a ":fil/id", :v "uid:2", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "secret.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/id", :v "uid:2", :t "-2", :o "false", :ref "[:m/id id]"}
                       {:e "2", :a ":fil/name", :v "secret.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
              :status "Scheduled patch",
              :tags ["[:delete-file uid:2]"]}
             (u/abbr (rh/schedule-patch! conn [:m/id "id"] [:delete-file fil-uuid] hist new-hist))))
      @(d/transact conn [{:m/id "id"
                          :m/info "janei"}])
      (rh/rewrite-scheduled! conn)
      #_(is (= {:add
                        [{:e "4", :a ":fil/id", :v "uid:0", :t "1", :o "true", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/name", :v "deleted.txt", :t "1", :o "true", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/id", :v "uid:0", :t "2", :o "false", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/name", :v "deleted.txt", :t "2", :o "false", :ref "[:m/id id]"}],
                :remove
                        [{:e "4", :a ":fil/id", :v "uid:f3a0530b-6645-475f-b5ab-4000849fc2b9", :t "1", :o "true", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/name", :v "secret.txt", :t "1", :o "true", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/id", :v "uid:f3a0530b-6645-475f-b5ab-4000849fc2b9", :t "2", :o "false", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/name", :v "secret.txt", :t "2", :o "false", :ref "[:m/id id]"}],
                :status "No changes",
                :tags ["[:delete-file uid:f3a0530b-6645-475f-b5ab-4000849fc2b9]"]}
               (u/abbr (rh/schedule-patch! conn [:m/id "id"] [:delete-file fil-uuid] hist new-hist))))
      #_(is (= {:add
                        [{:e "4", :a ":fil/id", :v "uid:0", :t "1", :o "true", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/name", :v "deleted.txt", :t "1", :o "true", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/id", :v "uid:0", :t "2", :o "false", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/name", :v "deleted.txt", :t "2", :o "false", :ref "[:m/id id]"}],
                :remove
                        [{:e "4", :a ":fil/id", :v "uid:f3a0530b-6645-475f-b5ab-4000849fc2b9", :t "1", :o "true", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/name", :v "secret.txt", :t "1", :o "true", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/id", :v "uid:f3a0530b-6645-475f-b5ab-4000849fc2b9", :t "2", :o "false", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/name", :v "secret.txt", :t "2", :o "false", :ref "[:m/id id]"}],
                :status "List",
                :tags ["[:delete-file uid:f3a0530b-6645-475f-b5ab-4000849fc2b9]"]}
               (u/abbr (rh/all-pending-patches conn))))

      #_@(d/transact conn [{:m/id "i2" :m/files
                                  {:db/id    "fil"
                                   :fil/id   #uuid"00000000-1111-2222-3333-000000000000"
                                   :fil/name "strengt-hemmeleg.txt"}}])
      #_(rh/schedule-patch! conn
                            [:m/id "i2"]
                            [:delete-file #uuid"00000000-1111-2222-3333-000000000000"]
                            (rh/pull-flat-history conn [:m/id "i2"])
                            (rh/assoc-lookup-ref (rh/pull-flat-history conn [:m/id "i2"])
                                                 [:fil/id #uuid"00000000-1111-2222-3333-000000000000"]
                                                 :fil/id #uuid"00000000-0000-0000-1111-000000000000"
                                                 :fil/name "deleted2.txt"))
      #_(is (= {:add
                        [{:e "3", :a ":fil/id", :v "uid:00000000-0000-0000-1111-000000000000", :t "1", :o "true", :ref "[:m/id i2]"}
                         {:e "3", :a ":fil/name", :v "deleted2.txt", :t "1", :o "true", :ref "[:m/id i2]"}
                         {:e "4", :a ":fil/id", :v "uid:0", :t "1", :o "true", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/name", :v "deleted.txt", :t "1", :o "true", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/id", :v "uid:0", :t "2", :o "false", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/name", :v "deleted.txt", :t "2", :o "false", :ref "[:m/id id]"}],
                :remove
                        [{:e "3", :a ":fil/id", :v "uid:00000000-1111-2222-3333-000000000000", :t "1", :o "true", :ref "[:m/id i2]"}
                         {:e "3", :a ":fil/name", :v "strengt-hemmeleg.txt", :t "1", :o "true", :ref "[:m/id i2]"}
                         {:e "4", :a ":fil/id", :v "uid:f3a0530b-6645-475f-b5ab-4000849fc2b9", :t "1", :o "true", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/name", :v "secret.txt", :t "1", :o "true", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/id", :v "uid:f3a0530b-6645-475f-b5ab-4000849fc2b9", :t "2", :o "false", :ref "[:m/id id]"}
                         {:e "4", :a ":fil/name", :v "secret.txt", :t "2", :o "false", :ref "[:m/id id]"}],
                :status "List",
                :tags
                        ["[:delete-file uid:00000000-1111-2222-3333-000000000000]" "[:delete-file uid:f3a0530b-6645-475f-b5ab-4000849fc2b9]"]}
               (u/abbr (rh/all-pending-patches conn))))
      #_(rh/rewrite-scheduled! conn)
      #_(is (= new-hist
               (rh/pull-flat-history conn [:m/id "id"]))))))

#_(deftest patch-test
    (let [conn (u/empty-conn-days-txtime)
          fil-uuid #uuid"00000000-0000-0000-0000-000000000002"]
      @(d/transact conn #d/schema[[:m/id :one :string :id]
                                  [:m/files :many :ref :component]
                                  [:m/info :one :string]
                                  [:fil/id :one :uuid]
                                  [:fil/name :one :string]])
      @(d/transact conn rh/schema)

      (let [fil-eid (get-in @(d/transact conn [{:m/id "id" :m/files
                                                      {:db/id    "fil"
                                                       :fil/id   fil-uuid
                                                       :fil/name "secret.txt"}}])
                            [:tempids "fil"])
            _ @(d/transact conn [{:db/id    fil-eid
                                  :fil/id   #uuid"00000000-0000-0000-1111-000000000000"
                                  :fil/name "new.txt"}])
            hist (rh/pull-flat-history conn [:m/id "id"])
            new-hist (-> hist
                         (rh/assoc-lookup-ref [:fil/id fil-uuid]
                                              :fil/id #uuid"00000000-0000-0000-0000-000000000000"
                                              :fil/name "deleted.txt"))]
        (is (= [[1 :m/files 2 -1 true]
                [1 :m/id "id" -1 true]
                [2 :fil/id #uuid "00000000-0000-0000-0000-000000000002" -1 true]
                [2 :fil/name "secret.txt" -1 true]
                [2 :fil/id #uuid "00000000-0000-0000-0000-000000000002" -2 false]
                [2 :fil/id #uuid "00000000-0000-0000-1111-000000000000" -2 true]
                [2 :fil/name "secret.txt" -2 false]
                [2 :fil/name "new.txt" -2 true]]
               (u/ignore-txInstant hist)))
        (is (= [[1 :m/files 2 -1 true]
                [1 :m/id "id" -1 true]
                [2 :fil/id #uuid "00000000-0000-0000-0000-000000000000" -1 true]
                [2 :fil/name "deleted.txt" -1 true]
                [2 :fil/id #uuid "00000000-0000-0000-0000-000000000000" -2 false]
                [2 :fil/id #uuid "00000000-0000-0000-1111-000000000000" -2 true]
                [2 :fil/name "deleted.txt" -2 false]
                [2 :fil/name "new.txt" -2 true]]
               (u/pprint (u/ignore-txInstant new-hist))))
        (is (= {:add
                        [{:e "2", :a ":fil/id", :v "uid:0", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "deleted.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/id", :v "uid:0", :t "-2", :o "false", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "deleted.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
                :remove
                        [{:e "2", :a ":fil/id", :v "uid:2", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "secret.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/id", :v "uid:2", :t "-2", :o "false", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "secret.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
                :status "Scheduled patch",
                :tags ["[:delete-file uid:2]"]}
               (u/abbr (rh/schedule-patch! conn [:m/id "id"] [:delete-file fil-uuid] hist new-hist))))
        @(d/transact conn [{:m/id "id"
                            :m/info "janei"}])
        (is (= {:add
                        [{:e "2", :a ":fil/id", :v "uid:0", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "deleted.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/id", :v "uid:0", :t "-2", :o "false", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "deleted.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
                :remove
                        [{:e "2", :a ":fil/id", :v "uid:2", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "secret.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/id", :v "uid:2", :t "-2", :o "false", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "secret.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
                :status "No changes",
                :tags ["[:delete-file uid:2]"]}
               (u/abbr (rh/schedule-patch! conn [:m/id "id"] [:delete-file fil-uuid] hist new-hist))))
        (is (= {:add
                        [{:e "2", :a ":fil/id", :v "uid:0", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "deleted.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/id", :v "uid:0", :t "-2", :o "false", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "deleted.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
                :remove
                        [{:e "2", :a ":fil/id", :v "uid:2", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "secret.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/id", :v "uid:2", :t "-2", :o "false", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "secret.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
                :status "List",
                :tags ["[:delete-file uid:2]"]}
               (u/abbr (rh/all-pending-patches conn))))

        @(d/transact conn [{:m/id "i2" :m/files
                                  {:db/id    "fil"
                                   :fil/id   #uuid"00000000-1111-2222-3333-000000000000"
                                   :fil/name "strengt-hemmeleg.txt"}}])
        (rh/schedule-patch! conn
                            [:m/id "i2"]
                            [:delete-file #uuid"00000000-1111-2222-3333-000000000000"]
                            (rh/pull-flat-history conn [:m/id "i2"])
                            (rh/assoc-lookup-ref (rh/pull-flat-history conn [:m/id "i2"])
                                                 [:fil/id #uuid"00000000-1111-2222-3333-000000000000"]
                                                 :fil/id #uuid"00000000-0000-0000-1111-000000000000"
                                                 :fil/name "deleted2.txt"))
        (is (= {:add
                        [{:e "2", :a ":fil/id", :v "uid:00000000-0000-0000-1111-000000000000", :t "-1", :o "true", :ref "[:m/id i2]"}
                         {:e "2", :a ":fil/name", :v "deleted2.txt", :t "-1", :o "true", :ref "[:m/id i2]"}
                         {:e "2", :a ":fil/id", :v "uid:0", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "deleted.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/id", :v "uid:0", :t "-2", :o "false", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "deleted.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
                :remove
                        [{:e "2", :a ":fil/id", :v "uid:00000000-1111-2222-3333-000000000000", :t "-1", :o "true", :ref "[:m/id i2]"}
                         {:e "2", :a ":fil/name", :v "strengt-hemmeleg.txt", :t "-1", :o "true", :ref "[:m/id i2]"}
                         {:e "2", :a ":fil/id", :v "uid:2", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "secret.txt", :t "-1", :o "true", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/id", :v "uid:2", :t "-2", :o "false", :ref "[:m/id id]"}
                         {:e "2", :a ":fil/name", :v "secret.txt", :t "-2", :o "false", :ref "[:m/id id]"}],
                :status "List",
                :tags ["[:delete-file uid:00000000-1111-2222-3333-000000000000]" "[:delete-file uid:2]"]}
               (u/pprint (u/abbr (rh/all-pending-patches conn)))))
        (rh/rewrite-scheduled! conn)
        (is (= new-hist
               (u/pprint (rh/pull-flat-history conn [:m/id "id"])))))))