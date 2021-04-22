(ns no.nsd.patch-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history :as rh]
            [datomic.api :as d]))

(deftest patch-test
  (let [conn (u/empty-conn-days-txtime)
        fil-uuid #uuid"f3a0530b-6645-475f-b5ab-4000849fc2b9"]
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/files :many :ref :component]
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
      (is (= [[3 :m/files 4 1 true]
              [3 :m/id "id" 1 true]
              [4 :fil/id #uuid "f3a0530b-6645-475f-b5ab-4000849fc2b9" 1 true]
              [4 :fil/name "secret.txt" 1 true]
              [4 :fil/id #uuid "f3a0530b-6645-475f-b5ab-4000849fc2b9" 2 false]
              [4 :fil/id #uuid "00000000-0000-0000-1111-000000000000" 2 true]
              [4 :fil/name "secret.txt" 2 false]
              [4 :fil/name "new.txt" 2 true]]
             (u/ignore-txInstant hist)))
      (is (= [[3 :m/files 4 1 true]
              [3 :m/id "id" 1 true]
              [4 :fil/id #uuid "00000000-0000-0000-0000-000000000000" 1 true]
              [4 :fil/name "deleted.txt" 1 true]
              [4 :fil/id #uuid "00000000-0000-0000-0000-000000000000" 2 false]
              [4 :fil/id #uuid "00000000-0000-0000-1111-000000000000" 2 true]
              [4 :fil/name "deleted.txt" 2 false]
              [4 :fil/name "new.txt" 2 true]]
             (u/ignore-txInstant new-hist)))
      (is (= {:add
                      [{:e "4", :a ":fil/id", :v "#uuid \"00000000-0000-0000-0000-000000000000\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"deleted.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e   "4",
                        :a   ":fil/id",
                        :v   "#uuid \"00000000-0000-0000-0000-000000000000\"",
                        :t   "2",
                        :o   "false",
                        :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"deleted.txt\"", :t "2", :o "false", :ref "[:m/id \"id\"]"}],
              :remove
                      [{:e "4", :a ":fil/id", :v "#uuid \"f3a0530b-6645-475f-b5ab-4000849fc2b9\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"secret.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e   "4",
                        :a   ":fil/id",
                        :v   "#uuid \"f3a0530b-6645-475f-b5ab-4000849fc2b9\"",
                        :t   "2",
                        :o   "false",
                        :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"secret.txt\"", :t "2", :o "false", :ref "[:m/id \"id\"]"}],
              :status "Scheduled patch"}
             (rh/schedule-patch! conn [:m/id "id"] hist new-hist)))
      (is (= {:add
                      [{:e "4", :a ":fil/id", :v "#uuid \"00000000-0000-0000-0000-000000000000\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"deleted.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e   "4",
                        :a   ":fil/id",
                        :v   "#uuid \"00000000-0000-0000-0000-000000000000\"",
                        :t   "2",
                        :o   "false",
                        :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"deleted.txt\"", :t "2", :o "false", :ref "[:m/id \"id\"]"}],
              :remove
                      [{:e "4", :a ":fil/id", :v "#uuid \"f3a0530b-6645-475f-b5ab-4000849fc2b9\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"secret.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e   "4",
                        :a   ":fil/id",
                        :v   "#uuid \"f3a0530b-6645-475f-b5ab-4000849fc2b9\"",
                        :t   "2",
                        :o   "false",
                        :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"secret.txt\"", :t "2", :o "false", :ref "[:m/id \"id\"]"}],
              :status "No changes"}
             (rh/schedule-patch! conn [:m/id "id"] hist new-hist)))
      (is (= {:add
                      [{:e "4", :a ":fil/id", :v "#uuid \"00000000-0000-0000-0000-000000000000\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"deleted.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e   "4",
                        :a   ":fil/id",
                        :v   "#uuid \"00000000-0000-0000-0000-000000000000\"",
                        :t   "2",
                        :o   "false",
                        :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"deleted.txt\"", :t "2", :o "false", :ref "[:m/id \"id\"]"}],
              :remove
                      [{:e "4", :a ":fil/id", :v "#uuid \"f3a0530b-6645-475f-b5ab-4000849fc2b9\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"secret.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e   "4",
                        :a   ":fil/id",
                        :v   "#uuid \"f3a0530b-6645-475f-b5ab-4000849fc2b9\"",
                        :t   "2",
                        :o   "false",
                        :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"secret.txt\"", :t "2", :o "false", :ref "[:m/id \"id\"]"}],
              :status "List"}
             (rh/all-pending-patches conn)))

      @(d/transact conn [{:m/id "i2" :m/files
                                {:db/id    "fil"
                                 :fil/id   #uuid"00000000-1111-2222-3333-000000000000"
                                 :fil/name "strengt-hemmeleg.txt"}}])
      (rh/schedule-patch! conn
                          [:m/id "i2"]
                          (rh/pull-flat-history conn [:m/id "i2"])
                          (rh/assoc-lookup-ref (rh/pull-flat-history conn [:m/id "i2"])
                                               [:fil/id #uuid"00000000-1111-2222-3333-000000000000"]
                                               :fil/id #uuid"00000000-0000-0000-1111-000000000000"
                                               :fil/name "deleted2.txt"))
      (is (= {:add
                      [{:e "3", :a ":fil/id", :v "#uuid \"00000000-0000-0000-1111-000000000000\"", :t "1", :o "true", :ref "[:m/id \"i2\"]"}
                       {:e "3", :a ":fil/name", :v "\"deleted2.txt\"", :t "1", :o "true", :ref "[:m/id \"i2\"]"}
                       {:e "4", :a ":fil/id", :v "#uuid \"00000000-0000-0000-0000-000000000000\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"deleted.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e   "4",
                        :a   ":fil/id",
                        :v   "#uuid \"00000000-0000-0000-0000-000000000000\"",
                        :t   "2",
                        :o   "false",
                        :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"deleted.txt\"", :t "2", :o "false", :ref "[:m/id \"id\"]"}],
              :remove
                      [{:e "3", :a ":fil/id", :v "#uuid \"00000000-1111-2222-3333-000000000000\"", :t "1", :o "true", :ref "[:m/id \"i2\"]"}
                       {:e "3", :a ":fil/name", :v "\"strengt-hemmeleg.txt\"", :t "1", :o "true", :ref "[:m/id \"i2\"]"}
                       {:e "4", :a ":fil/id", :v "#uuid \"f3a0530b-6645-475f-b5ab-4000849fc2b9\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"secret.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
                       {:e   "4",
                        :a   ":fil/id",
                        :v   "#uuid \"f3a0530b-6645-475f-b5ab-4000849fc2b9\"",
                        :t   "2",
                        :o   "false",
                        :ref "[:m/id \"id\"]"}
                       {:e "4", :a ":fil/name", :v "\"secret.txt\"", :t "2", :o "false", :ref "[:m/id \"id\"]"}],
              :status "List"}
             (rh/all-pending-patches conn)))
      (rh/rewrite-scheduled! conn)
      (is (= new-hist
             (rh/pull-flat-history conn [:m/id "id"]))))))

