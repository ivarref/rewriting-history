(ns no.nsd.patch-test
  (:require [clojure.test :refer :all]
            [no.nsd.utils :as u]
            [no.nsd.rewriting-history :as rh]
            [datomic.api :as d]))

(deftest patch-test
  (let [conn (u/empty-conn)
        fil-uuid #uuid"f3a0530b-6645-475f-b5ab-4000849fc2b9"]
    @(d/transact conn #d/schema[[:m/id :one :string :id]
                                [:m/files :many :ref :component]
                                [:fil/id :one :uuid]
                                [:fil/name :one :string]])
    @(d/transact conn rh/schema)

    @(d/transact conn [{:m/id "id" :m/files
                              {:db/id "fil"
                               :fil/id fil-uuid
                               :fil/name "secret.txt"}}])

    (let [hist (rh/pull-flat-history conn [:m/id "id"])
          new-hist (-> hist
                       (rh/assoc-lookup-ref [:fil/id fil-uuid]
                                            :fil/id #uuid"00000000-0000-0000-0000-000000000000"
                                            :fil/name "deleted.txt"))]
      (is (= [[1 :tx/txInstant #inst "1973" 1 true]
              [2 :m/files 3 1 true]
              [2 :m/id "id" 1 true]
              [3 :fil/id fil-uuid 1 true]
              [3 :fil/name "secret.txt" 1 true]]
             hist))
      (is (= [[1 :tx/txInstant #inst "1973" 1 true]
              [2 :m/files 3 1 true]
              [2 :m/id "id" 1 true]
              [3 :fil/id #uuid"00000000-0000-0000-0000-000000000000" 1 true]
              [3 :fil/name "deleted.txt" 1 true]]
             new-hist))
      (is (= {:add
              [{:e "3", :a ":fil/id", :v "#uuid \"00000000-0000-0000-0000-000000000000\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
               {:e "3", :a ":fil/name", :v "\"deleted.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}],
              :remove
              [{:e "3", :a ":fil/id", :v "#uuid \"f3a0530b-6645-475f-b5ab-4000849fc2b9\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
               {:e "3", :a ":fil/name", :v "\"secret.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}]}
             (rh/schedule-patch! conn [:m/id "id"] hist new-hist)))
      (is (= {:add
              [{:e "3", :a ":fil/id", :v "#uuid \"00000000-0000-0000-0000-000000000000\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
               {:e "3", :a ":fil/name", :v "\"deleted.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}],
              :remove
              [{:e "3", :a ":fil/id", :v "#uuid \"f3a0530b-6645-475f-b5ab-4000849fc2b9\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
               {:e "3", :a ":fil/name", :v "\"secret.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}]}
             (rh/schedule-patch! conn [:m/id "id"] hist new-hist)))
      (is (= {:add
              [{:e "3", :a ":fil/id", :v "#uuid \"00000000-0000-0000-0000-000000000000\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
               {:e "3", :a ":fil/name", :v "\"deleted.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}],
              :remove
              [{:e "3", :a ":fil/id", :v "#uuid \"f3a0530b-6645-475f-b5ab-4000849fc2b9\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
               {:e "3", :a ":fil/name", :v "\"secret.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}]}
             (rh/all-pending-patches conn)))

      @(d/transact conn [{:m/id "i2" :m/files
                                {:db/id "fil"
                                 :fil/id #uuid"00000000-1111-2222-3333-000000000000"
                                 :fil/name "streng-hemmeleg.txt"}}])
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
               {:e "3", :a ":fil/id", :v "#uuid \"00000000-0000-0000-0000-000000000000\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
               {:e "3", :a ":fil/name", :v "\"deleted.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}],
              :remove
              [{:e "3", :a ":fil/id", :v "#uuid \"00000000-1111-2222-3333-000000000000\"", :t "1", :o "true", :ref "[:m/id \"i2\"]"}
               {:e "3", :a ":fil/name", :v "\"streng-hemmeleg.txt\"", :t "1", :o "true", :ref "[:m/id \"i2\"]"}
               {:e "3", :a ":fil/id", :v "#uuid \"f3a0530b-6645-475f-b5ab-4000849fc2b9\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}
               {:e "3", :a ":fil/name", :v "\"secret.txt\"", :t "1", :o "true", :ref "[:m/id \"id\"]"}]}
            (rh/all-pending-patches conn)))
      (rh/rewrite-scheduled! conn)
      (is (= new-hist
             (rh/pull-flat-history conn [:m/id "id"]))))))

