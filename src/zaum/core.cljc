(ns zaum.core
    (:import (java.sql Connection DriverManager PreparedStatement ResultSet Statement ResultSetMetaData)))

(defmulti prepare-connection :dbtype)

(defprotocol IZaumDatabase
  (perform-create [this command-map])
  (perform-read [this command-map])
  (perform-update [this command-map])
  (perform-delete [this command-map]))

(defmulti perform-op (fn [op-key struct] op-key))

(defmethod perform-op :default
  [op-key struct]
  (throw
   (IllegalArgumentException. (str "Unknown Operation: " op-key))))

(defn current-time
  []
  #?(:cljs (system-time)
     :clj  (System/currentTimeMillis)))

(defn wrap-op
  [the-op]
  (let [st     (current-time)
        result (try
                 (the-op)
                 (catch Throwable t
                   {:status :error
                    :data   t}))
        et     (current-time)]
    (assoc result :time (- et st))))

(defmethod perform-op :create
  [op-key {:keys [connection] :as command}]
  (let [data (wrap-op #(perform-create (:impl connection) command))]
    (assoc data
           :result  :create
           :command command
           :count   (if (= :ok (:status data)) (count (:data data)) 0))))

(defmethod perform-op :read
  [op-key {:keys [connection] :as command}]
  (let [data (wrap-op #(perform-read (:impl connection) command))]
    (assoc data
           :result  :read
           :command command
           :count   (if (= :ok (:status data)) (count (:data data)) 0))))

(defmethod perform-op :update
  [op-key {:keys [connection] :as command}]
  (let [data (wrap-op #(perform-update (:impl connection) command))]
    (assoc data
           :result  :update
           :command command
           :count   (if (= :ok (:status data)) (count (:data data)) 0))))

(defmethod perform-op :delete
  [op-key {:keys [connection] :as command}]
  (let [data (wrap-op #(perform-delete (:impl connection) command))]
    (assoc data
           :result  :delete
           :command command
           :count   (if (= :ok (:status data)) (count (:data data)) 0))))

(defn init-connection
  [connection-map]
  (assoc connection-map :impl (prepare-connection connection-map)))

#_(defn process-command
  ([command]
   (perform-op (:operation command) command))
  ([connection command]
   (perform-op (:operation command) (assoc command :connection connection))))

(declare process-sub-select)

(declare process-select-command)

(defn process-union-all [clauses]
  (let [things (map (fn [clause]
                      (process-sub-select clause)) clauses)
        wholething (clojure.string/join " UNION ALL " things)
        args ["(" wholething ")"]]
    (clojure.string/join "" args)))

(defn process-union [clauses]
  (let [things (map (fn [clause]
                      (process-sub-select clause)) clauses)
        wholething (clojure.string/join " UNION " things)
        args ["(" wholething ")"]]
    (clojure.string/join "" args)))

(defn process-left-right-inner [clause]
  (let [table-alias (:table-alias clause "")
        joiner (if (contains? clause :table-alias) "." "")
        field-name (:field-name clause "")
        value (:value clause)
        args [table-alias joiner field-name value]]
    (clojure.string/join "" args)))

(defn process-left-or-right [clause]
  (if (map? clause)
    (cond
      (contains? clause :sub-select)
      (process-sub-select (:sub-select clause))
      (and (contains? clause :quoted) (= (:quoted clause) true))
      (let [opener "'"
            middle (process-left-right-inner clause)
            closer "'"
            args [opener middle closer]]
        (clojure.string/join "" args))
      (and (contains? clause :quoted) (= (:quoted clause) false))
      (let [middle (process-left-right-inner clause)
            args [middle]]
        (clojure.string/join "" args))
      (and (contains? clause :data-type) (or (= (:data-type clause) :text)
                                             (= (:data-type clause) :date)
                                             (= (:data-type clause) :timestamp)))
      (let [opener "'"
            middle (process-left-right-inner clause)
            closer "'"
            args [opener middle closer]]
        (clojure.string/join "" args))
      :else
      (let [middle (process-left-right-inner clause)
            args [middle]]
        (clojure.string/join "" args))
      )
    clause))

(defn process-condition-block [clauses & {:keys [stringify?] :or {stringify? true}}]
  ;;(clojure.pprint/pprint clauses)
  (if (string? clauses)
    clauses
    (let [things
          (map (fn [clause]
                 ;;(clojure.pprint/pprint clause)
                 (if (string? clause)
                   (clause)
                   (condp #(contains? %2 %1) clause
                     :or
                     (let [myclause (:or clause)]
                       ;;(clojure.pprint/pprint "OR found: ")
                       (apply str ["(" (clojure.string/join " OR " (process-condition-block myclause :stringify? false)) ")"]))
                     :and
                     (let [myclause (:and clause)]
                       #_(clojure.pprint/pprint (process-condition-block myclause))
                       (apply str ["(" (clojure.string/join " AND " (process-condition-block myclause :stringify? false)) ")"])

                       )
                     (let [
                           left (process-left-or-right (:left clause "ERROR"))
                           comparison (:comparison clause "ERROR")
                           right (process-left-or-right (:right clause "ERROR"))
                           args [left comparison right]
                           ]
                       (clojure.string/join " " args))
                     )))
               clauses
               )]
                         (if stringify?
                           (apply str things)
                           things)
  ))
)

(defn process-where-clause [clauses-in]
  (if (string? clauses-in)
    (apply str ["WHERE " clauses-in])
    (let [clauses (cond
                                          (map? clauses-in)
                                          [clauses-in]
                                          :else
                                          clauses-in)]
                            (if (some? clauses) (apply str ["WHERE " (process-condition-block clauses)]) ""))))
  
(defn process-having-clause [clauses-in]
  (if (string? clauses-in)
    (apply str ["HAVING " clauses-in])
    (let [clauses (cond
                                          (map? clauses-in)
                                          [clauses-in]
                                          :else
                                          clauses-in)]
                            (if (some? clauses) (apply str ["HAVING " (process-condition-block clauses)]) ""))))

(defn process-from-clause [clauses-in]
  ;;(apply str ["FROM "
  (let [clauses (cond
                  (string? clauses-in)
                  [{:table-name clauses-in}]
                  (map? clauses-in)
                  [clauses-in]
                  :else
                  clauses-in)
        things
              (map (fn [clause]
                     (if (string? clause)
                       clause
                       (condp #(contains? %2 %1) clause
                                           :join-type
                                           (let [table-name (:table-name clause)
                                                 table-alias (:table-alias clause)
                                                 sub-select (:sub-select clause)
                                                 sub-select-text (process-select-command sub-select)
                                                 union (cond
                                                         (contains? clause :union)
                                                         (process-union (:union clause))
                                                         (contains? clause :union-all)
                                                         (process-union-all (:union-all clause))
                                                         :else nil)
                                                 join-clause (:join-clause clause)
                                                 join-clause-text (process-condition-block join-clause)
                                                 join-type (:join-type clause)
                                                 join-type-text (condp = join-type
                                                                  :inner-join
                                                                  "INNER JOIN"
                                                                  :left-outer-join
                                                                  "LEFT OUTER JOIN"
                                                                  :right-outer-join
                                                                  "RIGHT-OUTER-JOIN"
                                                                  :full-outer-join
                                                                  "FULL OUTER JOIN"
                                                                  :cross-join
                                                                  "CROSS JOIN"
                                                                  :unnamed-join
                                                                  ", "
                                                                  )
                                                 args [join-type-text
                                                       (if (contains? clause :sub-select) (process-sub-select (:sub-select clause)) table-name)
                                                       (if (contains? clause :table-alias) " ")
                                                       (if (contains? clause :table-alias) table-alias)
                                                       (if (some? join-clause-text) " ")
                                                       (if (= join-type :unnamed-join)
                                                         ""
                                                         "ON ") 
                                                       join-clause-text]
                                                 ]
                                             (clojure.string/join "" args)
                                             )
                                           (let [sub-select (:sub-select clause)
                                                 sub-select-text (process-select-command sub-select)
                                                 union (cond
                                                         (contains? clause :union)
                                                         (process-union (:union clause))
                                                         (contains? clause :union-all)
                                                         (process-union-all (:union-all clause))
                                                         :else nil)
                                                 table-name (:table-name clause)
                                                 table-alias (:table-alias clause)
                                                 args [(cond
                                                         (contains? clause :sub-select)
                                                         (process-sub-select (:sub-select clause))
                                                         (some? union)
                                                         union
                                                         :else
                                                         table-name)
                                                       (if (some? table-alias) " ")
                                                       table-alias]]
                                             (clojure.string/join "" args))
                                           ))) clauses);;]
  ;;)
        ]
    (clojure.string/join " " things))
 
  )

(defn process-field-list-select [clauses-in]
  (let [clauses (cond
                  (string? clauses-in)
                  [{:field-name clauses-in}]
                  (map? clauses-in)
                  [clauses-in]
                  :else
                  clauses-in)
        things
        (map (fn [clause]
               (if (string? clause)
                 clause
                 (let [field-name  (:field-name clause)
                                         field-alias (:field-alias clause)
                                         table-alias (:table-alias clause)
                                         args [(if (contains? clause :table-alias) (str table-alias "."))
                                               field-name
                                               (if (contains? clause :field-alias) (str " AS " field-alias))]]
                                     (clojure.string/join "" args)))) clauses)]
    (clojure.string/join ", " things)))

(defn process-field-list-insert-dest [clauses-in]
  (let [clauses (cond
                  (string? clauses-in)
                  [{:field-name clauses-in}]
                  (map? clauses-in)
                  [clauses-in]
                  :else
                  clauses-in)
        things
        (map (fn [clause]
               (if (string? clause)
                 clause
                 (let [field-name  (:field-name clause)
                                         args [field-name]]
                                     (clojure.string/join "" args)))) clauses)]
    (clojure.string/join ", " things)))

(defn process-field-list-order-by [clauses-in]
  (let [clauses (cond
                  (string? clauses-in)
                  [{:field-name clauses-in}]
                  (map? clauses-in)
                  [clauses-in]
                  :else
                  clauses-in)
        things
        (map (fn [clause]
               (if (string? clause)
                 clause
                 (let [field-name  (:field-name clause)
                                         sort-order (:sort-order clause nil)
                                         sort-order-text (condp = sort-order
                                                           :desc
                                                           "DESC"
                                                           :asc
                                                           "ASC"
                                                           "UNKNOWN SORT ORDER")
                                         args [field-name
                                               (if (some? sort-order) " ")
                                               (if (some? sort-order) sort-order-text)]]
                                     (clojure.string/join "" args)))) clauses)]
    (clojure.string/join ", " things)))

(defn process-field-list-insert-values [clauses-in]
  (let [clauses (cond
                  (map? clauses-in)
                  [clauses-in]
                  :else
                  clauses-in)
        things
        (map (fn [clause]
               (let [set-to  (if (or
                                  (nil? (:set-to clause))
                                  (= (:set-to clause) ""))
                               "NULL"
                               (process-left-or-right (:set-to clause)))
                     args [set-to]]
                 (clojure.string/join "" args))) clauses)]
    (clojure.string/join ", " things)))

(defn process-field-list-update [clauses-in]
  (let [clauses (cond
                  (map? clauses-in)
                  [clauses-in]
                  :else
                  clauses-in)
        things
        (map (fn [clause]
               (let [field-name  (:field-name clause)
                     set-to (if (or
                                 (nil? (:set-to clause))
                                 (= (:set-to clause) ""))
                              "NULL"
                              (process-left-or-right (:set-to clause)))
                     args [field-name " = " set-to]]
                 (clojure.string/join "" args))) clauses)]
    (clojure.string/join ", " things)))

(defn process-sub-select [clause]
  #_(clojure.pprint/pprint clause)
  (if (map? clause) (clojure.string/join "" ["(" (process-select-command clause) ")"]) (str clause)))

(defn process-group-by-clause [clauses]
  (if (some? clauses) (apply str ["GROUP BY " (process-field-list-insert-dest clauses)]) ""))

(defn process-order-by-clause [clauses]
  (if (some? clauses) (apply str ["ORDER BY " (process-field-list-order-by clauses)]) ""))

(defn process-select-command [clauses]
  #_(clojure.pprint/pprint clauses)
  (let [distinct (:distinct clauses false)
        field-clause (cond
                       (contains? clauses :field-clause) (:field-clause clauses)
                       (contains? clauses :field-list) (:field-list clauses)
                       :else nil)
        from-clause (cond
                      (contains? clauses :from-clause) (:from-clause clauses)
                      (contains? clauses :from) (:from clauses)
                      (contains? clauses :source) (:source clauses)
                      :else nil)
        where-clause (cond
                       (contains? clauses :where-clause) (:where-clause clauses)
                       (contains? clauses :where) (:where clauses)
                       (contains? clauses :conditions) (:conditions clauses)
                       (contains? clauses :selector) (:selector clauses)
                       :else nil)
        having-clause (cond
                        (contains? clauses :having-clause) (:having-clause clauses)
                        (contains? clauses :having) (:having clauses)
                        :else nil)
        group-by-clause (cond
                          (contains? clauses :group-by-clause) (:group-by-clause clauses)
                          (contains? clauses :group-by) (:group-by clauses)
                          :else nil)
        order-by-clause (cond
                          (contains? clauses :order-by-clause) (:order-by-clause clauses)
                          (contains? clauses :order-by) (:order-by clauses)
                          :else nil)
        command-text (if (= distinct true) "SELECT DISTINCT " "SELECT ")
        field-text (process-field-list-select field-clause)
        mid-command1 " FROM "
        from-text (process-from-clause from-clause)
        where-text (process-where-clause where-clause)
        group-by-text (process-group-by-clause group-by-clause)
        having-text (process-having-clause having-clause)
        order-by-text (process-order-by-clause order-by-clause)
        args [command-text
              field-text
              mid-command1
              from-text
              (if (some? where-clause) " " "")
              where-text
              (if (some? group-by-clause) " " "")
              group-by-text
              (if (some? having-clause) " " "")
              having-text
              (if (some? order-by-clause) " " "")
              order-by-text]]
    (clojure.string/join "" args)))

(defn process-insert-values-command [clauses]
  (let [to-clause (:to-clause clauses)
        field-clause (:field-clause clauses)
        command-text "INSERT INTO "
        dest-text (process-from-clause to-clause)
        mid-command1 " ( "
        field-text (process-field-list-insert-dest field-clause)
        mid-command2 " ) VALUES ( "
        values-text (process-field-list-insert-values field-clause)
        mid-command3 " )"
        args [command-text dest-text mid-command1 field-text mid-command2 values-text mid-command3]]
    (clojure.string/join "" args)))

(defn process-insert-select-command [clauses]
  (let [to-clause (:to-clause clauses)
        field-clause (:field-clause clauses)
        select-clause (:select-clause clauses)
        command-text "INSERT INTO "
        dest-text (process-from-clause to-clause)
        mid-command1 " ( "
        field-text (process-field-list-insert-dest field-clause)
        mid-command2 " ) "
        select-text (process-select-command select-clause)
        args [command-text dest-text mid-command1 field-text mid-command2 select-text]]
    (clojure.string/join "" args)))

(defn process-delete-command [clauses]
  (let [from-clause (:from-clause clauses)
        where-clause (:where-clause clauses nil)
        command-text "DELETE FROM "
        from-text (process-from-clause from-clause)
        where-text (process-where-clause where-clause)
        args [command-text
              from-text
              (if (some? where-clause) " " "")
              where-text]]
    (clojure.string/join "" args)))

(defn process-update-simple-command [clauses]
  (let [from-clause (:from-clause clauses)
        field-clause (:field-clause clauses)
        where-clause (:where-clause clauses nil)
        command-text "UPDATE "
        from-text (process-from-clause from-clause)
        mid-command1 " SET "
        field-text (process-field-list-update field-clause)
        where-text (process-where-clause where-clause)
        args [command-text
              from-text
              mid-command1
              field-text
              (if (some? where-clause) " " "")
              where-text]]
    (clojure.string/join "" args)))

(defn process-field-list-create-table [clauses]
  (let [things
        (map (fn [clause]
               (let [field-name  (:field-name clause)
                     data-type (:data-type clause)
                     data-size (:data-size clause)
                     nullability (:nullability clause)
                     default (:default clause)
                     args [field-name
                           " "
                           data-type
                           (if (or (= data-type :char)
                                   (= data-type :varchar)
                                   (= data-type :number))
                             "("
                             "")
                           (if (or (= data-type :char)
                                   (= data-type :varchar)
                                   (= data-type :number))
                             data-size
                             "")
                           (if (or (= data-type :char)
                                   (= data-type :varchar)
                                   (= data-type :number))
                             ")"
                             "")
                           " "
                           (if (= nullability :not-null)
                             "NOT NULL"
                             "NULL")
                           " "
                           (if (some? default)
                             "DEFAULT "
                             "")
                           (if (some? default)
                             default
                             "")]]
                 (clojure.string/join "" args))) clauses)]
    (clojure.string/join ", " things)))

(defn process-create-command [clauses]
  (let [target-type (:target-type clauses)
        into (:into clauses)
        field-list (:field-list clauses)
        sub-select (:insert-select clauses)]
    (cond = target-type
          :table
          (let [command-text "CREATE TABLE "
                table-name (:table-name into)
                mid-command1 " ( "
                field-text (process-field-list-create-table field-list)
                mid-command2 " ) "
                args [command-text table-name mid-command1 field-text mid-command2]]
            (clojure.string/join "" args))
          :view
          (let [command-text "CREATE VIEW "
                view-name (:table-name into)
                mid-command1 " AS "
                select-text (process-select-command sub-select)
                args [command-text view-name mid-command1 select-text]]
            (clojure.string/join "" args))
          :else
          "UNKNOWN TARGET-TYPE"
          )))

(defn process-drop-command [clauses]
  (let [target-type (:target-type clauses)
        into (:into clauses)
        command (cond = target-type
                      :table
                      "DROP TABLE "
                      :view
                      "DROP VIEW "
                      :else
                      "UNKNOWN TARGET-TYPE ")
        args [command into]]
    (clojure.string/join "" args)))

(defn process-transaction-start-command [clauses]
  "BEGIN TRANSACTION")

(defn process-transaction-commit-command [clauses]
  "COMMIT TRANSACTION")

(defn process-transaction-rollback-command [clauses]
  "ROLLBACK TRANSACTION")

(defn new-val [clauses]
  (let [in-operation (:operation clauses :select)
        operation (condp = in-operation
                    :read :select
                    :write :insert
                    :write-from-read :insert-from-select
                    :change-simple :update-simple
                    :change-multi :update-multi
                    :write-change :merge
                    in-operation
                    )
        fault-tolerance (:fault-tolerance clauses :strict)
        target-type (:target-type clauses :table)
        simulate (:simulate clauses :true)
        return-type (:return-type clauses :all-rows-as-map)
        field-list (:field-list clauses nil)
        from (cond
               (contains? clauses :from) (:from clauses)
               (contains? clauses :source) (:source clauses)
               :else nil)
        into (cond
               (contains? clauses :into) (:into clauses)
               (contains? clauses :destination) (:destination clauses)
               :else nil)
        insert-select (:insert-select clauses nil)
        where (cond
                (contains? clauses :where) (:where clauses)
                (contains? clauses :conditions) (:conditions clauses)
                (contains? clauses :selector) (:selector clauses)
                :else nil)
        having (:having clauses nil)
        group-by-list (:group-by clauses nil)
        order-by-list (:order-by clauses nil)]
    (conj []
          (condp = operation
            :select
            (conj []
                  {:type :info :message ":select being validated" :source clauses}
                  (if (not (vector? field-list))
                    {:type :error :message ":field-list must be a vector" :source clauses}
                    (new-val {:operation :select-field-clause :field-list field-list}))
                  (if (not (vector? from))
                    {:type :error :message ":from must be a vector" :source clauses}
                    (new-val {:operation :select-from-clause :from from})))
            :select-field-clause
            (conj []
                  {:type :info :message ":field-list inside :select being validated" :source clauses}
                  (map (fn [clause]
                         (let [table-alias (:table-alias clause)
                               field-alias (:field-alias clause)
                               field-name (:field-name clause)]
                           (conj []
                                 (if (some? table-alias)
                                   (if (not (string? table-alias))
                                     {:type :error :message ":table-alias must be a string" :source clause}
                                     {:type :info :message ":table-alias okay" :source clause})
                                   {:type :info :message ":table-alias not specified (optional)" :source clause})
                                 (if (some? field-alias)
                                   (if (not (string? field-alias))
                                     {:type :error :message ":field-alias must be a string" :source clause}
                                     {:type :info :message ":field-alias okay" :source clause})
                                   {:type :info :message ":field-alias not specified (optional)" :source clause})
                                 (if (not (some? field-name))
                                   {:type :error :message ":field-name must be present" :source clause}
                                   (if (not (string? field-name))
                                     {:type :error :message ":field-name must be a string" :source clause}
                                     {:type :info :message ":field-name okay" :source clause}))))
                         ) (:field-list clauses)))
            :select-from-clause
            (conj []
                  {:type :info :message ":from inside :select being validated" :source clauses}
                  (map (fn [clause]
                         (let [sub-select (:sub-select clause)
                               table-name (:table-name clause)
                               table-alias (:table-alias clause)
                               join-clause (:join-clause clause)
                               join-type (:join-type clause)]
                           (conj []
                                 (if (not (some? table-alias))
                                   {:type :info :message ":table-alias not specified (optional)" :source clause}
                                   (if (not (string? table-alias))
                                     {:type :error :message ":table-alias must be a string" :source clause}
                                     {:type :info :message ":table-alias okay" :source clause}))
                                 (if (not (or (some? table-name) (some? sub-select)))
                                   {:type :error :message ":table-name or :sub-select must be present" :source clause}
                                   (if (some? table-name)
                                     (if (not (string? table-name))
                                       {:type :error :message ":table-name must be a string" :source clause}
                                       {:type :info :message ":table-name okay" :source clause})
                                     (new-val (assoc sub-select :operation :select))))
                                 (if (not (some? join-type))
                                   {:type :info :message "join info not specified" :source clause}
                                   (if (or (= join-type :inner-join)
                                           (= join-type :left-outer-join)
                                           (= join-type :right-outer-join)
                                           (= join-type :full-outer-join)
                                           (= join-type :cross-join)
                                           (= join-type :unnames-join))
                                     {:type :info :message "join type valid" :source clause}
                                     {:type :error :message "join type invalid" :source clause}))
                                 (if (not (some? join-clause))
                                   {:type :info :message "join clause not specified (optional)" :source clause}
                                   (new-val (assoc join-clause :operation :condition-block)))))
                         ) (:from clauses)))
            :condition-block
            (conj []
                  (map (fn [clause]
                         (let [or (:or clause)
                               and (:and clause)
                               left (:left clause)
                               comparison (:comparison clause)
                               right (:right clause)]
                           (conj []
                                 (if (some? or)
                                   (new-val (assoc or :operation :condition-block))
                                   (if (some? and)
                                     (new-val (assoc and :operation :condition-block))
                                     (conj []
                                           (if (not (some? left))
                                             {:type :error :message ":left must be specified" :source clause}
                                             (if (not (string? left))
                                               {:type :error :message ":left must be a string" :source clause}
                                               {:type :info :message ":left okay" :source clause}))
                                           (if (not (some? comparison))
                                             {:type :error :message ":comparison must be specified" :source clause}
                                             (if (not (string? comparison))
                                               {:type :error :message ":comparison must be a string" :source clause}
                                               {:type :info :message ":comparison okay" :source clause}))
                                           (if (not (some? right))
                                             {:type :error :message ":right must be specified" :source clause}
                                             (if (not (string? right))
                                               {:type :error :message ":right must be a string" :source clause}
                                               {:type :info :message ":right okay" :source clause})))))))
                         ) clauses))
            :else
            (conj []
                  {:type :error :message "unknown operation" :source clauses})))))



#_(defn validate-command [clauses]
  (clojure.pprint/pprint clauses)
  (let [validation-errors []
        in-operation (:operation clauses :select)
        operation (condp = in-operation
                    :read :select
                    :write :insert
                    :write-from-read :insert-from-select
                    :change-simple :update-simple
                    :change-multi :update-multi
                    :write-change :merge
                    in-operation
                    )
        fault-tolerance (:fault-tolerance clauses :strict)
        target-type (:target-type clauses :table)
        simulate (:simulate clauses :true)
        return-type (:return-type clauses :all-rows-as-map)
        field-list (:field-list clauses nil)
        from (cond
               (contains? clauses :from) (:from clauses)
               (contains? clauses :source) (:source clauses)
               :else nil)
        into (cond
               (contains? clauses :into) (:into clauses)
               (contains? clauses :destination) (:destination clauses)
               :else nil)
        insert-select (:insert-select clauses nil)
        where (cond
                (contains? clauses :where) (:where clauses)
                (contains? clauses :conditions) (:conditions clauses)
                (contains? clauses :selector) (:selector clauses)
                :else nil)
        having (:having clauses nil)
        group-by-list (:group-by clauses nil)
        order-by-list (:order-by clauses nil)]
    ;(reduce
     ;(fn [validation-errors]
    (clojure.pprint/pprint field-list)
    (let [suberrors []
          val-errors (condp = operation
         :select
         (do
           (clojure.pprint/pprint "Got here")
           (if (not (vector? field-list))
             (do
               (clojure.pprint/pprint validation-errors)
               (conj validation-errors {:type :error :message "Field list must be a vector" :source field-list})
               #_(clojure.pprint/pprint "I even set validation-errors")
               #_(clojure.pprint/pprint validation-errors))
             (let [subcommand (validate-command {:operation :select-field-clause :field-clause field-list})]
               (when (not (empty? subcommand)) (conj validation-errors subcommand))))
           (if (not (vector? from))
             (conj validation-errors {:type :error :message "From clause must be a vector" :source from})
             (let [subcommand (validate-command {:operation :select-from-clause :from from})]
               (when (not (empty? subcommand)) (conj validation-errors subcommand))))
           (when (some? where)
             (if (not (vector? where))
               (conj suberrors {:type :error :message "Where clause must be a vector" :source where})
               (let [subcommand (validate-command {:operation :select-where-clause :where where})]
                 (when (not (empty? subcommand)) (conj suberrors subcommand)))))
           (when (some? having)
             (if (not (vector? having))
               (conj suberrors {:type :error :message "Having clause must be a vector" :source having})
               (let [subcommand (validate-command {:operation :select-having-clause :having having})]
                 (when (not (empty? subcommand)) (conj suberrors subcommand)))))
           (when (some? group-by-list)
             (if (not (vector? group-by-list))
               (conj suberrors {:type :error :message "Group by clause must be a vector" :source group-by-list})
               (let [subcommand (validate-command {:operation :select-group-by-clause :group-by group-by-list})]
                 (when (not (empty? subcommand)) (conj suberrors subcommand)))))
           (when (some? order-by-list)
             (if (not (vector? order-by-list))
               (conj suberrors {:type :error :message "Order by clause must be a vector" :source order-by-list})
               (let [subcommand (validate-command {:operation :select-order-by-clause :order-by order-by-list})]
                 (when (not (empty? subcommand)) (conj suberrors subcommand)))))
           ;;(conj validation-errors suberrors)
           )
         )]
       (clojure.pprint/pprint val-errors))
       ;)
     ;[])
    ))

#_(validate-command {:operation :select })

(defn substitute-parameters [^java.sql.PreparedStatement ps params]
  (doseq [[idx param] (map-indexed (fn [i a] [(inc i) a]) params)]
    (condp = (:type param)
      :bigdecimal
      (.setBigDecimal ps idx (:value param))
      :boolean
      (.setBoolean ps idx (:value param))
      :date
      (.setDate ps idx (:value param))
      :double
      (.setDouble ps idx (:value param))
      :float
      (.setFloat ps idx (:value param))
      :int
      (.setInt ps idx (:value param))
      :long
      (.setLong ps idx (:value param))
      :string
      (.setString ps idx (:value param))
      :time
      (.setTime ps idx (:value param))
      :timestamp
      (.setTimestamp ps idx (:value param))
      ))
  ps)

(defn get-prepared-statement-internal [^java.sql.Connection conn statement]
  (.prepareStatement conn statement))

(defn get-result-set [^java.sql.PreparedStatement ps]
  (.executeQuery ps))

(defn get-result-set-meta-data [^java.sql.ResultSet rs]
  (.getMetaData rs))

(defn get-result-set-meta-data-columns [^java.sql.ResultSetMetaData rsmd]
  (let [column-count (.getColumnCount rsmd)
        column-names (mapv (fn [i] (keyword (clojure.string/replace (.getColumnLabel rsmd i) " " "-"))) (range 1 (+ 1 column-count)))]
    column-names))

(defn run-prepared-statement [^java.sql.PreparedStatement ps]
  (.executeUpdate ps))

(defn get-results-vectors [^java.sql.ResultSet rs ^java.sql.ResultSetMetaData rsmd]
  (vec (for [x (repeat 0)
            :while (.next rs)]
        (mapv (fn [i] (.getObject rs i)) (range 1 (+ 1 (.getColumnCount rsmd)))))))

(defn get-first-result-vector [^java.sql.ResultSet rs ^java.sql.ResultSetMetaData rsmd]
  (first (get-results-vectors rs rsmd)))

(defn get-results-maps [^java.sql.ResultSet rs ^java.sql.ResultSetMetaData rsmd]
  (vec (for [x (repeat 0)
             :while (.next rs)]
         (into {} (mapv (fn [i] {(keyword (clojure.string/replace (.getColumnLabel rsmd i) " " "-")) (.getObject rs i)}) (range 1 (+ 1 (.getColumnCount rsmd))))))))

(defn get-first-result-map [^java.sql.ResultSet rs ^java.sql.ResultSetMetaData rsmd]
  (first (get-results-maps rs rsmd)))

(defn result-set-vector-seq
  ([^java.sql.ResultSet rs] (let [rsmd (get-result-set-meta-data rs)]
                              (result-set-vector-seq rs rsmd)))
  ([^java.sql.ResultSet rs
    ^java.sql.ResultSetMetaData rsmd] (let [next (if (.isAfterLast rs) false (.next rs))]
                                        (if next
                                          (lazy-seq (cons (mapv
                                                           (fn [i] (.getObject rs i))
                                                           (range 1 (+ 1 (.getColumnCount rsmd))))
                                                          (result-set-vector-seq rs rsmd)))
                                          nil))))

(defn say-hi []
  "Hi")

(defn temp-seq
  ([] (do
        (println "(temp-seq) called")
        (temp-seq 1)))
  ([n] (do
         (println "(temp-seq 1) called")
         (lazy-seq (cons (say-hi) (temp-seq 1))))))

(defn newtempseq []
  (repeatedly #(say-hi)))

(defn result-set-vector-seq2 [^java.sql.ResultSet rs]
  (let [rsmd (get-result-set-meta-data rs)]
    (repeatedly #(let [next (if (.isAfterLast rs) false (.next rs))]
                   (when next
                     (mapv
                      (fn [i] (.getObject rs i))
                      (range 1 (+ 1 (.getColumnCount rsmd)))))))))

(defn take-maps [amount ^java.sql.ResultSet rs]
  (let [rsmd (get-result-set-meta-data rs)]
    (cond
        (= amount :all)
        (seq (get-results-maps rs rsmd))
        (= amount :rest)
        (seq (get-results-maps rs rsmd))
        (int? amount)
        (seq (for [x (range amount)
                   :while (.next rs)]
               (into {} (mapv (fn [i] {(keyword (clojure.string/replace (.getColumnLabel rsmd i) " " "-")) (.getObject rs i)}) (range 1 (+ 1 (.getColumnCount rsmd)))))))
        :else
        (seq (for [x [1]
                   :while (.next rs)]
               (into {} (mapv (fn [i] {(keyword (clojure.string/replace (.getColumnLabel rsmd i) " " "-")) (.getObject rs i)}) (range 1 (+ 1 (.getColumnCount rsmd))))))))))

(defn take-vectors [amount ^java.sql.ResultSet rs]
  (let [rsmd (get-result-set-meta-data rs)]
    (cond
      (= amount :all)
      (seq (get-results-vectors rs rsmd))
      (= amount :rest)
      (seq (get-results-vectors rs rsmd))
      (int? amount)
      (seq (for [x (range amount)
                 :while (.next rs)]
             (mapv (fn [i] (.getObject rs i)) (range 1 (+ 1 (.getColumnCount rsmd))))))
      :else
      (seq (for [x [1]
                 :while (.next rs)]
             (mapv (fn [i] (.getObject rs i)) (range 1 (+ 1 (.getColumnCount rsmd)))))))))

#_(def con (get-connection {:connection-info {:dbtype "mysql"
                                                 :dbname "your-db-name?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8"
                                                 :host "your-db-host"
                                                 :port "3306"
                                                 :user "dev"
                                                 :password "your-password"}}))

  #_(process-command {:connection con
                      :operation :update-simple
                      :return-type :all-rows-as-maps
                      :into "bob_dummy"
                      :field-list [{:field-name "name" :set-to "'D'||name"}]
                      :where [{:left "name"
                               :comparison "like"
                               :right "'D%'"}]})

(defn process-command [clauses]
  (let [start-timestamp (System/currentTimeMillis)
        ps-in (:prepared-statement clauses nil)
        in-operation (:operation clauses (if (map? ps-in) (:operation (:statement ps-in)) :select))
        operation (condp = in-operation
                    :read :select
                    :write :insert
                    :write-from-read :insert-from-select
                    :change-simple :update-simple
                    :change-multi :update-multi
                    :write-change :merge
                    in-operation
                    )
        distinct (:distinct clauses (if (map? ps-in) (:distinct (:statement ps-in)) false))
        connection (:connection clauses (if (map? ps-in) (:connection (:statement ps-in)) nil))
        fault-tolerance (:fault-tolerance clauses (if (map? ps-in) (:fault-tolerance (:statement ps-in)) :strict))
        target-type (:target-type clauses (if (map? ps-in) (:target-type (:statement ps-in)) :table))
        simulate (:simulate clauses :true)
        return-type (:return-type clauses :all-rows-as-vectors)
        field-list (:field-list clauses (if (map? ps-in) (:field-list (:statement ps-in)) nil))
        from (cond
               (contains? clauses :from) (:from clauses)
               (contains? clauses :source) (:source clauses)
               :else (if (map? ps-in) (cond
                                        (contains? (:statement ps-in) :from) (:from (:statement ps-in))
                                        (contains? (:statement ps-in) :source) (:source (:statement ps-in))
                                        :else nil) nil))
        into (cond
               (contains? clauses :into) (:into clauses)
               (contains? clauses :destination) (:destination clauses)
               :else (if (map? ps-in) (cond
                                        (contains? (:statement ps-in) :from) (:into (:statement ps-in))
                                        (contains? (:statement ps-in) :source) (:destination (:statement ps-in))
                                        :else nil) nil))
        insert-select (:insert-select clauses (if (map? ps-in) (:insert-select (:statement ps-in)) nil))
        where (cond
                (contains? clauses :where) (:where clauses)
                (contains? clauses :conditions) (:conditions clauses)
                (contains? clauses :selector) (:selector clauses)
                :else (if (map? ps-in) (cond
                                         (contains? (:statement ps-in) :where) (:where (:statement ps-in))
                                         (contains? (:statement ps-in) :conditions) (:conditions (:statement ps-in))
                                         (contains? (:statement ps-in) :selector) (:selector (:statement ps-in))
                                         :else nil) nil))
        having (:having clauses (if (map? ps-in) (:having (:statement ps-in)) nil))
        group-by-list (:group-by clauses (if (map? ps-in) (:group-by (:statement ps-in)) nil))
        order-by-list (:order-by clauses (if (map? ps-in) (:order-by (:statement ps-in)) nil))
        actual-sql (if (some? ps-in)
                     (if (map? ps-in) (:actual-sql ps-in) nil)
                     (condp = operation
                       :select
                       (process-select-command {:distinct distinct
                                                :field-clause field-list
                                                :from-clause from
                                                :where-clause where
                                                :having-clause having
                                                :group-by-clause group-by-list
                                                :order-by-clause order-by-list})
                       :insert
                       (process-insert-values-command {:to-clause into
                                                       :field-clause field-list})
                       :insert-from-select
                       (process-insert-select-command {:to-clause into
                                                       :field-clause field-list
                                                       :select-clause insert-select})
                       :update-simple
                       (process-update-simple-command {:from-clause into
                                                       :field-clause field-list
                                                       :where-clause where})
                       :delete
                       (process-delete-command {:from-clause from
                                                :where-clause where})
                       :drop
                       (process-drop-command {:target-type target-type
                                              :into into})
                       :create
                       (process-create-command {:target-type target-type
                                                :into into
                                                :field-list field-list
                                                :insert-select insert-select})
                       :transaction-start
                       (process-transaction-start-command nil)
                       :transaction-commit
                       (process-transaction-commit-command nil)
                       :transaction-rollback
                       (process-transaction-rollback-command nil)
                     "NOT YET IMPLEMENTED"))
        simulate-sql (if (some? ps-in)
                       nil
                       (condp = operation
                          :select
                          (process-select-command {:field-clause [{:field-name "count(0)"}]
                                                   :from-clause from
                                                   :where-clause where
                                                   :having-clause having
                                                   :group-by-clause group-by-list
                                                   :order-by-clause order-by-list})
                          :insert
                          "NOT VALID"
                          :insert-from-select
                          (process-select-command {:field-clause [{:field-name "count(0)"}]
                                                   :from-clause (:from insert-select)
                                                   :where-clause (:where insert-select nil)
                                                   :having-clause (:having insert-select nil)
                                                   :group-by-clause (:group-by insert-select nil)
                                                   :order-by-clause (:order-by insert-select nil)})
                          :update-simple
                          (process-select-command {:from-clause into
                                                   :field-clause [{:field-name "count(0)"}]
                                                   :where-clause where})
                          :delete
                          (process-select-command {:from-clause from
                                                   :field-clause [{:field-name "count(0)"}]
                                                   :where-clause where})
                          "NOT YET IMPLEMENTED"))
        replacement-values (:replacement-values clauses nil)
        ps (if (some? ps-in)
             (if (some? replacement-values)
               (substitute-parameters (if (map? ps-in) (:prepared-statement ps-in) ps-in) replacement-values)
               (if (map? ps-in) (:prepared-statement ps-in) ps-in))
             (if (some? replacement-values)
                              (substitute-parameters (get-prepared-statement-internal connection actual-sql) replacement-values)
                              (get-prepared-statement-internal connection actual-sql)))
        
        rs (if (= operation :select) (get-result-set ps) (run-prepared-statement ps))
        rsmd (if (= operation :select) (get-result-set-meta-data rs) nil)
        result-set (cond
                     (= operation :select)
                     (cond
                       (= return-type :all-rows-as-maps)
                       (get-results-maps rs rsmd)
                       (= return-type :first-row-as-map)
                       (get-first-result-map rs rsmd)
                       (= return-type :first-row-as-vector)
                       (get-first-result-vector rs rsmd)
                       (= return-type :result-set)
                       rs
                       :else
                       (get-results-vectors rs rsmd))
                     :else nil)
        end-timestamp (System/currentTimeMillis)
        return-map {:result-set result-set
                    :count (if (= operation :select)
                             (if (= return-type :result-set)
                               1
                               (count result-set))
                             rs)
                    :command clauses
                    :db-command actual-sql
                    :simulate-db-command simulate-sql
                    :statistics {:duration (- end-timestamp start-timestamp)
                                 :start-time start-timestamp
                                 :end-time end-timestamp}
                    :result-headers (if (= operation :select)
                                      (get-result-set-meta-data-columns rsmd)
                                      nil)
                    :status "PUT :error :ok OR :warning HERE"
                    :status-source "PUT :db OR :internal HERE"
                    :status-message "PUT STATUS MESSAGE HERE"
                    :status-return-code "PUT STATUS RETURN CODE HERE"
                    }]
    (if (and (= operation :select) (not (= return-type :result-set)))
      (.close rs))
    (if (not (some? ps-in)) (.close ps))
    return-map))

(defn get-prepared-statement [clauses]
  (let [in-operation (:operation clauses :select)
        operation (condp = in-operation
                    :read :select
                    :write :insert
                    :write-from-read :insert-from-select
                    :change-simple :update-simple
                    :change-multi :update-multi
                    :write-change :merge
                    in-operation
                    )
        distinct (:distinct clauses false)
        connection (:connection clauses)
        fault-tolerance (:fault-tolerance clauses :strict)
        target-type (:target-type clauses :table)
        field-list (:field-list clauses nil)
        from (cond
               (contains? clauses :from) (:from clauses)
               (contains? clauses :source) (:source clauses)
               :else nil)
        into (cond
               (contains? clauses :into) (:into clauses)
               (contains? clauses :destination) (:destination clauses)
               :else nil)
        insert-select (:insert-select clauses nil)
        where (cond
                (contains? clauses :where) (:where clauses)
                (contains? clauses :conditions) (:conditions clauses)
                (contains? clauses :selector) (:selector clauses)
                :else nil)
        having (:having clauses nil)
        group-by-list (:group-by clauses nil)
        order-by-list (:order-by clauses nil)
        actual-sql (condp = operation
                     :select
                     (process-select-command {:distinct distinct
                                              :field-clause field-list
                                              :from-clause from
                                              :where-clause where
                                              :having-clause having
                                              :group-by-clause group-by-list
                                              :order-by-clause order-by-list})
                     :insert
                     (process-insert-values-command {:to-clause into
                                                     :field-clause field-list})
                     :insert-from-select
                     (process-insert-select-command {:to-clause into
                                                     :field-clause field-list
                                                     :select-clause insert-select})
                     :update-simple
                     (process-update-simple-command {:from-clause into
                                                     :field-clause field-list
                                                     :where-clause where})
                     :delete
                     (process-delete-command {:from-clause from
                                              :where-clause where})
                     :drop
                     (process-drop-command {:target-type target-type
                                            :into into})
                     :create
                     (process-create-command {:target-type target-type
                                              :into into
                                              :field-list field-list
                                              :insert-select insert-select})
                     :transaction-start
                     (process-transaction-start-command nil)
                     :transaction-commit
                     (process-transaction-commit-command nil)
                     :transaction-rollback
                     (process-transaction-rollback-command nil)
                     "NOT YET IMPLEMENTED")
        ps {:statement clauses
            :actual-sql actual-sql
            :prepared-statement (get-prepared-statement-internal connection actual-sql)}]
    ps))

(defn get-connection [connection-info]
  (let [dbspec (:connection-info connection-info)
        dbtype (:dbtype dbspec "mysql")
        dbname (:dbname dbspec)
        host (:host dbspec)
        port (:port dbspec "3306")
        user (:user dbspec)
        password (:password dbspec)
        classname "com.mysql.jdbc.Driver"
        args ["jdbc:"
              dbtype
              "://"
              host
              ":"
              port
              "/"
              dbname]
        connect-string (clojure.string/join "" args)]
    (clojure.lang.RT/loadClassForName classname)
    (DriverManager/getConnection connect-string user password)))

(defn close-connection [connection]
  (.close connection))

(defn close-result-set [rs]
  (.close rs))

(defn close-prepared-statement [ps]
  (if (map? ps)
    (.close (:prepared-statement ps))
    (.close ps)))

#_(def con (get-connection {:connection-info {:dbtype "mysql"
                                              :dbname "your-db-name?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8"
                                              :host "your-db-host"
                                              :port "3306"
                                              :user "dev"
                                              :password "your-password"}}))

#_(process-command
 {:operation :insert
  :connection con
  :field-list [{:field-name "name"
                :set-to "?"}]
  :into [{:table-name "bob_dummy"}]
  :replacement-values [{:type :string
                        :value "Danny"}]})

#_(process-command
 {:operation :select
  :replacement-values [{:type :string :value "First Thing Here"}
                       {:type :string :value "Second Thing Here"}]
  :connection "PUT YOUR CONNECTION OBJECT HERE"
  :field-list "*"
  :from {:union [
    {:field-list
     [{:field-name "count(action)", :field-alias "count"}
      {:field-name "action"}
      {:field-name "activity_id"}
      {:field-name "1", :field-alias "registered"}],
     :from
     [{:sub-select
       {:distinct true,
        :field-list
        [{:field-name "context_user-id"}
         {:field-name "action"}
         {:field-name "activity_id"}],
        :from "participation_log",
        :where
        [{:and
          [{:left "space_id", :comparison "=", :right "?"}
           {:left "status", :comparison "=", :right "1"}
           {:left "context_user_id", :comparison ">", :right "0"}
           {:left "action",
            :comparison "in",
            :right
            {:sub-select
             {:field-list "id",
              :from "participation_log_type",
              :where
              [{:left "action",
                :comparison "in",
                :right
                "('action-tag-rendered','action-combine-rendered','action-vote-rendered','action-rate-rendered','action-prioritise-rendered','action-actions-rendered')"}]}}}]}]}}],
    :group-by [{:field-name "action"} {:field-name "activity_id"}]}
   {:field-list
     [{:field-name "count(action)", :field-alias "count"}
      {:field-name "action"}
      {:field-name "activity_id"}
      {:field-name "0", :field-alias "registered"}],
     :from
     [{:sub-select
       {:distinct true,
        :field-list
        [{:field-name "context_user-id"}
         {:field-name "action"}
         {:field-name "activity_id"}],
        :from "participation_log",
        :where
        [{:and
          [{:left "space_id", :comparison "=", :right "?"}
           {:left "status", :comparison "=", :right "1"}
           {:left "context_user_id", :comparison "<", :right "0"}
           {:left "action",
            :comparison "in",
            :right
            {:sub-select
             {:field-list "id",
              :from "participation_log_type",
              :where
              [{:left "action",
                :comparison "in",
                :right
                "('action-tag-rendered','action-combine-rendered','action-vote-rendered','action-rate-rendered','action-prioritise-rendered','action-actions-rendered')"}]}}}]}]}}],
     :group-by
    [{:field-name "action"} {:field-name "activity_id"}]}]}})

#_(process-command
 {:operation :select
  :connection con
  :replacement-values [{:type :string :value "space_id"}]
  :field-list [{:field-name "count(action)" :field-alias "count"}
               {:field-name "action"}
               {:field-name "activity_id"}
               {:field-name "registered"}]
  :from {:table-alias "derived_1"
         :sub-select {:distinct true
                      :field-list [{:field-name "context_user_id"}
                                   {:field-name "action"}
                                   {:field-name "activity_id"}
                                   {:field-name "case when context_user_id > 0 then 1 else 0 end" :field-alias "registered"}]
                      :from "participation_log"
                      :where [{:and [{:left "space_id"
                                      :comparison "="
                                      :right "?"}
                                     {:left "status"
                                      :comparison "="
                                      :right "1"}
                                     {:left "action"
                                      :comparison "between"
                                      :right "11 and 17"}]}]}}
  :group-by [{:field-name "action"}
             {:field-name "activity_id"}
             {:field-name "registered"}]})



#_(process-command
 {:operation :select
  :field-list [{:field-name "*"}]
  :from [:union {
   {:field-list
     [{:field-name "count(action)", :field-alias "count"}
      {:field-name "action"}
      {:field-name "activity_id"}
      {:field-name "1", :field-alias "registered"}],
     :from
     [{:sub-select
       {:distinct true,
        :field-list
        [{:field-name "context_user-id"}
         {:field-name "action"}
         {:field-name "activity_id"}],
        :from [{:table-name "participation_log"}],
        :where
        [{:and
          [{:left "space_id", :comparison "=", :right "?"}
           {:left "status", :comparison "=", :right "1"}
           {:left "context_user_id", :comparison ">", :right "0"}
           {:left "action",
            :comparison "in",
            :right
            {:sub-select
             {:field-list [{:field-name "id"}],
              :from [{:table-name "participation_log_type"}],
              :where
              [{:left "action",
                :comparison "in",
                :right
                "('action-tag-rendered','action-combine-rendered','action-vote-rendered','action-rate-rendered','action-prioritise-rendered','action-actions-rendered')"}]}}}]}]}}],
    :group-by [{:field-name "action"} {:field-name "activity_id"}]}
   {:field-list
     [{:field-name "count(action)", :field-alias "count"}
      {:field-name "action"}
      {:field-name "activity_id"}
      {:field-name "0", :field-alias "registered"}],
     :from
     [{:sub-select
       {:distinct true,
        :field-list
        [{:field-name "context_user-id"}
         {:field-name "action"}
         {:field-name "activity_id"}],
        :from [{:table-name "participation_log"}],
        :where
        [{:and
          [{:left "space_id", :comparison "=", :right "?"}
           {:left "status", :comparison "=", :right "1"}
           {:left "context_user_id", :comparison "<", :right "0"}
           {:left "action",
            :comparison "in",
            :right
            {:sub-select
             {:field-list [{:field-name "id"}],
              :from [{:table-name "participation_log_type"}],
              :where
              [{:left "action",
                :comparison "in",
                :right
                "('action-tag-rendered','action-combine-rendered','action-vote-rendered','action-rate-rendered','action-prioritise-rendered','action-actions-rendered')"}]}}}]}]}}],
     :group-by
     [{:field-name "action"} {:field-name "activity_id"}]}}]})
   

#_(process-command
    {:operation :select,
  :field-list [{:field-name "*"}],
  :from
  [{:sub-select
    {:field-list
     [{:field-name "count(action)", :field-alias "count"}
      {:field-name "action"}
      {:field-name "activity_id"}
      {:field-name "1", :field-alias "registered"}],
     :from
     [{:sub-select
       {:distinct true,
        :field-list
        [{:field-name "context_user-id"}
         {:field-name "action"}
         {:field-name "activity_id"}],
        :from [{:table-name "participation_log"}],
        :where
        [{:and
          [{:left "space_id", :comparison "=", :right "?"}
           {:left "status", :comparison "=", :right "1"}
           {:left "context_user_id", :comparison ">", :right "0"}
           {:left "action",
            :comparison "in",
            :right
            {:sub-select
             {:field-list [{:field-name "id"}],
              :from [{:table-name "participation_log_type"}],
              :where
              [{:left "action",
                :comparison "in",
                :right
                "('action-tag-rendered','action-combine-rendered','action-vote-rendered','action-rate-rendered','action-prioritise-rendered','action-actions-rendered')"}]}}}]}]}}],
     :group-by [{:field-name "action"} {:field-name "activity_id"}]}}
   {:join-type :union,
    :sub-select
    {:field-list
     [{:field-name "count(action)", :field-alias "count"}
      {:field-name "action"}
      {:field-name "activity_id"}
      {:field-name "0", :field-alias "registered"}],
     :from
     [{:sub-select
       {:distinct true,
        :field-list
        [{:field-name "context_user-id"}
         {:field-name "action"}
         {:field-name "activity_id"}],
        :from [{:table-name "participation_log"}],
        :where
        [{:and
          [{:left "space_id", :comparison "=", :right "?"}
           {:left "status", :comparison "=", :right "1"}
           {:left "context_user_id", :comparison "<", :right "0"}
           {:left "action",
            :comparison "in",
            :right
            {:sub-select
             {:field-list [{:field-name "id"}],
              :from [{:table-name "participation_log_type"}],
              :where
              [{:left "action",
                :comparison "in",
                :right
                "('action-tag-rendered','action-combine-rendered','action-vote-rendered','action-rate-rendered','action-prioritise-rendered','action-actions-rendered')"}]}}}]}]}}],
     :group-by
     [{:field-name "action"} {:field-name "activity_id"}]}}]})



#_{:operation :write,
 :connection conn
 :replacement-values ["Nick"]
 :into [{:table-name "bob_dummy"}],
 :field-list
 [{:field-name "name", :set-to "?"}]}

#_{:operation :write,
 :connection conn,
 :to :contacts__contacts,
 :field-list
 [{:field-name "user_id", :set-to "?"}
  {:field-name "email", :set-to "?"}
  {:field-name "first_name", :set-to "?"}
  {:field-name "last_name", :set-to "?"}
  {:field-name "industry", :set-to "?"}
  {:field-name "status", :set-to "?"}
  {:field-name "company", :set-to "?"}]
 :replacement-values [8952 "giddyup@powernoodle.com" "" "" "" 1 ""]}

#_(process-command {:operation :write,
                  :connection conn
                  :into [{:table-name "bob_dummy"}],
                  :field-list
                  [{:field-name "name", :set-to "?"}]
                  :replacement-values ["Nick"]})

#_(def dbspec {:connection-info {:dbtype "mysql"
                                 :dbname "your-db-name?zeroDateTimeBehavior=convertToNull&useUnicode=true&characterEncoding=UTF-8"
                                 :host "your-db-host"
                                 :port "3306"
                                 :user "dev"
                                 :password "your-password"}})

#_(def ds (next.jdbc/get-datasource dbspec))

#_(next.jdbc/execute! ds "insert into bob_dummy (name) values ('Kyle')")

#_(process-command {:operation :update-simple
                  :field-list [{:field-name "field1" :set-to "blah"}]
                  :into [{:table-name "table1"}]})

#_(process-select-command {:field-clause [{:field-name "field1" :field-alias "field1" :table-alias "t1"}
                                        {:field-name "field2" :field-alias "field2" :table-alias "t1"}
                                        {:field-name "field3" :field-alias "field3" :table-alias "t1"}]
                         :from-clause [{:table-name "table2" :table-alias "t1"}]
                         :where-clause [{:left "t1.field4" :comparison "=" :right "'Manager'"}]
                         :group-by-clause [{:field-name "field1"}
                                           {:field-name "field2"}]
                         :having-clause [{:left "t1.field1" :comparison "=" :right "1"}]
                         :order-by-clause [{:field-name "field1"}
                                           {:field-name "field2" :sort-order :desc}
                                           {:field-name "field3"}]})

#_(process-update-simple-command {:from-clause [{:table-name "table1"}]
                                :field-clause [{:field-name "field1" :set-to "'Manager'"}
                                               {:field-name "field2" :set-to "12345"}
                                               {:field-name "field3" :set-to "current_timestamp"}]
                                :where-clause [{:and [{:left "field4"
                                                       :comparison "NOT IN"
                                                       :right {:sub-select {:field-clause [{:field-name "*"}]
                                                                                 :from-clause [{:table-name "table5"}]
                                                                                 :where-clause [{:left "this"
                                                                                                 :comparison "="
                                                                                                 :right "that"}]}}}
                                                      {:left "field5"
                                                       :comparison "IN"
                                                       :right "('A','B','C')"}]}]})

#_(process-insert-values-command {:to-clause [{:table-name "table1"}]
                                :field-clause [{:field-name "field1" :set-to " "}]})

#_(process-delete-command {:from-clause [{:table-name "table1"}]
                         :where-clause [{:and [{:left "field1"
                                                :comparison "="
                                                :right "'Manager'"}
                                               {:left "field2"
                                                :comparison "="
                                                :right "12345"}]}]})

#_(process-insert-select-command {:to-clause [{:table-name "table1"}]
                               :field-clause [{:field-name "field1"}
                                              {:field-name "field2"}
                                              {:field-name "field3"}]
                               :select-clause {:field-clause [{:field-name "field1" :field-alias "field1" :table-alias "t1"}
                                                              {:field-name "field2" :field-alias "field2" :table-alias "t1"}
                                                              {:field-name "field3" :field-alias "field3" :table-alias "t1"}]
                                               :from-clause [{:table-name "table2" :table-alias "t1"}]
                                               :where-clause [{:left "t1.field4" :comparison "=" :right "'Manager'"}]}})

#_(process-select-command [{:field-name "field1" :field-alias "field1" :table-alias "t1"}
                         {:field-name "field2" :field-alias "field2" :table-alias "t1"}
                         {:field-name "field3" :field-alias "field3" :table-alias "t1"}]
                        [{:table-name "table2" :table-alias "t1"}]
                        [{:left "t1.field4" :comparison "=" :right "'Manager'"}])

