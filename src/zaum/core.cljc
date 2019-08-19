(ns zaum.core)

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

(defn process-condition-block [clauses & {:keys [stringify?] :or {stringify? true}}]
  ;;(clojure.pprint/pprint clauses)
  (let [things
  (map (fn [clause]
         ;;(clojure.pprint/pprint clause)
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
                  left (process-sub-select (:left clause "ERROR"))
                  comparison (:comparison clause "ERROR")
                  right (process-sub-select (:right clause "ERROR"))
                  args [left comparison right]
                  ]
                 (clojure.string/join " " args))
         ))
          clauses
          )]
    (if stringify?
      (apply str things)
      things)
    
  
    )
)

(defn process-where-clause [clauses]
  (if (some? clauses) (apply str ["WHERE " (process-condition-block clauses)]) ""))
  
(defn process-having-clause [clauses]
  (if (some? clauses) (apply str ["HAVING " (process-condition-block clauses)]) ""))

(defn process-from-clause [clauses]
  ;;(apply str ["FROM "
  (let [things
              (map (fn [clause]
                     (condp #(contains? %2 %1) clause
                       :join-type
                       (let [table-name (:table-name clause)
                             table-alias (:table-alias clause)
                             sub-select (:sub-select clause)
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
                                   (if (contains? clause :sub-select) sub-select table-name)
                                   (if (contains? clause :table-alias) table-alias)
                                   "ON"
                                   join-clause-text]
                             ]
                             (clojure.string/join " " args)
                             )
                       (let [sub-select (:sub-select clause)
                             table-name (:table-name clause)
                             table-alias (:table-alias clause)
                             args [(if (contains? clause :sub-select) sub-select table-name) table-alias]]
                         (clojure.string/join " " args))
                     )) clauses);;]
  ;;)
        ]
    (clojure.string/join " " things))
 
  )

(defn process-field-list-select [clauses]
  (let [things
        (map (fn [clause]
               (let [field-name  (:field-name clause)
                     field-alias (:field-alias clause)
                     table-alias (:table-alias clause)
                     args [(if (contains? clause :table-alias) (str table-alias "."))
                           field-name
                           (if (contains? clause :field-alias) (str " AS " field-alias))]]
                 (clojure.string/join "" args))) clauses)]
    (clojure.string/join ", " things)))

(defn process-field-list-insert-dest [clauses]
  (let [things
        (map (fn [clause]
               (let [field-name  (:field-name clause)
                     args [field-name]]
                 (clojure.string/join "" args))) clauses)]
    (clojure.string/join ", " things)))

(defn process-field-list-order-by [clauses]
  (let [things
        (map (fn [clause]
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
                 (clojure.string/join "" args))) clauses)]
    (clojure.string/join ", " things)))

(defn process-field-list-insert-values [clauses]
  (let [things
        (map (fn [clause]
               (let [set-to  (:set-to clause)
                     args [set-to]]
                 (clojure.string/join "" args))) clauses)]
    (clojure.string/join ", " things)))

(defn process-field-list-update [clauses]
  (let [things
        (map (fn [clause]
               (let [field-name  (:field-name clause)
                     set-to (:set-to clause)
                     args [field-name " = " set-to]]
                 (clojure.string/join "" args))) clauses)]
    (clojure.string/join ", " things)))

(defn process-sub-select [clause]
  #_(clojure.pprint/pprint clause)
  (if (map? clause) (clojure.string/join "" ["(" (process-select-command (:sub-select clause)) ")"]) (str clause)))

(defn process-group-by-clause [clauses]
  (if (some? clauses) (apply str ["GROUP BY " (process-field-list-insert-dest clauses)]) ""))

(defn process-order-by-clause [clauses]
  (if (some? clauses) (apply str ["ORDER BY " (process-field-list-order-by clauses)]) ""))

(defn process-select-command [clauses]
  #_(clojure.pprint/pprint clauses)
  (let [field-clause (:field-clause clauses)
        from-clause (:from-clause clauses)
        where-clause (:where-clause clauses nil)
        having-clause (:having-clause clauses nil)
        group-by-clause (:group-by-clause clauses nil)
        order-by-clause (:order-by-clause clauses nil)
        command-text "SELECT"
        field-text (process-field-list-select field-clause)
        mid-command1 "FROM"
        from-text (process-from-clause from-clause)
        where-text (process-where-clause where-clause)
        group-by-text (process-group-by-clause group-by-clause)
        having-text (process-having-clause having-clause)
        order-by-text (process-order-by-clause order-by-clause)
        args [command-text field-text "FROM" from-text where-text group-by-text having-text order-by-text]]
    (clojure.string/join " " args)))

(defn process-insert-values-command [clauses]
  (let [to-clause (:to-clause clauses)
        field-clause (:field-clause clauses)
        command-text "INSERT INTO"
        dest-text (process-from-clause to-clause)
        mid-command1 "("
        field-text (process-field-list-insert-dest field-clause)
        mid-command2 ") VALUES ("
        values-text (process-field-list-insert-values field-clause)
        mid-command3 ")"
        args [command-text dest-text mid-command1 field-text mid-command2 values-text mid-command3]]
    (clojure.string/join " " args)))

(defn process-insert-select-command [clauses]
  (let [to-clause (:to-clause clauses)
        field-clause (:field-clause clauses)
        select-clause (:select-clause clauses)
        command-text "INSERT INTO"
        dest-text (process-from-clause to-clause)
        mid-command1 "("
        field-text (process-field-list-insert-dest field-clause)
        mid-command2 ")"
        select-text (process-select-command select-clause)
        args [command-text dest-text mid-command1 field-text mid-command2 select-text]]
    (clojure.string/join " " args)))

(defn process-delete-command [clauses]
  (let [from-clause (:from-clause clauses)
        where-clause (:where-clause clauses nil)
        command-text "DELETE FROM"
        from-text (process-from-clause from-clause)
        where-text (process-where-clause where-clause)
        args [command-text from-text where-text]]
    (clojure.string/join " " args)))

(defn process-update-simple-command [clauses]
  (let [from-clause (:from-clause clauses)
        field-clause (:field-clause clauses)
        where-clause (:where-clause clauses nil)
        command-text "UPDATE"
        from-text (process-from-clause from-clause)
        mid-command1 "SET"
        field-text (process-field-list-update field-clause)
        where-text (process-where-clause where-clause)
        args [command-text from-text mid-command1 field-text where-text]]
    (clojure.string/join " " args)))

(defn validate-command [clauses]
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
    (reduce
     (fn [validation-errors]
       (condp = operation
         :select
         (let [suberrors []]
           (when (some? field-list) (let [subcommand (validate-command {:operation :select-field-clause
                                                                        :field-clause field-list})]
                                      (if (not (empty? subcommand))
                                        (conj suberrors subcommand))))
           (when (some? from) (let [subcommand (validate-command {:operation :select-field-clause
                                                                  :field-clause field-list})]
                                (if (not (empty? subcommand))
                                  (conj suberrors subcommand))))
           (when (some? where) (let [subcommand (validate-command {:operation :select-field-clause
                                                                   :field-clause field-list})]
                                 (if (not (empty? subcommand))
                                   (conj suberrors subcommand))))
           (when (some? having) (let [subcommand (validate-command {:operation :select-field-clause
                                                                    :field-clause field-list})]
                                  (if (not (empty? subcommand))
                                    (conj suberrors subcommand))))
           (when (some? group-by-list) (let [subcommand (validate-command {:operation :select-field-clause
                                                                           :field-clause field-list})]
                                         (if (not (empty? subcommand))
                                           (conj suberrors subcommand))))
           (when (some? order-by-list) (let [subcommand (validate-command {:operation :select-field-clause
                                                                           :field-clause field-list})]
                                         (if (not (empty? subcommand))
                                           (conj suberrors subcommand))))
           )
         :select-field-clause
         (fn [] ((when (not (vector? field-list))
                   (print "SELECT field clause must be a vector: ")
                   (clojure.pprint/pprint field-list))
                 (map (fn [clause]
                        (let [field-name (:field-name clause)
                              field-alias (:field-alias clause)
                              table-alias (:table-alias clause)]
                          (when (some? field-alias) (when (not (string? field-alias))
                                                      (print "SELECT field alias must be a string: ")
                                                      (clojure.pprint/pprint clause)))
                          (when (some? table-alias) (when (not (string? table-alias))
                                                      (print "SELECT table alias must be a string: ")
                                                      (clojure.pprint/pprint clause)))
                          (if (some? field-name)
                            (when (not (string? field-name))
                              (print "SELECT field name must be a string: ")
                              (clojure.pprint/pprint clause))
                            (fn [] ((print "SELECT :field-name must exist")
                                    (clojure.pprint/pprint clause)))))))))
         :insert
         (fn [] ((when (some? into) (validate-command {:operation :to-clause
                                                             :into into}))
                 (when (some? field-list) (validate-command {:operation :field-clause
                                                             :field-list field-list}))))
         ))
     [])))

(defn process-command [clauses]
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
        order-by-list (:order-by clauses nil)
        actual-sql (condp = operation
                      :select
                      (process-select-command {:field-clause field-list
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
                      "NOT YET IMPLEMENTED")
        simulate-sql (condp = operation
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
                      "NOT YET IMPLEMENTED")]
    {:count "PUT COUNT HERE"
     :command clauses
     :db-command actual-sql
     :simulate-db-command simulate-sql
     :statistics {:db-duration "PUT TIME SPENT IN DB HERE"
                  :total-duration "PUT TOTAL TIME PROCESSING COMMAND HERE"
                  :start-time "PUT START TIME OF COMMAND HERE"
                  :end-time "PUT END TIME OF COMMAND HERE"}
     :result-set "PUT RESULT SET HERE"
     :result-headers "PUT HEADERS FROM RESULT SET HERE"
     :status "PUT :error :ok OR :warning HERE"
     :status-source "PUT :db OR :internal HERE"
     :status-message "PUT STATUS MESSAGE HERE"
     :status-return-code "PUT STATUS RETURN CODE HERE"
     }))

(process-command {:operation :update-simple
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
