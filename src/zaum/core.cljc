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

(defn process-command
  ([command]
   (perform-op (:operation command) command))
  ([connection command]
   (perform-op (:operation command) (assoc command :connection connection))))

(defn process-condition-block [clauses]
  (map (fn [clause]
         (condp #(contains? %2 %1) clause
           :or
           (let [
                 opening " ("
                 middle (process-condition-block-with-joiner (val clause) " OR ")
                 ending ") "
                 args [opening middle ending]
                 ]
             (clojure.string/join " " args))
           :and
           (let   [
                   opening " ("
                   middle (process-condition-block-with-joiner (val clause) " AND ")
                   ending ") "
                   args [opening middle ending]
                   ]
             (clojure.string/join " " args))
           (let [
                  left-side (:left-side clause "ERROR")
                  comparison (:comparison clause "ERROR")
                  right-side (:right-side clause "ERROR")
                  args [left-side comparison right-side]
                  ]
                 (clojure.string/join " " args))
         ))
          clauses
  )
)

(defn process-condition-block-with-joiner [clauses joiner]
  (map (fn [clause]
         (condp #(contains? %2 %1) clause
           :or
           (let [
                 opening " ("
                 middle (process-condition-block-with-joiner (val clause) " OR ")
                 ending ") "
                 args [opening middle ending]
                 ]
             (apply str args))
           :and
           (let   [
                   opening " ("
                   middle (process-condition-block-with-joiner (val clause) " AND ")
                   ending ") "
                   args [opening middle ending]
                   ]
             (apply str args))
           (let [
                  left-side (:left-side clause "ERROR")
                  comparison (:comparison clause "ERROR")
                  right-side (:right-side clause "ERROR")
                  args [left-side comparison right-side]
                  ]
                 (clojure.string/join " " args))
         ))
          clauses
  )
)

(defn process-where-clause [clauses]
  (apply str ["WHERE "
              (process-condition-block clauses)
              ]
         )
  )


(process-condition-block [{:left-side "this"
                           :comparison "equals"
                           :right-side "that"}])
  
