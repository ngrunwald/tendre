(ns tendre.core
  (:require [clojure.edn :as edn])
  (:import [jetbrains.exodus.env
            Environment Environments
            Transaction Cursor
            TransactionalExecutable TransactionalComputable
            StoreConfig Store]
           [jetbrains.exodus.bindings
            StringBinding LongBinding DoubleBinding]
           [jetbrains.exodus ByteIterable]))


(defn open-environment
  [path]
  (Environments/newInstance path))

(def store-config
  {:with-duplicates-with-prefixing    StoreConfig/WITH_DUPLICATES_WITH_PREFIXING
   :without-duplicates-with-prefixing StoreConfig/WITHOUT_DUPLICATES_WITH_PREFIXING
   :with-duplicates                   StoreConfig/WITH_DUPLICATES
   :without-duplicates                StoreConfig/WITHOUT_DUPLICATES
   :use-existing                      StoreConfig/USE_EXISTING})

(def default-store-config (:without-duplicates-with-prefixing store-config))

(defn begin-transaction
  ^Transaction
  [^Environment env]
  (.beginTransaction env))

(defn begin-read-only-transaction
  ^Transaction
  [^Environment env]
  (.beginReadonlyTransaction env))

(defn begin-exclusive-transaction
  ^Transaction
  [^Environment env]
  (.beginExclusiveTransaction env))

(defn open-store
  (^Store
   [^Environment env ^Transaction trx nam ^StoreConfig config]
   (.openStore env nam config trx))
  ([env trx nam]
   (open-store env trx nam default-store-config)))

(defn remove-store
  [^Environment env ^Transaction trx nam]
  (.removeStore env nam trx))

(def edn-serializer
  {:decoder #(edn/read-string (StringBinding/entryToString %))
   :encoder #(StringBinding/stringToEntry (pr-str %))
   :name ::edn-serializer})

(defn make-transactional-executable
  [f]
  (reify TransactionalExecutable
    (execute [this trx] (f trx) nil)))

(defn make-transactional-computable
  [f]
  (reify TransactionalComputable
    (compute [this trx] (f trx))))

(defn transactional-write
  ([env-or-trx f]
   (let [[trx transaction-mine?] (if (instance? Transaction env-or-trx)
                                   [env-or-trx false]
                                   [(begin-transaction env-or-trx) true])]
     (try
      (loop []
        (f trx)
        (if (.flush trx)
          trx
          (do
            (.revert trx)
            (recur))))
      (finally
        (when transaction-mine?
          (.abort trx)))))))

(defn transactional-read
  ([env-or-trx f]
   (let [[trx transaction-mine?] (if (instance? Transaction env-or-trx)
                                   [env-or-trx false]
                                   [(begin-read-only-transaction env-or-trx) true])]
     (try
       (f trx)
       (finally
         (when transaction-mine?
           (.abort trx)))))))


(defprotocol TendreMapProtocol
  (close [this] "Close the DB"))

(deftype TendreMap [opts ^Environment env
                    label
                    key-encoder key-decoder
                    value-encoder value-decoder
                    metadata]
  TendreMapProtocol
  (close [this] (.close env))
  clojure.lang.ITransientMap
  (assoc [this k v] (do
                      (transactional-write
                       env
                       (fn [trx]
                         (let [store (open-store env trx label)]
                           (.add store trx (key-encoder k) (value-encoder v)))))
                      this))
  (without [this k] (do
                      (transactional-write
                       env
                       (fn [trx]
                         (let [store (open-store env trx label)]
                           (.delete store trx (key-encoder k)))))
                      this))
  (valAt [_ k] (transactional-read
                env
                (fn [trx]
                  (let [store (open-store env trx label)]
                    (if-let [raw-v (.get store trx (key-encoder k))]
                      (value-decoder raw-v)
                      nil)))))
  (valAt [_ k default] (transactional-read
                        env
                        (fn [trx]
                          (let [store (open-store env trx label)]
                            (if-let [raw-v (.get store trx (key-encoder k))]
                              (value-decoder raw-v)
                              default)))))
  (conj [this [k v]] (do
                       (transactional-write
                        env
                        (fn [trx]
                          (let [store (open-store env trx label)]
                            (.add store trx (key-encoder k) (value-encoder v)))))
                       this))
  (count [_] (transactional-read
              env
              (fn [trx]
                (let [store (open-store env trx label)]
                  (.count store trx)))))
  clojure.lang.ITransientAssociative2
  (containsKey [_ k] (transactional-read
                      env
                      (fn [trx]
                        (let [store (open-store env trx label)]
                          (if (.get store trx (key-encoder k))
                            true
                            false)))))
  (entryAt [this k] (transactional-read
                     env
                     (fn [trx]
                       (let [store (open-store env trx label)]
                         (if-let [raw-v (.get store trx (key-encoder k))]
                           (clojure.lang.MapEntry. k (value-decoder raw-v))
                           nil)))))
  clojure.lang.Seqable
  (seq [this]
    (let [trx (begin-read-only-transaction env)
          store (open-store env trx label)
          cursor (.openCursor store trx)
          generator (fn lazy-gen [cursor]
                      (try
                        (if (.getNext cursor)
                          (lazy-seq
                           (cons
                            (clojure.lang.MapEntry. (key-decoder (.getKey cursor))
                                                    (value-decoder (.getValue cursor)))
                            (lazy-gen cursor)))
                          (do
                            (.close cursor)
                            (.abort trx)
                            nil))
                        (catch Exception e
                          (.close cursor)
                          (.abort trx)
                          (throw e))))]
      (generator cursor)))
  clojure.lang.IFn
  (invoke [this k] (.valAt this k))
  (invoke [this k default] (.valAt this k default))
  ;; clojure.lang.IMeta
  ;; (meta [_] @metadata)
  ;; clojure.lang.IReference
  ;; (alterMeta [_ f args] (reset! metadata (apply f @metadata args)))
  ;; (resetMeta [_ m] (reset! metadata m))
  clojure.lang.IKVReduce
  (kvreduce [_ f init]
    (let [trx (begin-read-only-transaction env)
          store (open-store env trx label)]
      (try
        (with-open [cursor (.openCursor store trx)]
         (loop [ret init]
           (if (.getNext cursor)
             (let [ret* (f ret (key-decoder (.getKey cursor)) (value-decoder (.getValue cursor)))]
               (if (reduced? ret*)
                 @ret*
                 (recur ret*)))
             ret)))
        (finally
          (.abort trx)))))
  clojure.lang.IReduceInit
  (reduce [_ f init]
    (let [trx (begin-read-only-transaction env)
          store (open-store env trx label)]
      (try
        (with-open [cursor (.openCursor store trx)]
         (loop [ret init]
           (if (.getNext cursor)
             (let [ret* (f ret (clojure.lang.MapEntry. (key-decoder (.getKey cursor))
                                                       (value-decoder (.getValue cursor))))]
               (if (reduced? ret*)
                 @ret*
                 (recur ret*)))
             ret)))
        (finally
          (.abort trx)))))
  clojure.lang.IReduce
  (reduce [_ f]
    (let [trx (begin-read-only-transaction env)
          store (open-store env trx label)]
      (try
        (with-open [cursor (.openCursor store trx)]
          (loop [ret nil]
            (if (.getNext cursor)
              (let [ret* (f ret (clojure.lang.MapEntry. (key-decoder (.getKey cursor))
                                                        (value-decoder (.getValue cursor))))]
                (if (reduced? ret*)
                  @ret*
                  (recur ret*)))
              ret)))
        (finally
          (.abort trx)))))
  clojure.lang.MapEquivalence
  ;; PTransactionable
  ;; (commit! [this] (.commit db) this)
  ;; (rollback! [this] (.rollback db) this)
  ;; java.io.Closeable
  ;; (close [_] (.close db))
  ;; Iterable
  ;; (iterator [_] (let [trx (map (fn [^java.util.Map$Entry entry]
  ;;                                (clojure.lang.MapEntry. (key-decoder (.getKey entry))
  ;;                                                        (value-decoder (.getValue entry)))))]
  ;;                 (.iterator ^Iterable (eduction trx (iterator-seq (.. hm entrySet iterator))))))
  ;; PMapDBTransient
  ;; (get-db ^DB [_] db)
  ;; (get-db-options [_] db-opts)
  ;; (get-db-type [_] (:db-type db-opts))
  ;; (get-collection ^HTreeMap [_] hm)
  ;; (get-collection-options [_] hm-opts)
  ;; (get-wrapper-serializers [_] {:key-encoder key-encoder
  ;;                               :key-decoder key-decoder
  ;;                               :value-encoder value-encoder
  ;;                               :value-decoder value-decoder})
  ;; PMapDBPersistent
  ;; (get-collection-name [_] hm-name)
  ;; (get-collection-type [_] :hash-map)
  ;; (compact! [this] (.. db getStore compact) this)
  ;; (empty! [this] (empty! this false))
  ;; (empty! [this notify-listener?]
  ;;   (if notify-listener?
  ;;     (.clearWithExpire hm)
  ;;     (.clearWithoutNotification hm))
  ;;   this)
  ;; clojure.lang.IDeref
  ;; (deref [_] (let [^java.util.Iterator iter (.. hm entrySet iterator)]
  ;;              (loop [ret (transient {})]
  ;;                (if (.hasNext iter)
  ;;                  (let [^java.util.Map$Entry kv (.next iter)]
  ;;                    (recur (assoc! ret (key-decoder (.getKey kv)) (value-decoder (.getValue kv)))))
  ;;                  (persistent! ret)))))
  )

(defn make-map
  [path {:keys [name key-serializer value-serializer]
         :or {name "default-clj-map"
              key-serializer edn-serializer
              value-serializer edn-serializer}
         :as opts}]
  (let [env (open-environment path)]
    (TendreMap. opts env name
                (:encoder key-serializer) (:decoder key-serializer)
                (:encoder value-serializer) (:decoder value-serializer)
                {})))
