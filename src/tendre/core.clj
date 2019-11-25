(ns tendre.core
  (:require [clojure.edn :as edn])
  (:import [jetbrains.exodus.env
            Environment Environments
            Transaction Cursor
            TransactionalExecutable TransactionalComputable
            StoreConfig Store]
           [jetbrains.exodus.bindings
            StringBinding LongBinding DoubleBinding ByteBinding]
           [jetbrains.exodus ByteIterable ArrayByteIterable]))

(defprotocol TendreMapProtocol
  (get-path [this] "Returns the path for this DB")
  (get-options [this] "Returns the options for this DB")
  (get-environment [this] "Returns the env for this DB")
  (get-transaction-type [this] "Returns the type of the current transaction, or nil if none")
  (transact [this transaction] "Returns a version of the DB participating in a transaction"))

(defprotocol TransactionalTendreMap
  (get-transaction [tm transaction-type] "Returns a transaction if it already exists, else creates a new one"))

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

(defmacro conditional-fn
  [namespace & body]
  (let [maybe-ex (try (require [namespace]) (catch Exception e e))]
    (if (instance? Exception maybe-ex)
      `(fn [v#] (throw (ex-info (format "Could not find class or file for ns %s" ~(str namespace))
                                {:namespace ~(str namespace)})))
      `(do
         (require '[~namespace])
         ~@body))))

(def nippy-serialyzer
  {:decoder (conditional-fn taoensso.nippy
                            (fn nippy-decoder [^ByteIterable v]
                              (taoensso.nippy/thaw (.getBytesUnsafe v))))
   :encoder (conditional-fn taoensso.nippy
                            (fn nippy-encoder [v]
                              (ArrayByteIterable. ^bytes (taoensso.nippy/freeze v))))
   :name ::nippy-serializer})

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
   (let [trx-type (when-not (instance? Environment env-or-trx)
                    (get-transaction-type env-or-trx))]
     (cond
       (nil? trx-type) (let [[^Transaction trx _] (get-transaction env-or-trx :read-write)]
                         (try
                           (loop []
                             (f trx)
                             (if (.flush trx)
                               trx
                               (do
                                 (.revert trx)
                                 (recur))))
                           (finally
                             (.abort trx))))
       (= :read-only trx-type) (throw (ex-info "Cannot write from a :read-only transaction" {}))
       :else (let [[trx _] (get-transaction env-or-trx trx-type)]
               (f trx))))))

(defn transactional-read
  ([env-or-trx f]
   (let [[^Transaction trx transaction-mine?] (get-transaction env-or-trx :read-only)]
     (try
       (f trx)
       (finally
         (when transaction-mine?
           (.abort trx)))))))

(def transaction-types
  {:read-only {:begin begin-read-only-transaction
               :flush  (fn [^Transaction trx] (.abort trx) true)
               :revert (fn [^Transaction trx] (.abort trx) true)
               :abort  (fn [^Transaction trx] (.abort trx) true)
               :predicate (fn [^Transaction trx] (.isReadonly trx))}
   :read-write {:begin begin-transaction
                :flush  (fn [^Transaction trx] (.flush trx))
                :revert (fn [^Transaction trx] (.revert trx))
                :abort  (fn [^Transaction trx] (.abort trx))
                :predicate (fn [^Transaction trx] (and (not (.isReadonly trx))
                                                       (not (.isExclusive trx))))}
   :exclusive {:begin begin-exclusive-transaction
               :flush  (fn [^Transaction trx] (.flush trx))
               :revert (fn [^Transaction trx] (.revert trx))
               :abort  (fn [^Transaction trx] (.abort trx))
               :predicate (fn [^Transaction trx] (.isExclusive trx))}})

(extend-protocol TransactionalTendreMap
  Transaction
  (get-transaction [this transaction-type] [this false])
  Environment
  (get-transaction [this transaction-type]
    (let [{:keys [begin]} (transaction-types transaction-type)]
      [(begin this) true])))

(defmacro with-transaction*
  [transaction bindings & body]
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) (let [[nam expr] (subvec bindings 0 2)]
                             `(let [~nam (transact ~expr ~transaction)]
                                (with-transaction* trx# ~(subvec bindings 2) ~@body)))
    :else (throw (IllegalArgumentException.
                   "with-transaction only allows Symbols in bindings"))))

(defmacro with-transaction-top*
  [trx-type bindings & body]
  (let [{:keys [flush revert abort]} (transaction-types trx-type)
        [nam expr] (subvec bindings 0 2)]
    `(let [db# ~expr]
       (let [[trx# transaction-mine?#] (get-transaction db# ~trx-type)]
         (if transaction-mine?#
           (let [~nam (transact db# trx#)]
             (try
               (loop []
                 (let [result# (with-transaction* trx# ~(subvec bindings 2) ~@body)]
                   (if (~flush trx#)
                     (do
                       result#)
                     (do
                       (~revert trx#)
                       (recur)))))
               (finally
                 (~abort trx#))))
           (let [~nam db#]
             (with-transaction* trx# ~(subvec bindings 2) ~@body)))))))

(defmacro with-transaction
  [bindings & body]
  `(with-transaction-top* :read-write ~bindings ~@body))


(defn close!
  [^java.lang.AutoCloseable tm]
  (.close tm))

(defn update!
  [this k f & args]
  (with-transaction [tm this]
    (let [old-val (tm k)
          new-val (apply f old-val args)]
      (assoc! tm k new-val))))

(defn find-transaction-type
  [^Transaction trx]
  (when (instance? Transaction trx)
    (cond
      (.isReadonly trx) :read-only
      (.isExclusive trx) :exclusive
      :else :read-write)))

(defn update-in!
  [m ks f & args]
  (let [[top & left] ks]
    (update! m top (fn [old] (update-in old left #(apply f % args))))))

(deftype TendreMap [path opts ^Environment env
                    label
                    key-encoder key-decoder
                    value-encoder value-decoder
                    metadata ^Transaction transaction ^Store store]
  TendreMapProtocol
  (get-path [_] path)
  (get-options [_] opts)
  (get-environment [_] env)
  (get-transaction-type [this] (find-transaction-type transaction))
  (transact [this transaction]
    (TendreMap. path opts env label key-encoder key-decoder
                value-encoder value-decoder metadata
                transaction (open-store env transaction label)))
  TransactionalTendreMap
  (get-transaction [_ transaction-type]
    (cond
      (let [actual-type (find-transaction-type transaction)]
        (or (= actual-type transaction-type)
            (and actual-type (= transaction-type :read-only)))) [transaction false]
      transaction (throw (ex-info (format "Wrong type of transaction in progress, asked for %s but is %s"
                                          transaction-type (find-transaction-type transaction))
                                  {:transaction-type-asked transaction-type}))
      :else (get-transaction env transaction-type)))
  clojure.lang.ITransientMap
  (assoc [this k v] (do
                      (transactional-write
                       this
                       (fn [trx]
                         (let [^Store store (or store (open-store env trx label))]
                           (.put store trx (key-encoder k) (value-encoder v)))))
                      this))
  (without [this k] (do
                      (transactional-write
                       this
                       (fn [trx]
                         (let [^Store store (or store (open-store env trx label))]
                           (.delete store trx (key-encoder k)))))
                      this))
  (valAt [this k] (transactional-read
                   this
                   (fn [trx]
                     (let [^Store store (or store (open-store env trx label))]
                       (if-let [raw-v (.get store trx (key-encoder k))]
                         (value-decoder raw-v)
                         nil)))))
  (valAt [this k default] (transactional-read
                           this
                           (fn [trx]
                             (let [^Store store (or store (open-store env trx label))]
                               (if-let [raw-v (.get store trx (key-encoder k))]
                                 (value-decoder raw-v)
                                 default)))))
  (conj [this [k v]] (do
                       (transactional-write
                        this
                        (fn [trx]
                          (let [^Store store (or store (open-store env trx label))]
                            (.put store trx (key-encoder k) (value-encoder v)))))
                       this))
  (count [this] (transactional-read
                 this
                 (fn [trx]
                   (let [^Store store (or store (open-store env trx label))]
                     (.count store trx)))))
  clojure.lang.ITransientAssociative2
  (containsKey [this k] (transactional-read
                         this
                         (fn [trx]
                           (let [^Store store (or store (open-store env trx label))]
                             (if (.get store trx (key-encoder k))
                               true
                               false)))))
  (entryAt [this k] (transactional-read
                     this
                     (fn [trx]
                       (let [^Store store (or store (open-store env trx label))]
                         (if-let [raw-v (.get store trx (key-encoder k))]
                           (clojure.lang.MapEntry. k (value-decoder raw-v))
                           nil)))))
  clojure.lang.Seqable
  (seq [this]
    (let [[^Transaction trx own-trx?] (if transaction [transaction false] [(begin-read-only-transaction env) true])
          ^Store store (or store (open-store env trx label))
          cursor (.openCursor store trx)
          generator (fn lazy-gen [^Cursor cursor]
                      (try
                        (if (.getNext cursor)
                          (lazy-seq
                           (cons
                            (clojure.lang.MapEntry. (key-decoder (.getKey cursor))
                                                    (value-decoder (.getValue cursor)))
                            (lazy-gen cursor)))
                          (do
                            (.close cursor)
                            (when own-trx? (.abort trx))
                            nil))
                        (catch Exception e
                          (.close cursor)
                          (when own-trx? (.abort trx))
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
    (let [[^Transaction trx own-trx?] (if transaction [transaction false] [(begin-read-only-transaction env) true])
          ^Store store (or store (open-store env trx label))]
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
          (when own-trx? (.abort trx))))))
  clojure.lang.IReduceInit
  (reduce [_ f init]
    (let [[^Transaction trx own-trx?] (if transaction [transaction false] [(begin-read-only-transaction env) true])
          ^Store store (or store (open-store env trx label))]
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
          (when own-trx? (.abort trx))))))
  clojure.lang.IReduce
  (reduce [_ f]
    (let [[^Transaction trx own-trx?] (if transaction [transaction false] [(begin-read-only-transaction env) true])
          ^Store store (or store (open-store env trx label))]
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
          (when own-trx? (.abort trx))))))
  clojure.lang.MapEquivalence
  java.lang.AutoCloseable
  (close [this] (.close ^Environment (get-environment this)))
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
    (transactional-write
     env
     (fn [trx]
       (open-store env trx name)))
    (TendreMap. path opts env name
                (:encoder key-serializer) (:decoder key-serializer)
                (:encoder value-serializer) (:decoder value-serializer)
                {} nil nil)))
