(ns jepsen.t4.append
  "Elle list-append workload, ported from jepsen-io/etcd's append.clj.

  Each key holds an EDN list. A txn like [[:r 1 nil] [:append 2 5]] runs as
  two etcd Txns: one reads the current lists of every key the txn appends to,
  then a second, guarded on those keys' ModRevisions being unchanged, puts the
  extended lists and performs the reads. Elle checks the history is
  strict-serializable."
  (:require [jepsen.client             :as client]
            [jepsen.tests.cycle.append :as append]
            [jepsen.t4
             [client :as tc]
             [txn    :as t]])
  (:import [io.etcd.jetcd Client]))

(def ^:private key-prefix "/jepsen/append/")

(defn- key-name [k] (str key-prefix k))

(defn- written-keys
  "The set of keys the txn appends to."
  [txn]
  (->> txn (filter (comp #{:append} first)) (map second) set))

(defn- read-phase
  "One Txn of Gets over ks. Returns [revision {k {:value .. :mod-revision ..}}]
  with absent keys omitted."
  [kv ks]
  (let [ks  (vec ks)
        res (t/txn! kv [] (map (comp t/get-op key-name) ks))]
    [(:revision res)
     (into {} (keep (fn [[k r]] (when r [k r])) (map vector ks (:gets res))))]))

(defn- guards
  "Each written key must be unchanged since the read phase: a key that existed
  keeps its ModRevision; an absent key must not have been written at or after
  the read revision."
  [read-rev reads ks]
  (for [k ks]
    (if-let [r (get reads k)]
      (t/mod-revision= (key-name k) (:mod-revision r))
      (t/mod-revision< (key-name k) read-rev))))

(defn- write-ops
  "Turns the txn's micro-ops into etcd ops, playing appends forward over the
  read lists so several appends to one key become successive puts. (t4, like
  etcd, rejects a branch that puts one key twice; such a txn fails
  definitely.)"
  [reads txn]
  (second
    (reduce (fn [[state ops] [f k v]]
              (case f
                :r      [state (conj ops (t/get-op (key-name k)))]
                :append (let [l (conj (get state k []) v)]
                          [(assoc state k l)
                           (conj ops (t/put-op (key-name k) l))])))
            [(update-vals reads :value) []]
            txn)))

(defn- apply-ro!
  "A read-only txn. A single read is a plain Get; several share one Txn so
  they observe one revision."
  [kv op txn]
  (if (= 1 (count txn))
    (let [[f k _] (first txn)]
      (assoc op :type :ok :value [[f k (:value (t/get! kv (key-name k)))]]))
    (let [ks      (distinct (map second txn))
          [_ rs]  (read-phase kv ks)]
      (assoc op :type :ok
                :value (mapv (fn [[f k _]] [f k (:value (get rs k))]) txn)))))

(defn- apply-rw!
  "A txn with appends: read phase, then the guarded write Txn. wrote? is set
  before the write Txn goes out, so an error after that is :info."
  [kv op txn wrote?]
  (let [ks             (written-keys txn)
        [rev reads]    (read-phase kv ks)
        _              (vreset! wrote? true)
        res            (t/txn! kv (guards rev reads ks) (write-ops reads txn))]
    (if (:succeeded? res)
      (assoc op :type :ok :value (t/fill-reads txn (:gets res)))
      ;; A guard failed: a concurrent txn wrote one of our keys. Nothing was
      ;; applied.
      (assoc op :type :fail :error :conflict))))

(defrecord AppendClient [^Client conn kv]
  client/Client

  (open! [this test node]
    (let [c (tc/connect! node)]
      (assoc this :conn c :kv (.getKVClient c))))

  (setup! [this test])

  (invoke! [this test op]
    (let [txn    (:value op)
          wrote? (volatile! false)]
      (try
        (if (empty? (written-keys txn))
          (apply-ro! kv op txn)
          (apply-rw! kv op txn wrote?))
        (catch Exception e
          (t/error-op op (not @wrote?) e)))))

  (teardown! [this test])

  (close! [this test]
    (when kv   (.close kv))
    (when conn (.close conn))))

(defn workload
  "List-append over 3 keys with txns of up to 4 micro-ops, checked for
  strict serializability, as upstream does."
  [_opts]
  (let [{:keys [generator checker]}
        (append/test {:key-count          3
                      :max-txn-length     4
                      :consistency-models [:strict-serializable]})]
    {:client    (map->AppendClient {})
     :checker   checker
     :gen       generator
     ;; No final read: every :ok txn already carries its reads.
     :final-gen nil}))
