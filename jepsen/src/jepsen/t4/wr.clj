(ns jepsen.t4.wr
  "Elle rw-register workload, ported from jepsen-io/etcd's wr.clj.

  Each key is a register holding an integer. A txn like [[:r 1 nil] [:w 2 7]]
  runs as one etcd Txn with no compares: Gets and Puts in micro-op order.
  Elle checks the history is strict-serializable."
  (:require [jepsen.client         :as client]
            [jepsen.tests.cycle.wr :as wr]
            [jepsen.t4
             [client :as tc]
             [txn    :as t]])
  (:import [io.etcd.jetcd Client]))

(def ^:private key-prefix "/jepsen/wr/")

(defn- key-name [k] (str key-prefix k))

(defn- etcd-ops [txn]
  (mapv (fn [[f k v]]
          (case f
            :r (t/get-op (key-name k))
            :w (t/put-op (key-name k) v)))
        txn))

(defrecord WrClient [^Client conn kv]
  client/Client

  (open! [this test node]
    (let [c (tc/connect! node)]
      (assoc this :conn c :kv (.getKVClient c))))

  (setup! [this test])

  (invoke! [this test op]
    (let [txn (:value op)]
      (try
        (let [res (t/txn! kv [] (etcd-ops txn))]
          (if (:succeeded? res)
            (assoc op :type :ok :value (t/fill-reads txn (:gets res)))
            ;; No compares, so this cannot happen; nothing was applied if it
            ;; did.
            (assoc op :type :fail :error :didnt-succeed)))
        (catch Exception e
          ;; Only a read-only txn is definite.
          (t/error-op op (every? (comp #{:r} first) txn) e)))))

  (teardown! [this test])

  (close! [this test]
    (when kv   (.close kv))
    (when conn (.close conn))))

(defn workload
  "rw-register over 3 keys with txns of up to 4 micro-ops, checked for strict
  serializability. :wfr-keys? lets Elle infer that a txn's write of a key
  follows its own read of that key in version order, which any serializable
  Txn guarantees. (Upstream spells it :wfr-keys, which Elle ignores.)"
  [_opts]
  (let [{:keys [generator checker]}
        (wr/test {:key-count          3
                  :max-txn-length     4
                  :consistency-models [:strict-serializable]
                  :wfr-keys?          true})]
    {:client    (map->WrClient {})
     :checker   checker
     :gen       generator
     ;; No final read: every :ok txn already carries its reads.
     :final-gen nil}))
