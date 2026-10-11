(ns jepsen.t4.txn
  "Shared plumbing for the Elle transactional workloads (append, wr): etcd
  Txn construction over EDN-encoded values, and error classification."
  (:require [clojure.edn           :as edn]
            [jepsen.t4.client      :as tc])
  (:import [io.etcd.jetcd KV]
           [io.etcd.jetcd.kv GetResponse TxnResponse]
           [io.etcd.jetcd.op Op Cmp Cmp$Op CmpTarget]
           [io.etcd.jetcd.options GetOption PutOption]
           [java.util.concurrent ExecutionException TimeUnit TimeoutException]))

;; ── Encoding ──────────────────────────────────────────────────────────────────
;;
;; Values are stored as EDN so lists and integers round-trip exactly.

(defn encode [v] (tc/->bs (pr-str v)))

(defn decode [bs] (some-> (tc/bs-> bs) edn/read-string))

;; ── Txn ops ───────────────────────────────────────────────────────────────────

(defn get-op [^String k] (Op/get (tc/->bs k) GetOption/DEFAULT))

(defn put-op [^String k v] (Op/put (tc/->bs k) (encode v) PutOption/DEFAULT))

(defn mod-revision=
  "Guard: k's ModRevision equals rev."
  [^String k rev]
  (Cmp. (tc/->bs k) Cmp$Op/EQUAL (CmpTarget/modRevision rev)))

(defn mod-revision<
  "Guard: k's ModRevision is below rev. An absent key compares as 0."
  [^String k rev]
  (Cmp. (tc/->bs k) Cmp$Op/LESS (CmpTarget/modRevision rev)))

(defn get-result
  "Turns a GetResponse for one key into {:value v :mod-revision r}, or nil if
  the key is absent."
  [^GetResponse resp]
  (when-let [kv (first (.getKvs resp))]
    {:value        (decode (.getValue kv))
     :mod-revision (.getModRevision kv)}))

(defn txn!
  "Commits Txn(If cmps, Then ops) and returns

    :succeeded?  whether the compares held
    :revision    the response header's revision
    :gets        get-result of each Get op in Then, in order"
  [^KV kv cmps ops]
  (let [^TxnResponse resp (-> (.txn kv)
                              (.If   (into-array Cmp cmps))
                              (.Then (into-array Op  ops))
                              (.Else (into-array Op  []))
                              (.commit)
                              (.get tc/timeout-ms TimeUnit/MILLISECONDS))]
    {:succeeded? (.isSucceeded resp)
     :revision   (.getRevision (.getHeader resp))
     :gets       (mapv get-result (.getGetResponses resp))}))

(defn get!
  "A plain linearizable Get of one key; returns get-result."
  [^KV kv ^String k]
  (-> (.get kv (tc/->bs k))
      (.get tc/timeout-ms TimeUnit/MILLISECONDS)
      get-result))

;; ── Results ───────────────────────────────────────────────────────────────────

(defn fill-reads
  "Rewrites the [:r k nil] micro-ops of an Elle txn with the values of gets
  (get-results, in order); writes pass through."
  [txn gets]
  (first
    (reduce (fn [[txn' gets] [f k _ :as mop]]
              (if (= :r f)
                [(conj txn' [f k (:value (first gets))]) (rest gets)]
                [(conj txn' mop) gets]))
            [[] gets]
            txn)))

;; ── Errors ────────────────────────────────────────────────────────────────────

(defn- root-message [^Throwable e]
  (let [e (if (and (instance? ExecutionException e) (.getCause e))
            (.getCause e)
            e)]
    (or (.getMessage e) (str (class e)))))

(defn error-op
  "Completes op after exception e. definite? is true when the op has sent no
  write, so nothing it did can have been applied: it is a :fail. Otherwise a
  write may have committed with its reply lost, and the op is :info.

  A txn that writes one key twice is rejected by t4 before it is assigned a
  revision (as etcd does), so it is a definite :fail too."
  [op definite? e]
  (cond
    (instance? TimeoutException e)
    (assoc op :type (if definite? :fail :info) :error :timeout)

    (re-find #"duplicate key" (root-message e))
    (assoc op :type :fail :error :duplicate-key)

    :else
    (assoc op :type (if definite? :fail :info) :error (root-message e))))
