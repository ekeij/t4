(ns jepsen.t4.set
  "Set workload, ported from jepsen-io/etcd's set.clj.

  One key holds an EDN set of integers. :add reads the set and CASes it,
  guarded on the ModRevision it read, retrying on conflict; :read returns the
  set. checker/set-full checks every acknowledged add is eventually visible to
  every read and, with :linearizable?, that it is visible to every read that
  begins after the add completed."
  (:require [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.t4
             [client :as tc]
             [txn    :as t]])
  (:import [io.etcd.jetcd Client]))

(def ^:private set-key "/jepsen/set")

;; Bounds the CAS retry loop of one :add under contention. Every failed
;; attempt applied nothing, so running out is a definite :fail.
(def ^:private max-attempts 10)

(defn- add!
  "Adds x to the set. Returns true once a CAS lands, false if every attempt
  lost a race. wrote? is set before the first CAS goes out."
  [kv x wrote?]
  (loop [attempt 1]
    (let [{:keys [value mod-revision] :or {mod-revision 0}} (t/get! kv set-key)
          _   (vreset! wrote? true)
          ;; An absent key has ModRevision 0, so the first add creates it.
          res (t/txn! kv
                      [(t/mod-revision= set-key mod-revision)]
                      [(t/put-op set-key (conj (or value #{}) x))])]
      (cond (:succeeded? res)        true
            (< attempt max-attempts) (do (Thread/sleep (rand-int 20))
                                         (recur (inc attempt)))
            :else                    false))))

(defrecord SetClient [^Client conn kv]
  client/Client

  (open! [this test node]
    (let [c (tc/connect! node)]
      (assoc this :conn c :kv (.getKVClient c))))

  (setup! [this test])

  (invoke! [this test op]
    (let [wrote? (volatile! false)]
      (try
        (case (:f op)
          :read (assoc op :type :ok
                          :value (or (:value (t/get! kv set-key)) #{}))
          :add  (if (add! kv (:value op) wrote?)
                  (assoc op :type :ok)
                  (assoc op :type :fail :error :cas-retries-exhausted)))
        (catch Exception e
          (t/error-op op (not @wrote?) e)))))

  (teardown! [this test])

  (close! [this test]
    (when kv   (.close kv))
    (when conn (.close conn))))

(defn workload
  "Adds of unique integers mixed with reads. Upstream reserves 5 threads for
  reads; CI runs at concurrency 3, so this mixes them instead."
  [_opts]
  (let [n (atom -1)]
    {:client  (map->SetClient {})
     :checker (checker/set-full {:linearizable? true})
     :gen     (gen/mix
                [(repeat {:type :invoke :f :read :value nil})
                 (fn [] {:type :invoke :f :add :value (swap! n inc)})])}))
