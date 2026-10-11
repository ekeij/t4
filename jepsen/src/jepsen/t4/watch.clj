(ns jepsen.t4.watch
  "Watch workload, ported from jepsen-io/etcd's watch.clj.

  Writer threads put successive integers to one key. Watcher threads watch
  that key in a series of short watches, each resuming at the revision after
  the last event the thread saw, and record the values they observed. At the
  end every watcher catches up to the same final revision, and the checker
  confirms all watchers observed the same sequence of values: watch streams
  served by different nodes must expose one totally ordered revision log,
  with no event lost, duplicated, or reordered."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.checker        :as checker]
            [jepsen.client         :as client]
            [jepsen.generator      :as gen]
            [jepsen.util           :as util]
            [jepsen.t4.client      :as tc]
            [jepsen.t4.txn         :as t])
  (:import [io.etcd.jetcd Client Watch$Listener]
           [io.etcd.jetcd.options WatchOption]
           [io.etcd.jetcd.watch WatchEvent WatchResponse]
           [java.util.concurrent BrokenBarrierException TimeUnit]
           [java.util.concurrent.locks LockSupport]))

(def ^:private watch-key "/jepsen/watch")

;; ── Converger ─────────────────────────────────────────────────────────────────
;;
;; The final watch must keep each watcher going until all of them reach the
;; same revision. A converger holds every watcher's latest value; a thread
;; evolves its value while the set is divergent and parks otherwise.

(defn converger
  "A convergence context for n threads; values are converged once
  (converged? values) holds."
  [n converged?]
  (atom {:converged? converged?
         :threads    []
         :values     (vec (repeat n ::init))}))

(defn- signal! [c]
  (doseq [th (:threads @c)] (LockSupport/unpark th)))

(defn- stable?
  "No value is initial or mid-evolution."
  [c]
  (not-any? #{::init ::evolving} (:values c)))

(defn- divergent?
  "Some value is initial, or the settled values fail the convergence test."
  [c]
  (or (some #{::init} (:values c))
      (let [vs (remove #{::evolving} (:values c))]
        (and (seq vs) (not ((:converged? c) vs))))))

(defn- converged? [c]
  (and (stable? c) (not (divergent? c))))

(defn- evolve!
  "Replaces thread i's value v (or init) with (evolve v). If evolve throws,
  the converger is marked crashed, which fails every other thread, and the
  exception propagates to this thread's caller."
  [conv evolve init i]
  (let [v (atom nil)]
    (swap! conv (fn [c]
                  (let [x (nth (:values c) i)]
                    (reset! v (if (= ::init x) init x)))
                  (assoc-in c [:values i] ::evolving)))
    (try
      (let [v' (evolve @v)]
        (swap! conv assoc-in [:values i] v'))
      (catch Throwable e
        (swap! conv assoc :crashed? true)
        (throw e))
      (finally
        (signal! conv)))))

(defn converge!
  "Evolves this thread's value from init until every thread's value has
  converged, then returns it. Throws ex-info {:type ::converge-timeout :value
  v} after timeout-ms."
  [timeout-ms conv init evolve]
  (let [deadline (+ (System/nanoTime) (util/ms->nanos timeout-ms))
        i        (-> (swap! conv update :threads conj (Thread/currentThread))
                     :threads count dec)]
    (loop []
      (let [c @conv]
        (cond (:crashed? c)
              (throw (BrokenBarrierException. "convergence failed"))

              (< deadline (System/nanoTime))
              (throw (ex-info "converge timeout"
                              {:type ::converge-timeout
                               :value (get-in c [:values i])}))

              (converged? c)
              (get-in c [:values i])

              (divergent? c)
              (do (evolve! conv evolve init i) (recur))

              :else
              (do (LockSupport/parkNanos conv (util/ms->nanos 100))
                  (recur)))))))

;; ── Watching ──────────────────────────────────────────────────────────────────

(defn- watch-for
  "Watches the key from revision+1 for ms milliseconds. Returns {:revision r
  :log [v ...]}: the values seen, and the ModRevision of the last one (or
  revision if none). jetcd re-establishes a broken stream itself, resuming
  after the last event it delivered; an error it gives up on is thrown."
  [^Client conn revision ms]
  (let [state (atom {:revision revision :log []})
        error (promise)
        listener
        (reify Watch$Listener
          (onNext [_ resp]
            (let [evs (.getEvents ^WatchResponse resp)]
              (when (seq evs)
                (let [vals (mapv #(t/decode (.getValue (.getKeyValue ^WatchEvent %))) evs)
                      rev' (reduce max (map #(.getModRevision (.getKeyValue ^WatchEvent %)) evs))]
                  (swap! state
                         (fn [{:keys [revision] :as s}]
                           (if (< revision rev')
                             {:revision rev' :log (into (:log s) vals)}
                             ;; Events must arrive in strictly increasing
                             ;; revision order. Freeze the state and report.
                             (do (deliver error
                                          (ex-info "nonmonotonic watch"
                                                   {:type ::nonmonotonic
                                                    :description
                                                    (str "got event at revision " rev'
                                                         " after " revision)}))
                                 s))))))))
          (onError [_ e] (deliver error e))
          (onCompleted [_]))
        opt (-> (WatchOption/builder)
                ;; Inclusive; never pass 0, which means "from now".
                (.withRevision (inc revision))
                (.build))
        w   (.watch (.getWatchClient conn) (tc/->bs watch-key) opt listener)]
    (Thread/sleep (long ms))
    (try (.close w) (catch Exception _))
    ;; Events after this snapshot are dropped; the next watch resumes after
    ;; the snapshot's revision and sees them again.
    (let [s @state]
      (when (realized? error) (throw @error))
      s)))

;; ── Client ────────────────────────────────────────────────────────────────────
;;
;; Watch ops must never be :info: an :info op retires the process, its
;; successor opens a fresh client at revision 0, and that thread's log would
;; restart and repeat values. A watch changes nothing, so :fail is accurate.
;; A put that errors may have committed, so it is :info as everywhere else.

(defrecord WatchClient [^Client conn kv max-revision revision conv]
  client/Client

  (open! [this test node]
    (let [c (tc/connect! node)]
      (assoc this :conn c :kv (.getKVClient c) :revision (atom 0))))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :write
        (let [resp (-> (.put kv (tc/->bs watch-key) (t/encode (:value op)))
                       (.get tc/timeout-ms TimeUnit/MILLISECONDS))]
          (swap! max-revision max (.getRevision (.getHeader resp)))
          (assoc op :type :ok))

        :watch
        (let [res (watch-for conn @revision (rand-int 5000))]
          (reset! revision (:revision res))
          (swap! max-revision max (:revision res))
          (assoc op :type :ok :value res))

        :final-watch
        (try
          (let [v (converge!
                    60000 conv {:revision @revision :log []}
                    (fn [v]
                      (try
                        (info "at revision" (:revision v) "catching up to"
                              @max-revision)
                        (let [w (watch-for conn (:revision v) (rand-int 5000))]
                          (reset! revision (:revision w))
                          (swap! max-revision max (:revision w))
                          (update w :log #(into (:log v) %)))
                        ;; A nonmonotonic watch is a finding, not a
                        ;; transient error: give up.
                        (catch clojure.lang.ExceptionInfo e
                          (throw e))
                        (catch Exception e
                          (warn e "final watch failed; retrying")
                          (Thread/sleep 1000)
                          v))))]
            (assoc op :type :ok :value v))
          (catch clojure.lang.ExceptionInfo e
            (if (= ::converge-timeout (:type (ex-data e)))
              (assoc op :type :ok :value (:value (ex-data e))
                        :error :converge-timeout)
              (throw e)))))

      (catch clojure.lang.ExceptionInfo e
        (if (= ::nonmonotonic (:type (ex-data e)))
          (assoc op :type :fail
                    :error [:nonmonotonic-watch (:description (ex-data e))])
          (assoc op :type (if (= :write (:f op)) :info :fail)
                    :error (.getMessage e))))

      (catch java.util.concurrent.TimeoutException _
        (assoc op :type (if (= :write (:f op)) :info :fail) :error :timeout))

      (catch Exception e
        (assoc op :type (if (= :write (:f op)) :info :fail)
                  :error (str e)))))

  (teardown! [this test])

  (close! [this test]
    (when kv   (.close kv))
    (when conn (.close conn))))

;; ── Checker ───────────────────────────────────────────────────────────────────

(defn- per-thread-watches
  "Thread → ok :watch/:final-watch ops, in history order."
  [test history]
  (->> history
       (filter #(and (= :ok (:type %)) (#{:watch :final-watch} (:f %))))
       (group-by #(mod (:process %) (:concurrency test)))))

(defn- first-difference
  "Index of the first position where logs a and b differ, or nil."
  [a b]
  (let [n (count (take-while true? (map = a b)))]
    (when-not (= (count a) (count b) n) n)))

(defn checker
  "Every watcher's concatenated log must equal the canonical one (the most
  common log, else the longest). Revisions that never converged leave the
  result :unknown; a nonmonotonic event is invalid outright."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [watches   (per-thread-watches test history)
            logs      (update-vals watches #(vec (mapcat (comp :log :value) %)))
            revisions (update-vals watches
                                   #(reduce max 0 (map (comp :revision :value) %)))
            freqs     (frequencies (vals logs))
            [mode n]  (apply max-key val (or (seq freqs) [[[] 0]]))
            canonical (if (< 1 n) mode (apply max-key count (or (seq (vals logs)) [[]])))
            deltas    (->> logs
                           (keep (fn [[thread log]]
                                   (when-let [i (first-difference canonical log)]
                                     {:thread           thread
                                      :length           (count log)
                                      :first-difference i
                                      :expected         (take 5 (drop i canonical))
                                      :actual           (take 5 (drop i log))})))
                           (sort-by :thread))
            nm-errors (->> history
                           (keep :error)
                           (filter #(and (vector? %) (= :nonmonotonic-watch (first %))))
                           (map second))
            valid?    (cond (seq nm-errors)                     false
                            (seq deltas)                        false
                            (not (apply = (vals revisions)))    :unknown
                            :else                               true)]
        (cond-> {:valid?           valid?
                 :revisions        revisions
                 :canonical-length (count canonical)}
          (seq deltas)    (assoc :deltas deltas)
          (seq nm-errors) (assoc :nonmonotonic-errors nm-errors))))))

;; ── Workload ──────────────────────────────────────────────────────────────────

(defn workload
  "The first writer-count threads write; the rest watch. Upstream gives each
  node a writer; CI runs at concurrency 3, so one writer per three threads
  (at least one) leaves the rest to watch."
  [opts]
  (let [concurrency  (:concurrency opts)
        writer-count (max 1 (quot concurrency 3))
        watch-count  (- concurrency writer-count)
        conv         (converger watch-count
                                (fn [vs] (apply = (map :revision vs))))]
    (assert (pos? watch-count) "watch workload needs concurrency >= 2")
    {:client    (map->WatchClient {:max-revision (atom 0) :conv conv})
     :checker   (checker)
     :gen       (gen/reserve writer-count
                             (map (fn [x] {:type :invoke :f :write :value x})
                                  (range))
                             (repeat {:type :invoke :f :watch}))
     :final-gen (gen/reserve writer-count nil
                             (gen/each-thread {:type :invoke :f :final-watch}))}))
