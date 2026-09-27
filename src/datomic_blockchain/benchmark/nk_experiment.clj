(ns datomic-blockchain.benchmark.nk-experiment
  "The NK completeness experiment: varies interdependency K (derivation
   depth) and component count N, generates synthetic provenance chains as
   transactable PROV, scores each through the public completeness
   interface, and stamps a recall decision with the rubric rule.

   Method, not contribution (see CONTEXT.md): the NK model — via
   benchmark.nk-model, the single definition of the config and grid —
   varies path hardness. Per hop, one of the four evidence defects is
   injected with probability :defect-rate by a seeded RNG, so runs are
   deterministic given the seed (recorded in the manifest). As K grows,
   a path is less likely to be defect-free, so decision-readiness
   (do-not-pull rate) decays with interdependency — the figure the
   paper reports.

   Entry point: clojure -M:nk-experiment — writes raw rows, a summary
   CSV, and a manifest under benchmarks/reproducibility/nk/out/."
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [datomic.api :as d]
            [datomic-blockchain.benchmark.nk-model :as nk-model]
            [datomic-blockchain.datomic.schema :as schema]
            [datomic-blockchain.query.graph :as graph]
            [datomic-blockchain.traceability.completeness :as completeness]
            [datomic-blockchain.traceability.recall-rubric :as recall-rubric])
  (:import [java.util Date Random UUID]))

;; =============================================================================
;; Deterministic ids and RNG
;; =============================================================================

(def default-opts
  "Published defaults for the experiment run, recorded in the manifest."
  {:n-values [5 10 20 50]
   :k-values [1 2 3 5 8]
   :trials 10
   :defect-rate 0.15
   :seed 42})

(defn- stable-uuid
  [id-str]
  (UUID/nameUUIDFromBytes (.getBytes ^String id-str "UTF-8")))

(defn- chain-rng
  "Deterministic RNG for one (seed, n, k, trial) cell. String hashCode
   is stable across JVMs and platforms."
  [seed n k trial]
  (Random. (long (.hashCode ^String (str "nk-completeness/" seed "/" n "/" k "/" trial)))))

;; =============================================================================
;; Chain generation (transactable PROV)
;; =============================================================================

(def ^:private defect-types
  "The four first-class evidence defects, same types the labeled-path
   fixtures inject (see traceability.sample-data)."
  [:missing-activity :missing-agent :missing-start-time :invalid-time-order])

(defn- draw-hop-defect
  "One defect per affected hop, drawn from the seeded RNG."
  [rng defect-rate]
  (when (< (.nextDouble rng) defect-rate)
    (nth defect-types (.nextInt rng (count defect-types)))))

(defn- hop-times
  "Start/end times for hop i: a one-hour activity, one hour after the
   previous hop — chronologically valid unless a defect rewrites it."
  [i]
  (let [base 1700000000000          ; fixed epoch: deterministic runs
        start (+ base (* i 3600000))]
    {:started (Date. start)
     :ended (Date. (+ start 3600000))}))

(defn- hop-tx
  "Entity i, its generating activity, and its links. The chain: entity 0
   (the product under trace) derives from entity 1, ... up to entity k,
   the root, which derives from nothing."
  [{:keys [rng defect-rate hops id]} i participant-count]
  (let [entity-id (id (str "e" i))
        activity-id (id (str "a" i))
        agent-id (id (str "g" (mod i participant-count)))
        defect (draw-hop-defect rng defect-rate)
        {:keys [started ended]} (hop-times i)
        activity (cond-> {:db/id (str "activity-" i)
                          :prov/activity activity-id
                          :prov/activity-type :activity/processing
                          :prov/startedAtTime started
                          :prov/endedAtTime ended
                          :prov/wasAssociatedWith agent-id}
                   (= defect :missing-agent) (dissoc :prov/wasAssociatedWith)
                   (= defect :missing-start-time) (dissoc :prov/startedAtTime)
                   (= defect :invalid-time-order) (assoc :prov/endedAtTime
                                                         (Date. (- (.getTime ^Date started)
                                                                   3600000))))
        entity (cond-> {:db/id (str "entity-" i)
                        :prov/entity entity-id
                        :prov/entity-type :product/batch
                        :prov/wasGeneratedBy activity-id}
                 (< i hops) (assoc :prov/wasDerivedFrom (id (str "e" (inc i))))
                 (= defect :missing-activity) (dissoc :prov/wasGeneratedBy))]
    [entity activity {:db/id (str "agent-" i)
                      :prov/agent agent-id
                      :prov/agent-name (str "Participant " (mod i participant-count))
                      :prov/agent-type :organization/manufacturer}]))

(defn chain-config
  "The NKConfig for one experiment cell: N components (entities plus
   participants and batches, per nk-model's assembly), K as derivation
   depth (:k-traceability-hops)."
  [n k]
  (first (nk-model/nk-grid
          [n] [k]
          (fn [n] (max 3 (quot n 2)))
          (fn [k] {:k-qc-points 0
                   :k-traceability-hops k
                   :k-certifications 0
                   :k-processing-steps 0}))))

(defn seed-chain!
  "Generate and transact one synthetic provenance chain for the cell
   {:n :k :trial :defect-rate :seed}. Returns {:start-entity-id uuid} —
   entity 0, the product whose provenance path the chain encodes."
  [conn {:keys [n k trial defect-rate seed] :or {defect-rate 0.0
                                                 seed (:seed default-opts)}}]
  (let [config (chain-config n k)
        participant-count (max 1 (:n-participants config))
        hops (max 0 (:k-traceability-hops config))
        rng (chain-rng seed n k trial)
        prefix (str "nk-completeness/entity/" n "/" k "/" trial "/")
        id (fn [suffix] (stable-uuid (str prefix suffix)))
        txs (mapcat (fn [i] (hop-tx {:rng rng :defect-rate defect-rate
                                     :hops hops :id id}
                                    i participant-count))
                    (range (inc hops)))]
    @(d/transact conn (vec txs))
    {:start-entity-id (id "e0")}))

(defn assess-chain
  "Completeness assessment of a generated chain's start entity — the
   public completeness interface over generated data."
  [db start-entity-id]
  (completeness/assess-completeness db start-entity-id))

(defn chain-hop-count
  "Number of entities on the derivation chain from the start entity
   (k derivation hops = k+1 entities)."
  [db start-entity-id]
  (loop [id start-entity-id
         hops 1]
    (let [parents (graph/get-parents db id)]
      (if (seq parents)
        (recur (first parents) (inc hops))
        hops))))

;; =============================================================================
;; The experiment
;; =============================================================================

(defn- experiment-row
  [conn {:keys [n k trial defect-rate seed]}]
  (let [{:keys [start-entity-id]} (seed-chain! conn {:n n :k k :trial trial
                                                     :defect-rate defect-rate
                                                     :seed seed})
        {:keys [completeness-score missing-evidence]} (assess-chain (d/db conn) start-entity-id)]
    {:n n
     :k k
     :trial trial
     :completeness-score completeness-score
     :missing-evidence missing-evidence
     :recall-decision (recall-rubric/decision-for-evidence missing-evidence)}))

(defn run-experiment
  "Run the NK completeness experiment over the grid.

   Opts (see default-opts): :n-values :k-values :trials :defect-rate
   :seed. All chains of a run share one in-memory database (cells are
   id-disjoint). Returns the raw rows:

     {:n :k :trial :completeness-score :missing-evidence :recall-decision}"
  [opts]
  (let [{:keys [n-values k-values trials defect-rate seed] :as opts}
        (merge default-opts opts)
        uri (str "datomic:mem://nk-completeness-" (UUID/randomUUID))]
    (d/delete-database uri)
    (d/create-database uri)
    (let [conn (d/connect uri)]
      (try
        @(d/transact conn schema/full-schema)
        (vec (for [n n-values
                   k k-values
                   trial (range trials)]
               (experiment-row conn {:n n :k k :trial trial
                                     :defect-rate defect-rate :seed seed})))
        (finally
          (d/release conn)
          (d/delete-database uri))))))

;; =============================================================================
;; Summary, artifacts, and the CLI entry point
;; =============================================================================

(defn summarize-rows
  "Per-(N, K) summary of the raw rows: mean completeness score, the
   recall decision as a do-not-pull rate (decision-readiness), and the
   mean named-defect count. The ticket's summary-row contract — N, K,
   completeness score, recall decision — is met by these columns."
  [rows]
  (->> rows
       (group-by (juxt :n :k))
       (sort-by first)
       (mapv (fn [[[n k] rs]]
               (let [t (count rs)]
                 {:n n
                  :k k
                  :trials t
                  :mean-completeness-score (/ (reduce + (map :completeness-score rs)) t)
                  :do-not-pull-rate (/ (count (filter #(= :do-not-pull
                                                           (get-in % [:recall-decision :decision]))
                                                      rs))
                                       t)
                  :mean-missing-evidence-count (/ (reduce + (map (comp count :missing-evidence) rs))
                                                  t)})))))

(defn- write-csv!
  [path header rows]
  (io/make-parents path)
  (with-open [w (io/writer path)]
    (csv/write-csv w (into [header] rows)))
  path)

(defn- git-commit
  []
  (let [{:keys [exit out]} (shell/sh "git" "rev-parse" "HEAD")]
    (if (zero? exit) (clojure.string/trim out) "unknown")))

(defn manifest
  "Run manifest: the one command, the environment, a timestamp, the
   commit hash, and the published opts (grid, trials, defect rate,
   seed) that make the run attributable and reproducible."
  [opts]
  {:command "clojure -M:nk-experiment"
   :environment {:java-version (System/getProperty "java.version")
                 :os-name (System/getProperty "os.name")
                 :os-version (System/getProperty "os.version")
                 :clojure-version (clojure-version)}
   :timestamp (str (java.time.Instant/now))
   :commit (git-commit)
   :opts opts})

(defn write-experiment-outputs!
  "Run the experiment over opts and write the artifacts to out-dir:

     nk-completeness-raw.csv      one row per (n, k, trial)
     nk-completeness-summary.csv  per-K aggregates
     manifest.edn                 command, environment, timestamp,
                                  commit, opts

   Returns {:rows :summary :manifest :files}. Raw and summary CSVs and
   the manifest are kept together, as the reproducibility docs require."
  ([opts] (write-experiment-outputs! opts "benchmarks/reproducibility/nk/out"))
  ([opts out-dir]
   (let [opts (merge default-opts opts)
         rows (run-experiment opts)
         summary (summarize-rows rows)
         m (manifest opts)
         raw-path (write-csv! (str out-dir "/nk-completeness-raw.csv")
                              ["n" "k" "trial" "completeness-score"
                               "missing-evidence" "recall-decision"]
                              (map (fn [{:keys [n k trial completeness-score
                                                missing-evidence recall-decision]}]
                                     [n k trial completeness-score
                                      (str/join ";" missing-evidence)
                                      (name (:decision recall-decision))])
                                   rows))
         sum-path (write-csv! (str out-dir "/nk-completeness-summary.csv")
                              ["n" "k" "trials" "mean-completeness-score"
                               "do-not-pull-rate" "mean-missing-evidence-count"]
                              (map (fn [{:keys [n k trials mean-completeness-score
                                                do-not-pull-rate
                                                mean-missing-evidence-count]}]
                                     [n k trials (double mean-completeness-score)
                                      (double do-not-pull-rate)
                                      (double mean-missing-evidence-count)])
                                   summary))
         manifest-path (str out-dir "/manifest.edn")]
     (io/make-parents manifest-path)
     (spit manifest-path (with-out-str (pprint/pprint m)))
     {:rows rows
      :summary summary
      :manifest m
      :files {:raw raw-path :summary sum-path :manifest manifest-path}})))

(defn -main
  "Regenerate the NK completeness experiment artifacts under
   benchmarks/reproducibility/nk/out/ with the published defaults."
  [& _args]
  (let [{:keys [summary files]} (write-experiment-outputs! {})]
    (println "NK completeness experiment written:")
    (doseq [[k v] files]
      (println " " k "->" v))
    (println "Per-K summary (score is the 0-1 completeness mean; do-not-pull
  is the decision-readiness rate):")
    (doseq [{:keys [n k trials mean-completeness-score do-not-pull-rate]} summary]
      (println (format "  N=%-2d K=%-2d trials=%-3d mean-score=%.3f do-not-pull=%.3f"
                       n k trials (double mean-completeness-score)
                       (double do-not-pull-rate))))
    (System/exit 0)))
