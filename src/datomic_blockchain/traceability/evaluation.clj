(ns datomic-blockchain.traceability.evaluation
  "The labeled-path evaluation: the checkable harness that records
   completeness score, named missing evidence, and the rubric's recall
   decision per labeled path, then checks the score's agreement with
   the labels.

   This is the evaluation the paper reports: the score is higher on the
   complete lot than on defective lots, the named evidence matches the
   injected defect, and the ranking is consistent with pull /
   do-not-pull. The row shape here is what the NK experiment's summary
   rows extend (ticket 04 adds N and K)."
  (:require [datomic.api :as d]
            [datomic-blockchain.traceability.completeness :as completeness]
            [datomic-blockchain.traceability.recall-rubric :as recall-rubric]))

(defn- assess-labeled-path
  "One evaluation row: the assessment interface over the seeded path,
   stamped with the rubric decision."
  [{:keys [label expected-missing-evidence anchors recall-decision]} db]
  (let [assessment (completeness/assess-completeness
                    db (java.util.UUID/fromString (:entity-id anchors)))]
    {:label label
     :status (:status assessment)
     :completeness-score (:completeness-score assessment)
     :missing-evidence (:missing-evidence assessment)
     :expected-missing-evidence expected-missing-evidence
     :recall-decision recall-decision}))

(defn- complete-scores-above-defective?
  "The do-not-pull negative control scores above every pull path."
  [rows]
  (let [{:keys [complete]} (into {} (map (juxt :label identity) rows))
        defective (remove #(= :complete (:label %)) rows)]
    (and (some? complete)
         (seq defective)
         (every? #(< (:completeness-score %) (:completeness-score complete))
                 defective))))

(defn- expected-evidence-named?
  "Each defective path's named missing evidence includes the signal its
   injected defect must produce."
  [rows]
  (->> rows
       (remove #(= :complete (:label %)))
       (every? #(some #{(:expected-missing-evidence %)} (:missing-evidence %)))))

(defn- ranking-matches-decisions?
  "Every do-not-pull row scores above every pull row — the ranking the
   rubric's stamps imply. Generalizes complete-scores-above-defective?
   (kept separate: agreement-with-labels and agreement-with-decisions
   are distinct claims in the ticket)."
  [rows]
  (let [pull (filter #(= :pull (get-in % [:recall-decision :decision])) rows)
        keep (filter #(= :do-not-pull (get-in % [:recall-decision :decision])) rows)]
    (and (seq pull)
         (seq keep)
         (every? (fn [k] (every? #(< (:completeness-score %) (:completeness-score k))
                                 pull))
                 keep))))

(defn evaluate-labeled-paths!
  "Run the labeled-path evaluation against conn.

   Seeds the labeled paths (recall-rubric/labeled-paths!), assesses each
   through the public completeness interface, and returns:

     {:rows   one record per labeled path:
              {:label :status (recorded for transparency, not itself checked)
               :completeness-score :missing-evidence
               :expected-missing-evidence :recall-decision}
      :checks {:complete-scores-above-defective? boolean
               :expected-evidence-named? boolean
               :ranking-matches-decisions? boolean
               :all-agree? boolean}}

   all-agree? is the single checkable verdict: every agreement check
   holds. A false verdict means the completeness score disagrees with
   the rubric's labels — a finding about the score, not the labels."
  [conn]
  (let [labeled (recall-rubric/labeled-paths! conn)
        db (d/db conn)
        rows (mapv #(assess-labeled-path % db) labeled)
        checks {:complete-scores-above-defective? (complete-scores-above-defective? rows)
                :expected-evidence-named? (expected-evidence-named? rows)
                :ranking-matches-decisions? (ranking-matches-decisions? rows)}]
    {:rows rows
     :checks (assoc checks :all-agree? (every? true? (vals checks)))}))
