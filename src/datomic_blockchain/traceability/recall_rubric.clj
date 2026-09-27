(ns datomic-blockchain.traceability.recall-rubric
  "The recall rubric: written rules that stamp a provenance path
   pull or do-not-pull from named evidence defects.

   The rubric is deliberately independent of the completeness score —
   it reads only the named defects. Agreement between the score and
   these labels is the evaluation in ticket 03; deriving labels from
   the score would make that check circular.

   The written form lives in docs/recall-rubric.md; this namespace is
   the same rubric as data, plus the fixture loader that stamps the
   labeled paths."
  (:require [datomic-blockchain.traceability.sample-data :as sample]))

(def rubric
  "The rubric as data: path label -> recall decision + rationale.
   Precautionary principle: any named evidence defect pulls the lot;
   only a path with no named defects is decision-ready (do-not-pull)."
  {:complete
   {:decision :do-not-pull
    :rationale "No named evidence defects: the provenance path names the generating activity, the responsible agent, and a valid chronology. The lot is decision-ready; do not pull."}

   :missing-activity
   {:decision :pull
    :rationale "The generating activity is absent: there is no evidence of what was done to this lot. Pull until the step is documented."}

   :missing-agent
   {:decision :pull
    :rationale "No responsible agent is attached to the generating activity: due diligence on the responsible party is impossible. Pull until responsibility is named."}

   :missing-start-time
   {:decision :pull
    :rationale "The activity carries no start time: chronology cannot be established, so the recall window cannot be ordered. Pull until the timeline is evidenced."}

   :invalid-time-order
   {:decision :pull
    :rationale "The recorded times contradict (end before start): the evidence itself is internally inconsistent. Pull until the chronology is corrected."}})

(defn decision-for
  "Stamp a path label (:complete or a defect keyword) with the rubric's
   recall decision and written rationale. Unknown labels are treated as
   undocumented evidence and pull, precautionarily."
  [label]
  (or (get rubric label)
      {:decision :pull
       :rationale (str "Unknown path label " label
                       ": undocumented evidence, pull precautionarily.")}))

(defn decision-for-evidence
  "The rubric rule operationalized for generated paths (the NK
   experiment): stamp from the named missing evidence the assessment
   reports — no named defects is do-not-pull, any named defect pulls.
   Reads only the names, never the 0-1 score."
  [missing-evidence]
  (if (seq missing-evidence)
    {:decision :pull
     :rationale (str "Named evidence defects: "
                     (clojure.string/join ", " (map str missing-evidence))
                     ". Pull until the evidence is complete.")}
    (get rubric :complete)))

(defn labeled-paths!
  "Load the labeled-path fixtures into conn and return them stamped:

     {:label :complete | <defect-keyword>
      :expected-missing-evidence signal-name the assessment must name
      :anchors {:qr-code :batch-id :entity-id :activity-id}
      :recall-decision {:decision :pull|:do-not-pull :rationale}}

   Seeds the complete UHT chocolate path (the do-not-pull negative
   control) plus one defect variant per rubric defect type. All five
   coexist in one database, individually addressable by QR."
  [conn]
  (let [base (sample/seed-uht-sample! conn)
        complete {:label :complete
                  :expected-missing-evidence nil
                  :anchors (:benchmark-anchors base)
                  :recall-decision (decision-for :complete)}]
    (into [complete]
          (map (fn [label]
                 (let [{:keys [anchors expected-missing-evidence]}
                       (sample/seed-labeled-path! conn label)]
                   {:label label
                    :expected-missing-evidence expected-missing-evidence
                    :anchors anchors
                    :recall-decision (decision-for label)})))
          (sample/labeled-path-defects))))
