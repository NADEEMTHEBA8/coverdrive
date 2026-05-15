# ADR-001: Reframe PCA composite as a descriptive ranking metric, not a regression target

| | |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-05-15 |
| **Author** | Nadeem Theba |
| **Supersedes** | The methodology in *"Predicting Greatest Cricketer by Comparing Different Machine Learning Approaches"* (MSc dissertation, University of Hertfordshire, December 2022). |

---

## TL;DR

The original methodology used a PCA-derived composite score as the **target variable** for a supervised regression task whose **features were the same statistics the PCA was computed from**. The "99% R²" reported in the dissertation was target leakage — the model was learning to invert a deterministic linear function it had been told about both sides of. This ADR documents the flaw, the corrected framing, and what now lives in the codebase.

---

## Context

The MSc project followed the approach in Manage & Scariano (2013), *"An Introductory Application of Principal Components to Cricket Data,"* which constructs a single-number "greatness" composite for batsmen by extracting the first principal component of standardised career statistics (Runs, Batting Average, Strike Rate, 4s, 6s, 100s, 50s). For bowlers, the same technique is applied with sign-flipped weights on Average, Economy, and Strike Rate.

The dissertation extended this in one direction: it added a supervised-learning stage. XGBoost, RandomForest, and AdaBoost regressors were trained to **predict the PC1 score** from the underlying statistics. The reported test-set R² values were:

| Model | R² |
|---|---|
| XGBoost | 0.991 |
| RandomForest | 0.987 |
| AdaBoost | 0.962 |

These were taken at face value at the time. They are not real signal.

## The flaw

PC1 is **a linear function of the same features the model receives as inputs.** Specifically, for batsmen, the loadings extracted by `sklearn.decomposition.PCA` on the standardised feature matrix `Z` give:

```
PC1(player) = 0.458·Z_runs + 0.398·Z_avg + 0.325·Z_sr + 0.406·Z_fours
            + 0.417·Z_sixes + 0.432·Z_centuries_fifties
```

(Coefficients from the dissertation's own PCA output, preserved in `dbt/macros/compute_pca.sql` for traceability.)

The supervised pipeline then fit a regressor whose features were `(runs, avg, sr, fours, sixes, centuries, fifties)` — the **un-standardised originals of the very vector above** — against a target that is a deterministic, closed-form linear combination of those features. A flexible learner (XGBoost) only needs to discover the standardisation constants and recover the linear weights. There is no held-out information for it to learn from; the test set is information-theoretically identical to the training set with respect to this target.

**This is textbook target leakage.** The R² isn't measuring predictive skill — it's measuring how well the model can fit a function whose definition was leaked through the feature set. If the same statistics were given a different label (say, "batsman Z-score from the PC1 axis"), no one would frame it as a learning problem.

How did this get past supervision in 2022? Two reasons:
1. The PCA and the regression were implemented in separate notebooks. The deterministic relationship between them was never written down anywhere; the seam where leakage occurred was conceptual.
2. The published precedent (Manage & Scariano 2013) does the PCA and stops. The supervised extension was the student's own addition, and the failure mode it introduces is invisible if you don't draw the data lineage.

## Decision

PCA is repositioned from **target** to **descriptive metric**, computed inside the warehouse:

1. **Move PCA into the analytics layer.** `dbt/macros/compute_pca.sql` exposes `compute_pca_batsman(...)` and `compute_pca_bowler(...)` as Jinja-templated SQL expressions. They run once, at warehouse build time, against the Silver tables. There is no learned model — these are weighted sums, applied row-wise, that materialise into `mart_top_batsmen` and `mart_top_bowlers`.

2. **Drop the regression stage entirely.** The original XGBoost/RandomForest/AdaBoost code is removed from the pipeline. It is preserved in `docs/legacy/` for provenance and as the basis of this ADR; nothing in the current build calls it.

3. **Communicate the metric honestly.** `mart_top_batsmen` documents the PCA composite as *"an unsupervised summary of career bulk and rate — useful for ranking, not a prediction."* The dbt model comment reproduces the linear formula so any consumer sees the construction in plain SQL.

4. **If a learning problem is ever wanted, change the framing.** The credible supervised version of this problem is **forecasting next-season performance from prior-season statistics** — i.e. predicting `runs_at_t+1` from features observed up to time `t`. That has genuine held-out information and is non-trivial. It is listed under "Future work" in `docs/ARCHITECTURE.md`; it is not part of the current pipeline because doing it properly requires per-innings data, not the career aggregates available on ESPNcricinfo's stat tables.

## Consequences

### Positive

- The pipeline now reports metrics it can defend. `mart_top_batsmen.pca_score` is a transparent linear combination, traceable in SQL.
- The data lineage is cleaner: Bronze (raw scrape) → Silver (cleaned, typed) → Gold (`dim_player`, marts with PCA) is unambiguous. There's no "Gold' " that the model trained on.
- The dbt macro is reviewable and testable — `dbt test` enforces that `pca_score` falls in plausible bounds for the eligible population.
- Removing the model removes a class of failures: model drift, retraining cadence, model registry, online inference, monitoring of model outputs. None of these were ever solved problems in the original work; eliminating the surface eliminates the gap.

### Negative

- The project no longer demonstrates supervised ML. For an ML-engineering portfolio this would be a loss; for a data-engineering portfolio it is **net positive** because the credible story ("I found a flaw and fixed the framing") is more valuable than the spurious one ("look, 99% accuracy").
- Reviewers familiar with Manage & Scariano (2013) may ask "why not predict career averages from early-career stats?" The answer is in `docs/ARCHITECTURE.md` § "Future work": the data needed (ball-by-ball or per-innings records, joined to playing-XI lineups) is materially outside the scope of ESPNcricinfo's flat stat tables. Doing it well requires CricSheet or a similar source.

### Risks accepted

- The PCA loadings in `compute_pca.sql` are **fixed constants from the 2022 PCA**, not recomputed against the current data. This is intentional: it makes the metric stable across pipeline runs (a player's score doesn't change because someone else retired). The trade-off is that the loadings reflect the 2022 player distribution. Documented in the macro header; refresh strategy is an annual PCA recomputation, reviewed by a human, applied via a version bump on the macro.

## Alternatives considered

1. **Keep the regression, evaluate on a different target.**
   Rejected. Any target derivable from the same features has the same leakage issue. Switching to "career runs" as the target while keeping all the per-career statistics as features doesn't fix anything — they're contemporaneous.

2. **Keep the regression, hold out features.**
   Rejected. We could drop, say, `runs` from the features and predict it. This is a real prediction task, but it answers no question anyone cares about ("given everything except your runs, how many runs did you score?") and was not the original framing.

3. **Reframe as next-season forecasting using the existing data.**
   Rejected for v1. The ESPNcricinfo scrape returns career aggregates per player, not time-series. The temporal split needed for a forecasting task does not exist in the source. Doing this properly requires switching data sources, which is its own project. Listed under "Future work."

4. **Drop the dissertation framing entirely and rebuild around a different cricket question.**
   Considered but rejected. The dissertation already produced a substantial pipeline (~5,000 rows of cleaned ODI batting and bowling data, two PCA constructions, three model implementations). Throwing it away loses the provenance that makes this portfolio credible — *"I built this in 2022 for my MSc, then in 2026 caught a flaw and reshaped it into a data platform"* is a more honest story than starting over.

## Lessons (for the postmortem section of the next interview)

1. **Target leakage is a data-lineage problem, not a modeling problem.** No amount of cross-validation, no train/test split, no held-out fold catches a target that was *defined as a function of the features*. The fix isn't in the model code; it's in the diagram of where the data comes from.

2. **Reported accuracy is a hypothesis, not a fact.** A 99% R² on a non-trivial real-world problem should trigger "what's broken?" reflexively. In 2022 I treated it as success. The single most useful instinct I've added since is to assume too-good numbers are diagnostic of a bug somewhere upstream.

3. **Notebooks hide lineage.** Two notebooks — one that did PCA, one that did regression — were enough to obscure that the second was learning a deterministic function of the first. Putting everything in a dbt project, where the dependency graph is explicit and visualisable (`dbt docs serve`), makes this class of error much harder to commit unnoticed.

4. **The most senior thing you can do with old work is correct it in public.** This ADR exists precisely because the right move was not to pretend the dissertation was fine. It is more valuable to a reviewer than a clean pipeline with no scars.

## References

- Manage, A.B.W. and Scariano, S.M. (2013). *An Introductory Application of Principal Components to Cricket Data.* Journal of Statistics Education, 21(3).
- Kaufman, S., Rosset, S., Perlich, C., and Stitelman, O. (2012). *Leakage in Data Mining: Formulation, Detection, and Avoidance.* ACM Transactions on Knowledge Discovery from Data, 6(4).
- Theba, N. (2022). *Predicting Greatest Cricketer by Comparing Different Machine Learning Approaches.* MSc dissertation, University of Hertfordshire. (Preserved under `docs/legacy/` for provenance.)
