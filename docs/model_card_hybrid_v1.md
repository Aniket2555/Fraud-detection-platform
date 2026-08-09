# Model Card: Hybrid Fraud Detection Ensemble v1

## Model Overview
- **Model Name:** Hybrid Fraud Detection Stacking Ensemble
- **Version:** v1.0
- **Architecture:** Stacking Ensemble combining Supervised XGBoost, Deep Autoencoder (PyTorch), and Isolation Forest (Scikit-Learn) with Isotonic Score Calibration and a Logistic Regression Meta-Learner.
- **Input Features:** 43 features across 6 feature families (Stateless, Velocity, Geo-Velocity, Baselines, Merchant Risk, Graph).
- **Primary Metric:** Precision-Recall AUC (PR-AUC) >= 0.80.

---

## Intended Use
- **Primary Application:** Real-time transaction fraud risk scoring (<100ms SLA).
- **Target Actions:**
  - `score < 0.30`: Auto-approve (low risk)
  - `0.30 <= score < 0.75`: Step-up authentication / 3DS OTP challenge
  - `score >= 0.75`: Auto-decline & send to manual audit queue

---

## Component Models & Weights
1. **XGBoost Classifier:** Supervised gradient boosting trained with scale_pos_weight.
2. **PyTorch Deep Autoencoder:** Unsupervised reconstruction loss trained on legitimate transactions with Trimmed MSE Loss.
3. **Isolation Forest:** Tree-partitioning anomaly detector trained on legitimate transactions.
4. **Isotonic Calibrator:** Maps raw component scores to calibrated probabilities.
5. **Stacking Meta-Learner:** Logistic Regression combining calibrated component probabilities.

---

## Serving & Circuit Breaker Fallbacks
- **Tier 1 (Partial Features):** Default imputations for missing online features (+0.15 risk bias).
- **Tier 2 (Rule Engine Fallback):** Amount-based threshold rules when model times out (>80ms).
- **Tier 3 (Emergency Net):** Default approve transactions < $500 during total system outage.
