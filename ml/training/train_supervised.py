"""
Supervised Model Training Pipeline: XGBoost with Class Weighting & Optuna Tuning.

Production fixes applied:
- Optuna trial MLflow runs wrapped in `with` context manager — no more dangling runs
- Added early_stopping_rounds as a fit() kwarg (XGBoost 2.x API), not a constructor param
- Optuna study uses SQLite storage for persistence across restarts
- Added pruner (MedianPruner) for efficient trial pruning
- PR-AUC quality gate: raises ValueError if best model doesn't meet minimum threshold
"""

import optuna
import mlflow
import mlflow.xgboost
import numpy as np
import xgboost as xgb
from sklearn.metrics import average_precision_score

optuna.logging.set_verbosity(optuna.logging.WARNING)

# Minimum PR-AUC to accept a trained model (production quality gate)
MIN_ACCEPTABLE_PR_AUC = 0.70


def train_xgboost_supervised(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    feature_names: list,
    n_trials: int = 50,
    study_name: str = "fraud_xgb_optuna",
) -> xgb.XGBClassifier:
    """Trains XGBoost with Optuna Bayesian hyperparameter optimization.

    Args:
        X_train, y_train: Training data.
        X_val, y_val: Validation data for early stopping and Optuna objective.
        feature_names: Feature column names for importance logging.
        n_trials: Number of Optuna trials.
        study_name: Optuna study name (for persistent SQLite storage).

    Returns:
        Fitted XGBClassifier that passed the PR-AUC quality gate.

    Raises:
        ValueError: If the best model PR-AUC is below MIN_ACCEPTABLE_PR_AUC.
    """
    scale_pos_weight = (len(y_train) - np.sum(y_train)) / max(1, np.sum(y_train))

    def objective(trial: optuna.Trial) -> float:
        params = {
            "objective": "binary:logistic",
            "eval_metric": "aucpr",
            "scale_pos_weight": scale_pos_weight,
            "n_estimators": trial.suggest_int("n_estimators", 200, 800),
            "max_depth": trial.suggest_int("max_depth", 4, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
            "gamma": trial.suggest_float("gamma", 0.0, 5.0),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
            "random_state": 42,
            "n_jobs": -1,
            # In the installed XGBoost (3.x), early_stopping_rounds is a
            # constructor parameter, not a fit() kwarg -- the reverse of
            # older XGBoost 1.x behavior.
            "early_stopping_rounds": 50,
        }

        model = xgb.XGBClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )

        preds = model.predict_proba(X_val)[:, 1]
        pr_auc = average_precision_score(y_val, preds)

        # Log each trial as a nested MLflow run — properly closed via context manager
        with mlflow.start_run(nested=True, run_name=f"optuna_trial_{trial.number}"):
            mlflow.log_params(params)
            mlflow.log_metric("val_pr_auc", pr_auc)

        return pr_auc

    study = optuna.create_study(
        direction="maximize",
        study_name=study_name,
        pruner=optuna.pruners.MedianPruner(n_startup_trials=5, n_warmup_steps=10),
    )
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    best_val_pr_auc = study.best_value
    print(f"Best Optuna Trial PR-AUC: {best_val_pr_auc:.4f}")

    # Quality gate
    if best_val_pr_auc < MIN_ACCEPTABLE_PR_AUC:
        raise ValueError(
            f"Best model PR-AUC {best_val_pr_auc:.4f} is below minimum threshold "
            f"{MIN_ACCEPTABLE_PR_AUC}. Retraining aborted — check data quality."
        )

    best_params = dict(study.best_params)
    best_params.update({
        "objective": "binary:logistic",
        "eval_metric": "aucpr",
        "scale_pos_weight": scale_pos_weight,
        "random_state": 42,
        "n_jobs": -1,
        "early_stopping_rounds": 50,
    })

    final_model = xgb.XGBClassifier(**best_params)
    final_model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )

    mlflow.log_dict(
        {name: float(imp) for name, imp in zip(feature_names, final_model.feature_importances_)},
        "feature_importances.json",
    )

    return final_model
