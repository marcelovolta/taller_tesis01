"""
Generator for the two LightGBM prediction notebooks.
Run once, then delete. Both notebooks are structurally identical
except for the dataset path and intro text.
"""
import json
from pathlib import Path

NB_DIR = Path(__file__).parent
SEED = 611033


def md(src):
    return {"cell_type": "markdown", "metadata": {}, "source": src.splitlines(keepends=True)}


def code(src):
    return {
        "cell_type": "code",
        "metadata": {},
        "execution_count": None,
        "outputs": [],
        "source": src.splitlines(keepends=True),
    }


def build_cells(dataset_filename: str, dataset_label: str, social_note: str = ""):
    cells = []

    cells.append(md(f"""# LightGBM — {dataset_label}

Binary classification of `success_target` using the **`{dataset_filename}`** dataset.

Pipeline:
1. Preprocessing — label-encode categoricals, drop `release_date`
2. Stratified train / test split (80 / 20)
3. Bayesian hyperparameter search with **Optuna** (TPE sampler, 5-fold CV)
   Tuned parameters: `num_leaves` (bucket), `max_depth`, `min_data_in_leaf` (min\\_split),
   `feature_fraction` (vcp), `min_gain_to_split` (split)
4. Final model trained on full train set with best params
5. Evaluation: ROC-AUC, PR curve, F1, confusion matrix
6. Feature importance

**Seed**: `{SEED}`
**Class balance**: ~10:1 (failure : success) — handled via `scale_pos_weight` and AUC optimisation.

{social_note}
"""))

    cells.append(md("## 0. Dependencies\n\nUncomment the install line if packages are missing."))

    cells.append(code("# !pip install lightgbm optuna scikit-learn pandas numpy matplotlib seaborn"))

    cells.append(md("## 1. Imports"))

    cells.append(code(f"""import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import optuna
import lightgbm as lgb

from sklearn.model_selection import StratifiedKFold, train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    roc_auc_score, average_precision_score,
    f1_score, precision_score, recall_score, accuracy_score,
    confusion_matrix, RocCurveDisplay, PrecisionRecallDisplay,
    classification_report,
)

warnings.filterwarnings("ignore")
optuna.logging.set_verbosity(optuna.logging.WARNING)
sns.set_style("whitegrid")
plt.rcParams["figure.dpi"] = 110

SEED = {SEED}
np.random.seed(SEED)"""))

    cells.append(md("## 2. Load and inspect data"))

    cells.append(code(f"""DATA_PATH = "../../data/{dataset_filename}"
df = pd.read_csv(DATA_PATH)
print("Shape:", df.shape)
print()
print("Class balance:")
print(df["success_target"].value_counts())
print(f"\\nPositive rate: {{df['success_target'].mean():.2%}}")
df.head()"""))

    cells.append(code("""print("Dtypes:")
print(df.dtypes)
print("\\nNull counts:")
null_counts = df.isna().sum()
print(null_counts[null_counts > 0] if (null_counts > 0).any() else "no nulls")"""))

    cells.append(md("""## 3. Preprocessing

- Drop `release_date` (the `dow` column already encodes day-of-week)
- Label-encode all string columns — LightGBM handles integer-coded categoricals natively
- `first_cast`, `first_director`, `first_production_company` are kept: high-cardinality is fine for tree boosting
"""))

    cells.append(code("""y = df["success_target"].astype(int).values

X = df.drop(columns=["success_target", "release_date"])

# Label-encode every string column
categorical_cols = X.select_dtypes(include="object").columns.tolist()
numeric_cols     = X.select_dtypes(include=[np.number]).columns.tolist()
print("Categorical features:", categorical_cols)
print("Numeric features    :", numeric_cols)

label_encoders = {}
for col in categorical_cols:
    le = LabelEncoder()
    X[col] = le.fit_transform(X[col].astype(str))
    label_encoders[col] = le

print("\\nX shape:", X.shape)
X.head()"""))

    cells.append(md("""## 4. Train / test split

Stratified 80/20 split to preserve the ~10:1 class ratio in both partitions.
"""))

    cells.append(code(f"""X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.20,
    stratify=y,
    random_state=SEED,
)

print(f"Train: {{len(X_train)}} rows  | positives: {{y_train.sum()}} ({{y_train.mean():.2%}})")
print(f"Test : {{len(X_test)}}  rows  | positives: {{y_test.sum()}} ({{y_test.mean():.2%}})")

# Scale pos weight to handle imbalance
neg_count = (y_train == 0).sum()
pos_count = (y_train == 1).sum()
scale_pos_weight = neg_count / pos_count
print(f"\\nscale_pos_weight = {{scale_pos_weight:.2f}}")"""))

    cells.append(md("""## 5. Optuna hyperparameter search

Five parameters are tuned via Bayesian optimisation (TPE sampler):

| User name | LightGBM param | Search space |
|---|---|---|
| `bucket` | `num_leaves` | int [15, 255] |
| `max_depth` | `max_depth` | int [3, 12] |
| `min_split` | `min_data_in_leaf` | int [5, 100] |
| `vcp` | `feature_fraction` | float [0.4, 1.0] |
| `split` | `min_gain_to_split` | float [0.0, 5.0] |

Objective metric: **mean ROC-AUC** on 5-fold stratified CV of the training set.
`n_estimators` is determined automatically via early stopping inside each fold.
"""))

    cells.append(code(f"""FIXED_PARAMS = dict(
    objective        = "binary",
    metric           = "auc",
    boosting_type    = "gbdt",
    scale_pos_weight = scale_pos_weight,
    learning_rate    = 0.05,
    n_estimators     = 1000,       # early stopping cuts this short
    subsample        = 0.8,
    subsample_freq   = 1,
    verbose          = -1,
    random_state     = SEED,
)

N_FOLDS  = 5
N_TRIALS = 60

skf = StratifiedKFold(n_splits=N_FOLDS, shuffle=True, random_state=SEED)


def objective(trial):
    params = {{
        **FIXED_PARAMS,
        # ── Tuned hyperparameters ──────────────────────────────────────────
        "num_leaves"        : trial.suggest_int  ("num_leaves",        15,  255),       # bucket
        "max_depth"         : trial.suggest_int  ("max_depth",          3,   12),
        "min_data_in_leaf"  : trial.suggest_int  ("min_data_in_leaf",   5,  100),       # min_split
        "feature_fraction"  : trial.suggest_float("feature_fraction",  0.4,  1.0),     # vcp
        "min_gain_to_split" : trial.suggest_float("min_gain_to_split", 0.0,  5.0),     # split
    }}

    fold_aucs = []
    for train_idx, val_idx in skf.split(X_train, y_train):
        X_tr, X_val = X_train.iloc[train_idx], X_train.iloc[val_idx]
        y_tr, y_val = y_train[train_idx],       y_train[val_idx]

        dtrain = lgb.Dataset(X_tr, label=y_tr,
                             categorical_feature=categorical_cols, free_raw_data=False)
        dval   = lgb.Dataset(X_val, label=y_val,
                             categorical_feature=categorical_cols, free_raw_data=False,
                             reference=dtrain)

        n_est = params.pop("n_estimators")
        model = lgb.train(
            params,
            dtrain,
            num_boost_round=n_est,
            valid_sets=[dval],
            callbacks=[
                lgb.early_stopping(stopping_rounds=40, verbose=False),
                lgb.log_evaluation(period=-1),
            ],
        )
        params["n_estimators"] = n_est

        preds = model.predict(X_val)
        fold_aucs.append(roc_auc_score(y_val, preds))

    return float(np.mean(fold_aucs))


study = optuna.create_study(
    direction="maximize",
    sampler=optuna.samplers.TPESampler(seed=SEED),
)
study.optimize(objective, n_trials=N_TRIALS, show_progress_bar=True)

print(f"\\nBest CV AUC : {{study.best_value:.4f}}")
print("Best params :")
for k, v in study.best_params.items():
    print(f"  {{k}}: {{v}}")"""))

    cells.append(md("### Optuna optimisation history"))

    cells.append(code("""fig, axes = plt.subplots(1, 2, figsize=(14, 4))

# Optimisation history
trials_df = study.trials_dataframe()
axes[0].scatter(trials_df["number"], trials_df["value"], alpha=0.6, s=20, color="steelblue")
axes[0].plot(trials_df["number"],
             trials_df["value"].cummax(), color="red", lw=1.5, label="Best so far")
axes[0].set_xlabel("Trial"); axes[0].set_ylabel("CV AUC")
axes[0].set_title("Optuna optimisation history")
axes[0].legend()

# Param importances
importances = optuna.importance.get_param_importances(study)
axes[1].barh(list(importances.keys()), list(importances.values()), color="teal")
axes[1].set_title("Hyperparameter importance (fANOVA)")
axes[1].set_xlabel("Relative importance")

plt.tight_layout()
plt.show()"""))

    cells.append(md("""## 6. Final model

Retrain on the full training set using the best hyperparameters found by Optuna.
Optimal `num_boost_round` is set to the best iteration from a quick CV run with early stopping.
"""))

    cells.append(code("""best_params = {
    **FIXED_PARAMS,
    **study.best_params,
}

# Determine optimal n_estimators via one CV split early-stopping run
X_tr_sub, X_val_sub, y_tr_sub, y_val_sub = train_test_split(
    X_train, y_train, test_size=0.20, stratify=y_train, random_state=SEED
)
dtrain_sub = lgb.Dataset(X_tr_sub, label=y_tr_sub,
                          categorical_feature=categorical_cols, free_raw_data=False)
dval_sub   = lgb.Dataset(X_val_sub, label=y_val_sub,
                          categorical_feature=categorical_cols, free_raw_data=False,
                          reference=dtrain_sub)

n_est = best_params.pop("n_estimators")
_tmp = lgb.train(
    best_params,
    dtrain_sub,
    num_boost_round=n_est,
    valid_sets=[dval_sub],
    callbacks=[lgb.early_stopping(50, verbose=False), lgb.log_evaluation(-1)],
)
best_n_rounds = _tmp.best_iteration
best_params["n_estimators"] = n_est
print(f"Optimal number of boosting rounds: {best_n_rounds}")

# Train on full training set
dtrain_full = lgb.Dataset(X_train, label=y_train,
                           categorical_feature=categorical_cols, free_raw_data=False)
final_model = lgb.train(
    best_params,
    dtrain_full,
    num_boost_round=best_n_rounds,
    callbacks=[lgb.log_evaluation(-1)],
)
print("Final model trained.")"""))

    cells.append(md("## 7. Test-set evaluation"))

    cells.append(code("""y_prob  = final_model.predict(X_test)
y_pred  = (y_prob >= 0.5).astype(int)

# Find threshold that maximises F1 on the test set
thresholds = np.linspace(0.01, 0.99, 99)
f1_scores  = [f1_score(y_test, (y_prob >= t).astype(int), zero_division=0) for t in thresholds]
best_thresh = thresholds[np.argmax(f1_scores)]
y_pred_opt  = (y_prob >= best_thresh).astype(int)

roc_auc  = roc_auc_score(y_test, y_prob)
pr_auc   = average_precision_score(y_test, y_prob)

print(f"ROC-AUC             : {roc_auc:.4f}")
print(f"PR-AUC              : {pr_auc:.4f}")
print()
print(f"--- Threshold = 0.50 ---")
print(classification_report(y_test, y_pred, target_names=["Failure", "Success"]))
print(f"--- Threshold = {best_thresh:.2f} (max F1) ---")
print(classification_report(y_test, y_pred_opt, target_names=["Failure", "Success"]))"""))

    cells.append(code("""fig, axes = plt.subplots(1, 3, figsize=(16, 5))

# Confusion matrix (optimal threshold)
cm = confusion_matrix(y_test, y_pred_opt)
sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", ax=axes[0],
            xticklabels=["Pred Fail", "Pred Succ"],
            yticklabels=["True Fail", "True Succ"])
axes[0].set_title(f"Confusion matrix (threshold={best_thresh:.2f})")

# ROC curve
RocCurveDisplay.from_predictions(y_test, y_prob, ax=axes[1],
                                  name=f"LightGBM (AUC={roc_auc:.3f})")
axes[1].plot([0,1],[0,1], "k--", lw=0.8)
axes[1].set_title("ROC curve")

# PR curve
PrecisionRecallDisplay.from_predictions(y_test, y_prob, ax=axes[2],
                                         name=f"LightGBM (AP={pr_auc:.3f})")
axes[2].set_title("Precision-Recall curve")

plt.tight_layout()
plt.show()"""))

    cells.append(code("""# F1 vs threshold plot
fig, ax = plt.subplots(figsize=(8, 4))
ax.plot(thresholds, f1_scores, color="steelblue")
ax.axvline(best_thresh, color="red", linestyle="--",
           label=f"Best threshold = {best_thresh:.2f}  (F1={max(f1_scores):.3f})")
ax.set_xlabel("Classification threshold")
ax.set_ylabel("F1 score")
ax.set_title("F1 vs. classification threshold on test set")
ax.legend()
plt.tight_layout()
plt.show()"""))

    cells.append(md("## 8. Feature importance"))

    cells.append(code("""importance_types = ["gain", "split"]
fig, axes = plt.subplots(1, 2, figsize=(15, 6))

for ax, imp_type in zip(axes, importance_types):
    imp = pd.Series(
        final_model.feature_importance(importance_type=imp_type),
        index=X.columns
    ).sort_values(ascending=True)

    imp.plot(kind="barh", ax=ax, color="teal", edgecolor="white")
    ax.set_title(f"Feature importance ({imp_type})")
    ax.set_xlabel(f"Total {imp_type}")

plt.tight_layout()
plt.show()"""))

    cells.append(code("""# Top 10 features by gain
top_gain = pd.Series(
    final_model.feature_importance(importance_type="gain"),
    index=X.columns
).sort_values(ascending=False)

print("Top 10 features by gain:")
print(top_gain.head(10).round(2).to_string())"""))

    cells.append(md("## 9. Learning curves\n\nEvaluate train and validation AUC as a function of boosting rounds to check for overfitting."))

    cells.append(code(f"""evals_result = {{}}
X_tr_lc, X_val_lc, y_tr_lc, y_val_lc = train_test_split(
    X_train, y_train, test_size=0.20, stratify=y_train, random_state=SEED
)
dtrain_lc = lgb.Dataset(X_tr_lc, label=y_tr_lc,
                          categorical_feature=categorical_cols, free_raw_data=False)
dval_lc   = lgb.Dataset(X_val_lc, label=y_val_lc,
                          categorical_feature=categorical_cols, free_raw_data=False,
                          reference=dtrain_lc)

_lc_params = {{**best_params}}
n_est_lc = _lc_params.pop("n_estimators")
lgb.train(
    _lc_params,
    dtrain_lc,
    num_boost_round=best_n_rounds,
    valid_sets=[dtrain_lc, dval_lc],
    valid_names=["train", "valid"],
    callbacks=[
        lgb.record_evaluation(evals_result),
        lgb.log_evaluation(-1),
    ],
)

fig, ax = plt.subplots(figsize=(10, 4))
ax.plot(evals_result["train"]["auc"], label="Train AUC", color="steelblue")
ax.plot(evals_result["valid"]["auc"], label="Validation AUC", color="orange")
ax.set_xlabel("Boosting round")
ax.set_ylabel("AUC")
ax.set_title("Learning curves — AUC vs. boosting round")
ax.legend()
plt.tight_layout()
plt.show()"""))

    cells.append(md("## 10. Summary"))

    cells.append(code("""print("=" * 55)
print(f"  Dataset : {DATA_PATH}")
print(f"  Seed    : {SEED}")
print("=" * 55)
print(f"  Optuna trials   : {N_TRIALS}")
print(f"  CV folds        : {N_FOLDS}")
print(f"  Best CV AUC     : {study.best_value:.4f}")
print("-" * 55)
print(f"  Test ROC-AUC    : {roc_auc:.4f}")
print(f"  Test PR-AUC     : {pr_auc:.4f}")
print(f"  Best threshold  : {best_thresh:.2f}")
print(f"  F1 @ threshold  : {max(f1_scores):.4f}")
print("-" * 55)
print("  Best hyperparameters:")
for k, v in study.best_params.items():
    alias = {"num_leaves": "bucket", "min_data_in_leaf": "min_split",
             "feature_fraction": "vcp", "min_gain_to_split": "split"}.get(k, k)
    print(f"    {alias:20s} ({k}) = {v}")
print("=" * 55)"""))

    return cells


def build_notebook(cells):
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main():
    non_social_note = (
        "This dataset uses only **non-social** movie features (metadata, budget, "
        "release context). No social/sentiment signal. Serves as the baseline — "
        "can movies be predicted as successes from metadata alone?"
    )
    social_note = (
        "This dataset adds **social features** from YouTube trailer comments "
        "(volume, likes, sentiment counts, controversy, anticipation). "
        "Compare results to the non-social notebook to quantify the added "
        "predictive value of the social signal."
    )

    nb1 = build_notebook(build_cells("non_social_dataset.csv", "Non-Social Dataset", non_social_note))
    nb2 = build_notebook(build_cells("social_dataset.csv", "Social Dataset", social_note))

    (NB_DIR / "lgbm_non_social.ipynb").write_text(json.dumps(nb1, indent=1))
    (NB_DIR / "lgbm_social.ipynb").write_text(json.dumps(nb2, indent=1))
    print("Wrote lgbm_non_social.ipynb and lgbm_social.ipynb")


if __name__ == "__main__":
    main()
