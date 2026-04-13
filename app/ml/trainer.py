"""
ML model trainer for the Exotic Signal Factory v2.

Implements:
- Model 1: Entry Classifier — P(trade profitable)
- Model 2: Direction Predictor — P(price up) at multiple horizons
- Model 3: Magnitude Estimator — expected |move|

Training protocol:
- Time-series cross-validation (expanding window, no lookahead)
- Hyperparameter search via Optuna
- SHAP feature importance analysis
- Overfitting guards (train/val gap, regime diversity)

Model serving: export to pickle, load in HB controller at startup.
"""
import logging
import pickle
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    import xgboost as xgb
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    logger.warning("xgboost not installed — ML training unavailable")

try:
    import shap
    HAS_SHAP = True
except ImportError:
    HAS_SHAP = False

try:
    import optuna
    HAS_OPTUNA = True
except ImportError:
    HAS_OPTUNA = False


@dataclass
class ModelMetadata:
    """Metadata stored alongside each trained model."""
    model_name: str
    version: str
    trained_at: str
    feature_cols: List[str]
    n_train_samples: int
    n_val_samples: int
    train_auc: float
    val_auc: float
    shap_top_features: List[str]
    hyperparams: Dict[str, Any]
    regime_performance: Dict[str, float] = field(default_factory=dict)


class SignalModelTrainer:
    """Trains XGBoost models for entry classification, direction, and magnitude."""

    def __init__(
        self,
        model_dir: str = "app/models",
        n_optuna_trials: int = 50,
        random_state: int = 42,
    ):
        self.model_dir = Path(model_dir)
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.n_optuna_trials = n_optuna_trials
        self.random_state = random_state

    def train_entry_classifier(
        self,
        df: pd.DataFrame,
        feature_cols: List[str],
        target_col: str = "profitable",
        n_folds: int = 3,
    ) -> Tuple[Any, ModelMetadata]:
        """Train XGBoost entry classifier: P(trade profitable).

        Uses time-series CV with expanding window.
        Returns trained model + metadata.
        """
        if not HAS_XGB:
            raise RuntimeError("xgboost required: pip install xgboost")

        # Sort by entry timestamp for proper time-series splits
        df = df.sort_values("entry_timestamp").reset_index(drop=True)

        X = df[feature_cols].copy()
        y = df[target_col].values

        # Handle missing values — XGBoost handles NaN natively
        logger.info(
            f"Training entry classifier: {len(X)} samples, "
            f"{len(feature_cols)} features, {y.mean()*100:.1f}% positive rate"
        )

        # Time-series cross-validation folds
        fold_size = len(X) // (n_folds + 1)
        val_scores = []
        train_scores = []

        for fold in range(n_folds):
            train_end = fold_size * (fold + 2)
            val_start = train_end
            val_end = min(val_start + fold_size, len(X))

            if val_end <= val_start:
                continue

            X_train = X.iloc[:train_end]
            y_train = y[:train_end]
            X_val = X.iloc[val_start:val_end]
            y_val = y[val_start:val_end]

            # Optimize hyperparams on first fold, reuse for rest
            if fold == 0 and HAS_OPTUNA:
                best_params = self._optimize_hyperparams(
                    X_train, y_train, X_val, y_val
                )
            elif fold == 0:
                best_params = self._default_params()

            model = xgb.XGBClassifier(
                **best_params,
                random_state=self.random_state,
                eval_metric="auc",
                use_label_encoder=False,
            )
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                verbose=False,
            )

            from sklearn.metrics import roc_auc_score
            train_pred = model.predict_proba(X_train)[:, 1]
            val_pred = model.predict_proba(X_val)[:, 1]

            train_auc = roc_auc_score(y_train, train_pred)
            val_auc = roc_auc_score(y_val, val_pred)
            train_scores.append(train_auc)
            val_scores.append(val_auc)

            logger.info(
                f"  Fold {fold+1}: train AUC={train_auc:.4f}, "
                f"val AUC={val_auc:.4f}, gap={train_auc-val_auc:.4f}"
            )

            # Overfitting guard
            if train_auc - val_auc > 0.1:
                logger.warning(f"  Fold {fold+1}: OVERFITTING detected (gap > 0.1)")

        # Final model: train on all data except last fold (holdout)
        holdout_start = len(X) - fold_size
        X_train_final = X.iloc[:holdout_start]
        y_train_final = y[:holdout_start]
        X_holdout = X.iloc[holdout_start:]
        y_holdout = y[holdout_start:]

        final_model = xgb.XGBClassifier(
            **best_params,
            random_state=self.random_state,
            eval_metric="auc",
            use_label_encoder=False,
        )
        final_model.fit(
            X_train_final, y_train_final,
            eval_set=[(X_holdout, y_holdout)],
            verbose=False,
        )

        from sklearn.metrics import roc_auc_score
        holdout_pred = final_model.predict_proba(X_holdout)[:, 1]
        holdout_auc = roc_auc_score(y_holdout, holdout_pred)

        logger.info(
            f"Final model: holdout AUC={holdout_auc:.4f}, "
            f"mean CV AUC={np.mean(val_scores):.4f}"
        )

        # SHAP analysis
        top_features = self._shap_analysis(final_model, X_holdout, feature_cols)

        # Regime performance (if regime cols available)
        regime_perf = {}
        if "btc_sma50_above_sma200" in df.columns:
            holdout_df = df.iloc[holdout_start:].copy()
            holdout_df["pred_prob"] = holdout_pred
            for regime_val, regime_name in [(1.0, "BULL"), (0.0, "BEAR")]:
                mask = holdout_df["btc_sma50_above_sma200"] == regime_val
                if mask.sum() >= 10:
                    regime_auc = roc_auc_score(
                        holdout_df.loc[mask, "profitable"],
                        holdout_df.loc[mask, "pred_prob"],
                    )
                    regime_perf[regime_name] = regime_auc

        metadata = ModelMetadata(
            model_name="entry_classifier",
            version=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
            trained_at=datetime.now(timezone.utc).isoformat(),
            feature_cols=feature_cols,
            n_train_samples=len(X_train_final),
            n_val_samples=len(X_holdout),
            train_auc=float(np.mean(train_scores)),
            val_auc=holdout_auc,
            shap_top_features=top_features[:10],
            hyperparams=best_params,
            regime_performance=regime_perf,
        )

        # Save model + metadata
        self._save_model(final_model, metadata)

        return final_model, metadata

    def train_direction_predictor(
        self,
        df: pd.DataFrame,
        feature_cols: List[str],
        horizons: List[str] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, ModelMetadata]]:
        """Train direction predictors at multiple horizons.

        For each horizon, trains a binary classifier: P(price up next N bars).
        The target must be pre-computed in the DataFrame as 'direction_{horizon}'.
        """
        if horizons is None:
            # Default: use available direction columns
            horizons = [
                col.replace("direction_", "")
                for col in df.columns
                if col.startswith("direction_")
            ]

        models = {}
        metadatas = {}

        for horizon in horizons:
            target_col = f"direction_{horizon}"
            if target_col not in df.columns:
                logger.warning(f"Missing target column {target_col}, skipping")
                continue

            logger.info(f"\n=== Training direction predictor: {horizon} ===")
            model, meta = self.train_entry_classifier(
                df, feature_cols, target_col=target_col
            )
            meta.model_name = f"direction_{horizon}"
            models[horizon] = model
            metadatas[horizon] = meta

        return models, metadatas

    def train_magnitude_estimator(
        self,
        df: pd.DataFrame,
        feature_cols: List[str],
        target_col: str = "abs_return_pct",
    ) -> Tuple[Any, ModelMetadata]:
        """Train magnitude estimator: expected |move| in next period.

        Uses XGBoost regressor instead of classifier.
        """
        if not HAS_XGB:
            raise RuntimeError("xgboost required: pip install xgboost")

        df = df.sort_values("entry_timestamp").reset_index(drop=True)
        X = df[feature_cols].copy()
        y = df[target_col].values

        logger.info(f"Training magnitude estimator: {len(X)} samples")

        # Simple train/test split (time-ordered)
        split = int(len(X) * 0.8)
        X_train, X_test = X.iloc[:split], X.iloc[split:]
        y_train, y_test = y[:split], y[split:]

        params = self._default_params()
        params.pop("scale_pos_weight", None)

        model = xgb.XGBRegressor(
            **params,
            random_state=self.random_state,
            objective="reg:squarederror",
        )
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )

        from sklearn.metrics import mean_absolute_error, r2_score
        pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, pred)
        r2 = r2_score(y_test, pred)

        logger.info(f"Magnitude estimator: MAE={mae:.4f}, R2={r2:.4f}")

        top_features = self._shap_analysis(model, X_test, feature_cols)

        metadata = ModelMetadata(
            model_name="magnitude_estimator",
            version=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
            trained_at=datetime.now(timezone.utc).isoformat(),
            feature_cols=feature_cols,
            n_train_samples=len(X_train),
            n_val_samples=len(X_test),
            train_auc=0.0,  # Not applicable for regression
            val_auc=r2,  # Using R2 as quality metric
            shap_top_features=top_features[:10],
            hyperparams=params,
        )

        self._save_model(model, metadata)
        return model, metadata

    def _optimize_hyperparams(
        self,
        X_train: pd.DataFrame,
        y_train: np.ndarray,
        X_val: pd.DataFrame,
        y_val: np.ndarray,
    ) -> Dict[str, Any]:
        """Optimize hyperparameters with Optuna."""
        if not HAS_OPTUNA:
            return self._default_params()

        def objective(trial):
            params = {
                "n_estimators": trial.suggest_int("n_estimators", 100, 400),
                "max_depth": trial.suggest_int("max_depth", 2, 5),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.15, log=True),
                "min_child_weight": trial.suggest_int("min_child_weight", 5, 25),
                "subsample": trial.suggest_float("subsample", 0.5, 0.85),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.4, 0.75),
                "gamma": trial.suggest_float("gamma", 0.5, 8),
                "reg_alpha": trial.suggest_float("reg_alpha", 0.1, 30, log=True),
                "reg_lambda": trial.suggest_float("reg_lambda", 1, 30, log=True),
            }

            model = xgb.XGBClassifier(
                **params,
                random_state=self.random_state,
                eval_metric="auc",
                use_label_encoder=False,
            )
            model.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                verbose=False,
            )

            from sklearn.metrics import roc_auc_score
            train_pred = model.predict_proba(X_train)[:, 1]
            val_pred = model.predict_proba(X_val)[:, 1]
            train_auc = roc_auc_score(y_train, train_pred)
            val_auc = roc_auc_score(y_val, val_pred)

            # Penalize overfitting: reduce score when train/val gap is large
            gap = train_auc - val_auc
            penalty = max(0, gap - 0.05) * 2.0  # penalize gaps > 0.05
            return val_auc - penalty

        optuna.logging.set_verbosity(optuna.logging.WARNING)
        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=self.n_optuna_trials, show_progress_bar=False)

        logger.info(f"Optuna best AUC: {study.best_value:.4f}")
        return study.best_params

    def _default_params(self) -> Dict[str, Any]:
        """Default XGBoost hyperparameters (conservative, anti-overfit)."""
        return {
            "n_estimators": 200,
            "max_depth": 3,
            "learning_rate": 0.05,
            "min_child_weight": 15,
            "subsample": 0.6,
            "colsample_bytree": 0.5,
            "gamma": 3,
            "reg_alpha": 1.0,
            "reg_lambda": 10.0,
        }

    def _shap_analysis(
        self,
        model: Any,
        X: pd.DataFrame,
        feature_cols: List[str],
    ) -> List[str]:
        """Compute SHAP values and return top features by importance."""
        if not HAS_SHAP:
            # Fall back to built-in feature importance
            if hasattr(model, "feature_importances_"):
                importances = dict(zip(feature_cols, model.feature_importances_))
                sorted_feats = sorted(importances, key=importances.get, reverse=True)
                return sorted_feats
            return feature_cols

        try:
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X)

            # Mean absolute SHAP value per feature
            if isinstance(shap_values, list):
                shap_values = shap_values[1]  # For binary classifier, use positive class
            mean_abs_shap = np.abs(shap_values).mean(axis=0)
            importance = dict(zip(feature_cols, mean_abs_shap))
            sorted_feats = sorted(importance, key=importance.get, reverse=True)

            logger.info("SHAP top 10 features:")
            for feat in sorted_feats[:10]:
                logger.info(f"  {feat}: {importance[feat]:.4f}")

            return sorted_feats

        except Exception as e:
            logger.warning(f"SHAP analysis failed: {e}")
            return feature_cols

    def _save_model(self, model: Any, metadata: ModelMetadata) -> Path:
        """Save model and metadata to disk."""
        version = metadata.version
        name = metadata.model_name

        model_path = self.model_dir / f"{name}_{version}.pkl"
        meta_path = self.model_dir / f"{name}_{version}_meta.pkl"

        with open(model_path, "wb") as f:
            pickle.dump(model, f)
        with open(meta_path, "wb") as f:
            pickle.dump(metadata, f)

        # Also save as "latest" symlink
        latest_model = self.model_dir / f"{name}_latest.pkl"
        latest_meta = self.model_dir / f"{name}_latest_meta.pkl"

        # Remove old latest if exists
        latest_model.unlink(missing_ok=True)
        latest_meta.unlink(missing_ok=True)

        with open(latest_model, "wb") as f:
            pickle.dump(model, f)
        with open(latest_meta, "wb") as f:
            pickle.dump(metadata, f)

        logger.info(f"Saved model to {model_path}")
        return model_path

    @staticmethod
    def load_model(
        model_dir: str = "app/models",
        model_name: str = "entry_classifier",
    ) -> Tuple[Any, ModelMetadata]:
        """Load latest model + metadata from disk."""
        model_dir = Path(model_dir)
        model_path = model_dir / f"{model_name}_latest.pkl"
        meta_path = model_dir / f"{model_name}_latest_meta.pkl"

        if not model_path.exists():
            raise FileNotFoundError(f"No model found at {model_path}")

        with open(model_path, "rb") as f:
            model = pickle.load(f)
        with open(meta_path, "rb") as f:
            metadata = pickle.load(f)

        return model, metadata
