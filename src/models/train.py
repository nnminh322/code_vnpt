# !pip install optbinning==0.20.1 --proxy=http://10.144.13.144:3129
# !pip install scipy==1.8.1 --proxy=http://10.144.13.144:3129
# !pip install catboost --proxy=http://10.144.13.144:3129
# !pip install feature-engine --proxy=http://10.144.13.144:3129
# !pip install boruta  --proxy=http://10.144.13.144:3129
# !pip install optuna==3.0.3 --proxy=http://10.144.13.144:3129
## !pip install conditional_independence  --proxy=http://10.144.13.144:3129

import pandas as pd
from pandas.api.types import is_object_dtype
import numpy as np
from numpy import sqrt, log1p, abs, ix_, diag, corrcoef, errstate, cov, mean
from numpy.linalg import inv, pinv
import matplotlib.pyplot as plt

from math import erf
from datetime import datetime
from itertools import combinations
from typing import Union, Callable, List, Dict

import optuna
from boruta import BorutaPy
from optbinning import OptimalBinning
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import TimeSeriesSplit
from scipy.stats import zscore, median_abs_deviation
from feature_engine.selection import DropConstantFeatures, SelectByInformationValue, DropDuplicateFeatures, MRMR
# from conditional_independence import partial_correlation_suffstat, partial_correlation_test

bucket_name = "media_os_blueinfo_dev_2"
bucket_name_cos = f"cos://{bucket_name}.datalake"

def prepare_data(
    spark, data_path, output_path=False, snapshot: datetime = datetime(2024, 1, 1), snapshot_col = 'SNAPSHOT', drop_columns:list = None,
    label_path:str = None, key:tuple = ("PHONE_NUMBER", "msisdn"), mode:str = 'left',
    random_seed = 100, train_size = 0.8
):
    df = spark.read.parquet(f"{bucket_name_cos}/{data_path}")
    
    # join label 
    if label_path: 
        join_df = spark.read.parquet(f"{bucket_name_cos}/{label_path}")
        if key[0] == key[1]: 
            df = join_df.join(df, on=key[0], how=mode)
        else: 
            df = join_df.join(df, join_df[key[0]] == df[key[1]], how=mode)
    
    # drop unwanted columns 
    if drop_columns: 
        df = df.drop(*drop_columns)
    
    # split data 
    dev_set = df.filter(F.col(snapshot_col) < snapshot).drop(snapshot_col)
    train_set, val_set = dev_set.randomSplit(weights=[train_size, 1-train_size], seed=random_seed)
    test_set = df.filter(F.col(snapshot_col) >= snapshot).drop(snapshot_col)
    
    # save data
    if output_path: 
        train_set.write.parquet(f"{bucket_name_cos}/{output_path}/train_dataset.parquet", mode='overwrite')
        val_set.write.parquet(f"{bucket_name_cos}/{output_path}/val_dataset.parquet", mode='overwrite')
        test_set.write.parquet(f"{bucket_name_cos}/{output_path}/test_dataset.parquet", mode='overwrite')
    
    return train_set.toPandas(), val_set.toPandas(), test_set.toPandas()

def opt_binning_fit(data, features: list, label: str, dtype = "numerical", solver = "cp"): 
    df = data.copy()
    res_bin = {} 
    for feature in features: 
        try: 
            opt = OptimalBinning(
                name = feature,
                dtype=dtype,
                solver=solver,
                special_codes = [np.nan]
            )
            opt.fit(df[feature], df[label])
            res_bin[feature] = opt
        except Exception as e: 
            print(f"[Fit Error] {feature}: {e}")
            
    return res_bin

def opt_binning_transform(data, features, res_bin, metric = "woe", metric_missing=np.nan, metric_special=np.nan):
    df = data.copy()
    for feature in features: 
        try: 
            df[feature] = res_bin[feature].transform(df[feature],
                                                    metric = metric,
                                                    metric_missing = metric_missing,
                                                    metric_special=metric_missing)
            
        except Exception as e: 
            print(f"[Transform Error] {feature}: {e}")
    
    return df

def calculate_woe_iv(X: pd.Series, y: pd.Series, variable: str, max_bins: int = 10) -> float: 
    dtype = 'categorical' if is_object_dtype(X) else "numerical"
    optb = OptimalBinning(name=variable, dtype=dtype, max_n_bins=max_bins if dtype == 'numerical' else None)
    optb.fit(X, y)
    
    binning_table = optb.binning_table.build()
    iv = binning_table['IV'].max()
    return iv

def calc_iv(df, features, target_col, max_bins=10):
    results = []
    
    for feature in features: 
        try: 
            iv = calculate_woe_iv(df[feature], df[target_col], variable=feature, max_bins=max_bins)
            results.append({"feature" : feature, "IV" : iv})
        except Exception as e: 
            print(f"[Fit Error] {feature}: {e}")
    return pd.DataFrame(results).sort_values(by="IV", ascending=True).reset_index(drop=True) 

def high_correlation_filter(df, iv_df, target_col, threshold=0.85):
    df_copy = df.copy() 
    corr_matrix = df_copy.corr(numeric_only=True).abs()
    
    remove_feature = set() 
    features = [col for col in df.columns if col != target_col]
    
    for i in range(len(features)):
        for j in range(i+1, len(features)): 
            f1, f2 = features[i], features[j]
            corr_val = corr_matrix.loc[f1, f2]
            
            if corr_val > threshold: 
                try: 
                    iv1 = iv_df.loc[iv_df['feature'] == f1, 'IV'].values[0]
                    iv2 = iv_df.loc[iv_df['feature'] == f2, 'IV'].values[0]
                    
                    # remove the feature with lower IV
                    if iv1 < iv2 and f1 not in remove_feature: 
                        remove_feature.add(f1)
                    elif iv2 < iv1 and f2 not in remove_feature: 
                        remove_feature.add(f2)
                except Exception as e: 
                    print(f"[IV Lookup Error] {f1}, {f2}: {e}")
                    
    selected_features = [f for f in features if f not in remove_feature]
    print(f"{len(remove_feature)} features are removed")
    return selected_features

def drop_high_volatility_features(
    df_ref: pd.DataFrame,
    df_cur: pd.DataFrame,
    features: List[str],
    method: str = "psi",  # or "custom"
    threshold: float = 0.2,
    custom_metric: Callable[[pd.Series, pd.Series], float] = None,
    bins: int = 10,
    epsilon: float = 1e-6
) -> List[str]:
    """
    Drop features whose distribution shift between df_ref and df_cur exceeds threshold.

    Parameters:
    - df_ref: Reference DataFrame (baseline period)
    - df_cur: Current DataFrame (comparison period)
    - features: List of feature names to evaluate
    - method: 'psi', or 'custom'
    - threshold: Threshold above which a feature is dropped
    - custom_metric: Custom callable with signature (ref_series, cur_series) -> float
    - bins: Number of bins to use for PSI

    Returns:
    - List of kept features
    """

    def calc_psi(expected, actual, num_bins=10,):
        bins = pd.qcut(expected, q=num_bins, duplicates='drop')
        bin_edges = bins.cat.categories

        expected_freq = pd.value_counts(bins, normalize=True).sort_index() + epsilon
        actual_bins = pd.cut(actual, bins=bin_edges)
        actual_freq = pd.value_counts(actual_bins, normalize=True).sort_index() + epsilon

        expected_freq = expected_freq.reindex(bin_edges, fill_value=1e-5)
        actual_freq = actual_freq.reindex(bin_edges, fill_value=1e-5)

        return np.sum((expected_freq - actual_freq) * np.log(expected_freq / actual_freq))

    dropped = []

    for col in features:
        try:
            if method == "psi":
                score = calc_psi(df_ref[col], df_cur[col], bins)
            elif method == "custom" and custom_metric:
                score = custom_metric(df_ref[col], df_cur[col])
            else:
                raise ValueError("Invalid method or missing custom_metric")
            if score > threshold:
                dropped.append(col)

        except Exception as e:
            print(f"[Error] {col}: {e}")

    kept_features = [f for f in features if f not in dropped]
    return kept_features

def drop_constant_features(
    df: pd.DataFrame,
    variables: list = None,
    tol: float = 1,
    missing_values: str = 'raise',
    confirm_variables: bool = False,
    return_dropped: bool = False
) -> pd.DataFrame | tuple:
    """
    Drop constant or quasi-constant features using feature_engine's DropConstantFeatures.

    Parameters:
    - df: Input DataFrame
    - variables: List of variables to evaluate. If None, all features are considered.
    - tol: Threshold for dropping (1 = constant, 0.98 = quasi-constant)
    - missing_values: 'raise' or 'ignore' (how to handle missing values)
    - confirm_variables: Whether to confirm that variables exist in the dataframe
    - return_dropped: If True, returns a tuple (clean_df, dropped_features)

    Returns:
    - Cleaned DataFrame or (DataFrame, dropped_feature_list) if return_dropped=True
    """
    transformer = DropConstantFeatures(
        variables=variables,
        tol=tol,
        missing_values=missing_values,
        confirm_variables=confirm_variables
    )

    transformer.fit(df)
    df_clean = transformer.transform(df)

    if return_dropped:
        return df_clean, transformer.features_to_drop_
    return df_clean


def drop_low_variance_features(df: pd.DataFrame, threshold: float = 0.01) -> pd.DataFrame:
    """
    Remove features with variance lower than threshold.

    Parameters:
    - df: Input DataFrame (should be numeric)
    - threshold: Minimum variance required to keep a feature

    Returns:
    - Cleaned DataFrame with high-variance features
    """
    numeric_df = df.select_dtypes(include=[np.number])
    variances = numeric_df.var()
    selected_cols = variances[variances >= threshold].index
    return df[selected_cols]

def drop_low_iv_features(
    df: pd.DataFrame,
    target: str,
    variables: list = None,
    threshold: float = 0.02,
    bins: int = 5,
    strategy: str = "equal_frequency",
    return_iv: bool = False
) -> pd.DataFrame | tuple:
    """
    Drop features with IV below a threshold using feature_engine.

    Parameters:
    - df: Input DataFrame (includes features and target)
    - target: Name of target column
    - variables: List of features to evaluate (default: all except target)
    - threshold: IV threshold (e.g., 0.02 = weak predictive power)
    - bins: Number of bins for WOE discretization
    - strategy: Binning strategy ("equal_width", "equal_frequency", or "quantile")
    - return_iv: If True, return IV values as dict alongside transformed DataFrame

    Returns:
    - DataFrame (cleaned), or (DataFrame, dict of IV values) if return_iv=True
    """
    selector = SelectByInformationValue(
        threshold=threshold,
        bins=bins,
        strategy=strategy,
        variables=variables
    )

    selector.fit(df, df[target])
    df_transformed = selector.transform(df)

    if return_iv:
        return df_transformed, selector.information_values_
    return df_transformed

def drop_duplicate_features(
    df: pd.DataFrame,
    variables: list = None,
    missing_values: str = "ignore",  # 'ignore' or 'raise'
    confirm_variables: bool = False,
    return_dropped: bool = False
) -> pd.DataFrame | tuple:
    """
    Drop duplicate features using feature_engine's DropDuplicateFeatures.

    Parameters:
    ----------
    df : pd.DataFrame
        Input DataFrame (features only).
    variables : list, optional
        List of feature names to consider. If None, all columns are checked.
    missing_values : str, default='ignore'
        Whether to ignore or raise error on missing values.
    confirm_variables : bool, default=False
        If True, removes features in `variables` list that are not in df.
    return_dropped : bool, default=False
        If True, also return a list of dropped duplicate features.

    Returns:
    -------
    pd.DataFrame or (pd.DataFrame, list)
        Cleaned DataFrame, or tuple with dropped feature names.
    """
    dropper = DropDuplicateFeatures(
        variables=variables,
        missing_values=missing_values,
        confirm_variables=confirm_variables
    )

    dropper.fit(df)
    df_cleaned = dropper.transform(df)

    if return_dropped:
        return df_cleaned, dropper.features_to_drop_
    return df_cleaned

def select_important_features(
    X: pd.DataFrame,
    y: pd.Series,
    estimator,
    top_k: int = None,
    threshold: float = None,
    accumulate: float = None,
    return_importance: bool = False,
    class_weight: list | None = None
) -> pd.DataFrame | tuple:
    """
    Select features based on model feature_importances_ using accumulate > top-k > threshold.

    Parameters:
    ----------
    X : pd.DataFrame
        Input features.
    y : pd.Series
        Target variable.
    estimator : scikit-learn compatible model (must support .feature_importances_)
    top_k : int, optional
        Keep only top k features by importance.
    threshold : float, optional
        Keep all features with importance ≥ threshold.
    accumulate : float, optional
        Keep features whose cumulative importance ≤ accumulate (e.g., 0.95).
    return_importance : bool
        If True, also return feature importance DataFrame.
    class_weight : optional
        Used if estimator supports class_weight (e.g., RandomForest, CatBoost).

    Returns:
    --------
    - Filtered X with selected features (optionally with importance table).
    """
    if hasattr(estimator, "class_weight") and class_weight is not None:
        estimator.set_params(class_weight=class_weight)

    estimator.fit(X, y)

    if not hasattr(estimator, "feature_importances_"):
        raise ValueError("Estimator must have .feature_importances_ attribute.")
    
    importance_df = pd.DataFrame({
        "feature": X.columns,
        "importance": estimator.feature_importances_
    }).sort_values(by="importance", ascending=False).reset_index(drop=True)
    
    # Cumulative importance
    if accumulate is not None:
        importance_df["cumulative"] = importance_df["importance"].cumsum()
        selected_features = importance_df[importance_df["cumulative"] <= accumulate*100]["feature"].tolist()
    elif top_k is not None:
        selected_features = importance_df["feature"].tolist()[:top_k]
    elif threshold is not None:
        selected_features = importance_df[importance_df["importance"] >= threshold]["feature"].tolist()
    else:
        raise ValueError("You must specify one of: top_k, threshold, or accumulate.")

    X_selected = X[selected_features]

    if return_importance:
        return X_selected, importance_df
    return X_selected

def drop_mrmr_features(
    df: pd.DataFrame,
    target: str,
    method: str = "MIQ",         # 'MIQ' or 'MID'
    max_features: int = 200,      # Number of features to keep
    return_selected: bool = False,
    scoring: str = 'roc_auc'
) -> pd.DataFrame | tuple:
    """
    Drop features using the MRMR (Minimum Redundancy Maximum Relevance) algorithm
    via feature_engine's MRMR transformer.

    Parameters:
    - df: Input DataFrame with features + target
    - target: Target column name
    - method: MRMR strategy: 'MIQ' (default) or 'MID'
    - max_features: Number of top features to keep
    - regression: Set True for regression, False for classification
    - return_selected: If True, also return selected feature names

    Returns:
    - DataFrame with selected features (optionally with feature names)
    """
    selector = MRMR(
        method=method,
        max_features=max_features if max_features <= len(df.columns)-1 else len(df.columns)-1,
        regression=False,
        n_jobs = -1, 
        scoring = scoring
    )

    selector.fit(df, df[target])
    df_selected = selector.transform(df)

    if return_selected:
        selected = [f for f in df.columns if f in df_selected.columns and f != target]
        return df_selected, selected

    return df_selected

def handle_outliers_iqr(df, column, threshold=1.5, mode="drop"):

    q1 = df[column].quantile(0.25)
    q3 = df[column].quantile(0.75)
    iqr = q3 - q1
    lower = q1 - threshold * iqr
    upper = q3 + threshold * iqr

    if mode == "drop":
        return df[df[column].between(lower, upper)]
    elif mode == "clip":
        df = df.copy()
        df[column] = df[column].clip(lower=lower, upper=upper)
        return df

def handle_outliers_zscore(df, column, threshold=3.0, mode="drop"):

    z = np.abs(zscore(df[column], nan_policy="omit"))

    if mode == "drop":
        return df[z < threshold]
    elif mode == "clip":
        df = df.copy()
        mean = df[column].mean()
        std = df[column].std()
        lower = mean - threshold * std
        upper = mean + threshold * std
        df[column] = df[column].clip(lower=lower, upper=upper)
        return df

def handle_outliers_mad(df, column, threshold=3.0, mode="drop", nan_policy='propagate'):
    """ Median Absolute Deviation) """

    median = np.median(df[column].values)
    mad = np.abs(median_abs_deviation(df[column], nan_policy=nan_policy))
    
    if mad == 0:
        return df

    scores = 0.6745 * (df[column] - median) / mad
    outlier_mask = np.abs(scores) > threshold

    if mode == "drop":
        return df[~outlier_mask]

    elif mode == "clip":
        df = df.copy()
        valid_range = df.loc[~outlier_mask, column]
        lower = valid_range.min()
        upper = valid_range.max()
        df[column] = df[column].clip(lower=lower, upper=upper)
        return df
    
def detect_drift_with_boruta(
    X: pd.DataFrame,
    y: pd.Series,
    model,
    max_iter: int = 100,
    random_state: int = 42
) -> pd.DataFrame:
    """
    Uses Boruta to detect features with potential drift by comparing them to shadow features.
    Support only machine learning model of sklearn
    Parameters:
    ----------
    X : pd.DataFrame
        Input features (from test or current period).
    y : pd.Series
        Target variable.
    model : scikit-learn compatible model (must support .fit() and .feature_importances_)
        Example: RandomForestClassifier, XGBClassifier, ExtraTreesClassifier, etc.
    max_iter : int
        Number of Boruta iterations (default=100).
    random_state : int
        Random seed for reproducibility.

    Returns:
    -------
    pd.DataFrame with:
    - feature: column name
    - ranking: Boruta ranking
    - support: True if feature is accepted
    - drift_flag: 'drift' or 'stable'
    """
    boruta = BorutaPy(
        estimator=model,
        n_estimators='auto',
        max_iter=max_iter,
        random_state=random_state,
        verbose=1
    )
    
    boruta.fit(X.values, y.values)
    
    result_df = pd.DataFrame({
        'feature': X.columns,
        'ranking': boruta.ranking_,
        'support': boruta.support_,
        'drift_flag': ['stable' if s else 'drift' for s in boruta.support_]
    })

    return result_df.sort_values(by='ranking')

def gini(y_true, y_score):
    auc = roc_auc_score(y_true, y_score)
    return 2 * auc - 1

def time_series_cv(
    X: Union[pd.DataFrame, np.ndarray],
    y: Union[pd.Series, np.ndarray],
    model,
    n_splits: int = 5,
    scoring: Callable = roc_auc_score,
    score_name: str = "roc_auc",
    verbose: bool = True,
    return_predictions: bool = False
) -> pd.DataFrame:
    """
    Generic function for time series cross-validation.

    Parameters:
    ----------
    X : Features (DataFrame or array)
    y : Target (Series or array)
    model : scikit-learn estimator (must implement .fit() and .predict())
    n_splits : Number of cross-validation splits
    scoring : Scoring function with signature (y_true, y_pred)
    score_name : Name for the metric to display/return
    verbose : Whether to print fold results
    return_predictions : Whether to return predictions per fold

    Returns:
    --------
    DataFrame with scores per fold (and predictions if requested)
    """
    tscv = TimeSeriesSplit(n_splits=n_splits)
    results = []

    for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        model.fit(X_train, y_train)
        preds = model.predict(X_test)
        score = scoring(y_test, preds)  # e.g. RMSE

        if verbose:
            print(f"Fold {fold+1} — {score_name}: {score:.4f}")

        result = {
            "fold": fold + 1,
            score_name: score
        }

        if return_predictions:
            result.update({
                "y_test": y_test,
                "y_pred": preds
            })

        results.append(result)

    return pd.DataFrame(results)

def assign_score_segment(
    df: pd.DataFrame,
    score_col: str,
    bins: List[Union[int, float]] = [0, 579, 669, 739, 799, float('inf')],
    labels: List[str] = ['Poor', 'Fair', 'Good', 'Very Good', 'Excellent'],
    new_col: str = 'score_segment',
    right: bool = True
) -> pd.DataFrame:
    """
    Assigns a score segment label to each row based on score bins.

    Parameters:
    -----------
    df : pd.DataFrame
        Input DataFrame containing score column.
    score_col : str
        Name of the column containing the score.
    bins : list of int or float
        Bin edges (must be one more than number of labels).
    labels : list of str
        Labels corresponding to each bin.
    new_col : str
        Name of the new column to store the segment label.
    right : bool
        Indicates whether bins include the rightmost edge.

    Returns:
    --------
    pd.DataFrame with new column assigned to segment.
    """
    df = df.copy()
    df[new_col] = pd.cut(df[score_col], bins=bins, labels=labels, right=right)
    return df

def gini_multi_segment_over_time(
    df,
    date_col: str,
    y_true_col: str,
    y_pred_col: str,
    segment_col: str = 'score_segment', 
    segments: list = None,  # List of segment values to include
    time_freq: str = 'M',
    plot: bool = True
):
    """
    Track Gini over time for multiple segments on the same plot.

    Parameters:
    -----------
    df : pd.DataFrame
        Input data with date, segment, label, and model score.
    date_col : str
        Date column in yyyymmdd format.
    y_true_col : str
        Binary target label (e.g., renewed).
    y_pred_col : str
        Predicted score or probability.
    segment_col : str
        Column that holds the segment label (e.g., 'score_segment').
    segments : list
        List of segment values to plot (if None, plot all unique).
    time_freq : str
        Time period grouping (e.g., 'M' = monthly).
    plot : bool
        Whether to plot the Gini trends.

    Returns:
    --------
    pd.DataFrame with columns: period, segment, gini
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], format="%Y%m%d")
    df['period'] = df[date_col].dt.to_period(time_freq).dt.to_timestamp()

    if segments is None:
        segments = df[segment_col].dropna().unique()

    result = []

    for segment in segments:
        segment_df = df[df[segment_col] == segment]
        for period, group in segment_df.groupby('period'):
            if group[y_true_col].nunique() > 1:
                g = gini(group[y_true_col], group[y_pred_col])
            else:
                g = np.nan
            result.append({'period': period, 'segment': segment, 'gini': g})

    gini_df = pd.DataFrame(result)
    gini_df = gini_df.sort_values(by=['segment', 'period'])
    gini_df['segment'] = pd.Categorical(gini_df['segment'], categories=segments, ordered=True)

    if plot:
        plt.figure(figsize=(12, 6))
        for segment in segments:
            plot_data = gini_df[gini_df['segment'] == segment]
            plt.plot(plot_data['period'], plot_data['gini'], marker='o', label=str(segment))

        plt.title('Gini Over Time by Segment')
        plt.xlabel('Time')
        plt.ylabel('Gini Coefficient')
        plt.xticks(rotation=45)
        plt.legend(title=segment_col)
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    return gini_df

def objective(trial):
    
    model = CatBoostClassifier(cat_features=cat_features,iterations=trial.suggest_int('iterations', 100, 3000),
        learning_rate= trial.suggest_loguniform('learning_rate', 1e-5, 0.1),
        depth=trial.suggest_int('depth', 3, 6),
        l2_leaf_reg=trial.suggest_loguniform('l2_leaf_reg', 3, 10),
        border_count=trial.suggest_int('border_count', 32, 255),
        random_strength=trial.suggest_int('random_strength', 1, 10),  
         bagging_temperature=trial.suggest_categorical('bagging_temperature', [0, 0.5, 1, 10, 100]), 
        loss_function= 'Logloss',
        eval_metric='AUC',
        random_seed= 42,
        init_model = init_model)
    model.fit(X_train, y_train, eval_set=(X_val, y_val), verbose=False)
    
    y_pred = model.predict_proba(X_val)[:, 1] 
    roc_auc = roc_auc_score(y_val, y_pred)
    
    return roc_auc


def tuning(): 
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=50)
    best_params=study.best_params
    return best_params

def weighted_roc_auc(weights, models_preds, y_true):
    final_pred = np.sum([weights[i] * models_preds[i] for i in range(len(weights))]) 


def merge_model(X, y, models, grid_step=21):
    models_preds = [model.predict_proba(X)[:,1] for model in models]
    n_models = len(models)

    best_roc_auc = -np.inf
    best_weights = np.one(n_models) /n_models

    weight_grid = np.linspace(0, 1, grid_step)
    for weights in np.array(np.meshgrid(*[weight_grid] * n_models)).T.reshape(-1, n_models):
        if np.isclose(np.sum(weights), 1):
            roc_auc_value = weighted_roc_auc(weights, models_preds, y)
            if roc_auc_score > best_roc_auc:
                best_roc_auc = roc_auc_score
                best_weights = weights

    model_final = sum_models(models, weight=best_weights)
    return model_final