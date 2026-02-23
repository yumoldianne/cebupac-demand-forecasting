"""
routes_kmeans_elbow_pca.py

- Transforms skewed variables (log1p)
- Shows correlation and VIF
- Runs KMeans using elbow (inertia) and suggests best k via max-distance-to-line knee detector
- Repeats same for PCA space
- Saves CSVs with appended cluster columns
"""

import os
from io import StringIO
import textwrap
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.decomposition import PCA

from statsmodels.stats.outliers_influence import variance_inflation_factor

# ------------------ USER SETTINGS ------------------
input_path = "1104-probit.csv"
output_path = "clusters_elbow.csv"
output_path_pca = "clusters_pca_elbow.csv"

# columns to log-transform (counts / skewed). Adjust to your actual column names.
skew_cols = [
    "Travelers_From", "Travelers_To",
    "Establishment_From", "Establishment_To",
    "AirFli_From", "AirFli_To",
    "OFW_From", "OFW_To"
]

# features to use for clustering (after transform you'll use *_log for skew cols)
# If you want to use the raw versions of some columns, list them here.
feature_candidates = [
    # Structural / route
    "RouteHHI", "Dist",
    # airport-level flights (will use AirFli_From_log / AirFli_To_log after transform)
    "AirFli_From_log", "AirFli_To_log",
    # demand
    "Travelers_From_log", "Travelers_To_log",
    # socioeconomic
    "Establishment_From_log", "Establishment_To_log",
    "OFW_From_log", "OFW_To_log",
    # competition / growth (raw)
    "AirHHI_From", "AirHHI_To", "EGrowth_From", "EGrowth_To"
]

# elbow search range
min_k = 1
max_k = 8

# PCA settings
pca_variance_explained = 0.90

# Optionally force a k (set to None to use elbow suggestion)
force_k = None  # e.g., 3

# ----------------------------------------------------

# ---------- helper functions ----------
def load_sample_df():
    data = textwrap.dedent("""
    Route\tFrom\tTo\tRouteHHI\tAirHHI_From\tAirHHI_To\tAirFli_From\tAirFli_To\tBusiness_From\tBusiness_To\tAccommodation_From\tAccommodation_To\tPort_From\tPort_To\tEGrowth_From\tEGrowth_To\tOFW_From\tOFW_To\tTravelers_From\tTravelers_To\tEstablishment_From\tEstablishment_To\tDist
    CEB-BAG\tCEB\tBAG\t0\t2927.62\t0\t2535\t0\t0.0056\t0.2033\t0.5096\t0.5139\t1.891\t1.4349\t0.0813\t0.1274\t2606\t627\t7517450\t1983511\t386\t38\t300
    CYP-MNL\tCYP\tMNL\t10000\t5117.08\t3675.15\t32\t7196\t0.0009\t0.1691\t0.3387\t0.5725\t1.9297\t1.9891\t0.0019\t0.1717\t1528\t4565\t1695713\t7348669\t103\t1351\t600
    CYP-TAC\tCYP\tTAC\t0\t5117.08\t3705.6\t32\t359\t0.0009\t0.0177\t0.3387\t0.1266\t1.9297\t1.7168\t0.0019\t0.1699\t1528\t1528\t1695713\t1695713\t103\t103\t450
    IAO-MNL\tIAO\tMNL\t10000\t3896.3\t3675.15\t382\t7196\t0.0426\t0.1691\t0\t0.5725\t1.977\t1.9891\t0.0025\t0.1717\t920\t4565\t1728856\t7348669\t47\t1351\t700
    KLO-MPH\tKLO\tMPH\t0\t6206.53\t3921.83\t114\t768\t0.0773\t0.0902\t0.1044\t1.3168\t1.8404\t1.9746\t0.0029\t0.0014\t2618\t2618\t5909568\t5909568\t147\t147\t200
    MNL-CYP\tMNL\tCYP\t10000\t3779.26\t5178.59\t6510\t31\t0.1691\t0.0009\t0.5725\t0.3387\t1.9891\t1.9297\t0.1717\t0.0019\t4565\t1528\t7348669\t1695713\t1351\t103\t600
    MPH-KLO\tMPH\tKLO\t0\t3257.16\t6206.53\t972\t114\t0.0902\t0.0773\t1.3168\t0.1044\t1.9746\t1.8404\t0.0014\t0.0029\t2618\t2618\t5909568\t5909568\t147\t147\t200
    SUG-BXU\tSUG\tBXU\t0\t10000\t4941.57\t43\t282\t0.0282\t0.018\t0.0533\t0.0452\t1.8999\t1.8643\t0.0128\t0.2916\t920\t920\t1728856\t1728856\t47\t47\t150
    SUG-TAG\tSUG\tTAG\t0\t10000\t3453.04\t43\t377\t0.0282\t0.0022\t0.0533\t0.746\t1.8999\t1.9782\t0.0128\t0.0037\t920\t2606\t1728856\t7517450\t47\t386\t120
    TBH-MNL\tTBH\tMNL\t0\t0\t3675.15\t0\t7196\t0\t0.1691\t0.0036\t0.5725\t1.9984\t1.9891\t0.0002\t0.1717\t1095\t4565\t2810208\t7348669\t51\t1351\t800
    """)
    df = pd.read_csv(StringIO(data), sep="\t")
    return df

def log_transform(df, cols):
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c + "_log"] = np.log1p(df[c].astype(float).fillna(0))
        else:
            print(f"[warn] column '{c}' not found; skipping log transform.")
    return df

def show_corr_vif(df, features, figsize=(10,8)):
    numeric = df[features].select_dtypes(include=[np.number]).fillna(0)
    if numeric.shape[1] == 0:
        print("No numeric features to compute corr/VIF.")
        return
    plt.figure(figsize=figsize)
    sns.heatmap(numeric.corr(), annot=True, fmt=".2f", cmap="RdBu_r", center=0)
    plt.title("Feature correlation matrix")
    plt.show()
    # VIF
    X = numeric.values
    cols = numeric.columns
    vif = []
    for i in range(X.shape[1]):
        try:
            vif_val = variance_inflation_factor(X, i)
        except Exception as e:
            vif_val = np.nan
        vif.append(vif_val)
    vif_df = pd.DataFrame({"feature": cols, "VIF": vif}).sort_values("VIF", ascending=False)
    print("\nVIF:\n", vif_df)
    return vif_df

def compute_elbow_and_suggest_k(X_scaled, k_min=1, k_max=8, plot=True):
    ks = list(range(k_min, k_max + 1))
    inertias = []
    for k in ks:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        km.fit(X_scaled)
        inertias.append(km.inertia_)
    if plot:
        plt.figure(figsize=(7,4))
        plt.plot(ks, inertias, marker="o")
        plt.xlabel("k"); plt.ylabel("Inertia (WCSS)")
        plt.title("Elbow plot: inertia vs k")
        plt.xticks(ks); plt.grid(True, linestyle=':', linewidth=0.6)
        plt.show()

    # Suggest k by maximum distance from line between first and last points (common heuristic)
    # convert to arrays
    x = np.array(ks, dtype=float)
    y = np.array(inertias, dtype=float)
    # line between first and last
    p1 = np.array([x[0], y[0]])
    p2 = np.array([x[-1], y[-1]])
    # distance from each point to line
    def point_line_distance(pt, a, b):
        # distance from pt to line through a-b
        return np.abs(np.cross(b - a, pt - a)) / np.linalg.norm(b - a)
    dists = np.array([point_line_distance(np.array([xi, yi]), p1, p2) for xi, yi in zip(x, y)])
    # choose k with maximum distance (but ignore k=1 because trivial)
    if len(dists) > 1:
        # index of maximum distance (prefer k>=2)
        idx = int(np.nanargmax(dists[1:])) + 1
        suggested_k = int(ks[idx])
    else:
        suggested_k = ks[0]
    return ks, inertias, suggested_k

def fit_kmeans_and_append(df, features, k, prefix="cluster", save_path=None):
    X = df[features].astype(float).fillna(0).values
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    km = KMeans(n_clusters=k, random_state=42, n_init=50)
    labels = km.fit_predict(Xs)
    df_out = df.copy()
    df_out[prefix] = labels
    # also distance to centroid in scaled space
    dists = np.linalg.norm(Xs - km.cluster_centers_[labels], axis=1)
    df_out[prefix + "_dist_to_centroid"] = dists
    # silhouette
    sil = silhouette_score(Xs, labels) if k > 1 else np.nan
    if save_path:
        df_out.to_csv(save_path, index=False)
        print(f"Saved {save_path}")
    return df_out, km, scaler, sil

# ------------------ MAIN flow ------------------

# load
if input_path and os.path.exists(input_path):
    df = pd.read_csv(input_path)
else:
    df = load_sample_df()
    print("[info] using inline sample dataframe (set input_path to load your own CSV)")

# 1) log-transform skewed variables (creates *_log columns)
df_trans = log_transform(df, skew_cols)

# 2) decide which features to use (only keep those present in df_trans)
features = [f for f in feature_candidates if f in df_trans.columns]
print(f"[info] features used for clustering check: {features}")

# 3) show correlation and VIF
vif_df = show_corr_vif(df_trans, features)

# 4) prepare matrix for elbow (standardize)
# We'll use the features selected above (numeric), scale them, then run elbow
num_features = df_trans[features].select_dtypes(include=[np.number]).columns.tolist()
X = df_trans[num_features].astype(float).fillna(0).values
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# 5) compute elbow, plot, and get suggested k
ks, inertias, suggested_k = compute_elbow_and_suggest_k(X_scaled, k_min=min_k, k_max=min(max_k, len(df)-1))
print(f"[info] elbow suggested k = {suggested_k}")

# override if user forced k
chosen_k = int(force_k) if force_k else int(suggested_k)
print(f"[info] chosen k = {chosen_k}")

# 6) fit KMeans and append cluster column, then save CSV
df_with_clusters, km_obj, scaler_obj, sil_score = fit_kmeans_and_append(df_trans, num_features, chosen_k,
                                                                         prefix="cluster_elbow", save_path=output_path)
print(f"[info] KMeans (elbow) silhouette: {sil_score:.4f}")

# 7) Repeat **same approach** in PCA space: run PCA to keep `pca_variance_explained`, then elbow on PCA components
# Build PCA on the SAME scaled numeric features
pca = PCA(n_components=pca_variance_explained, svd_solver='full', random_state=42)
X_pca = pca.fit_transform(X_scaled)
print(f"[info] PCA reduced to {X_pca.shape[1]} components (explained {pca.explained_variance_ratio_.cumsum()[-1]:.3f})")

# 1) loadings: matrix (features x components)
loadings = pd.DataFrame(pca.components_.T, index=num_features,
                        columns=[f'PC{i+1}' for i in range(pca.n_components_)])
# 2) squared loadings (aka variable contribution to component variance before normalization)
squared_loadings = loadings**2

# 3) % contribution of each variable to each PC
# For each component (column), sum of squared loadings across variables equals 1 (if PCA on standardized data),
# but we compute percent to be explicit.
contrib_pct = squared_loadings.div(squared_loadings.sum(axis=0), axis=1) * 100
contrib_pct = contrib_pct.round(4)

# 4) Create a tidy long-form table for easy inspection / sorting
contrib_long = contrib_pct.reset_index().melt(id_vars='index', var_name='PC', value_name='contrib_pct')
contrib_long = contrib_long.rename(columns={'index': 'feature'}).sort_values(['PC','contrib_pct'], ascending=[True, False])

# 5) Print summary: explained variance + top contributors per PC
explained = pd.DataFrame({
    'PC': [f'PC{i+1}' for i in range(pca.n_components_)],
    'explained_variance_ratio': np.round(pca.explained_variance_ratio_, 4),
    'cumulative_explained': np.round(np.cumsum(pca.explained_variance_ratio_), 4)
})
print("\nPCA explained variance:")
print(explained.to_string(index=False))

top_n = 5
print(f"\nTop {top_n} contributing variables per PC (by % contribution):")
for pc in explained['PC']:
    top = contrib_long[contrib_long['PC'] == pc].head(top_n)
    print(f"\n{pc} (explained {explained.loc[explained['PC']==pc,'explained_variance_ratio'].values[0]:.4f}):")
    print(top[['feature','contrib_pct']].to_string(index=False))

# 6) Save outputs to CSV for later inspection
loadings.to_csv("pca_loadings_features_x_components.csv")
contrib_pct.to_csv("pca_contribution_pct_by_feature_and_pc.csv")
contrib_long.to_csv("pca_contrib_long.csv", index=False)
explained.to_csv("pca_explained_variance.csv", index=False)
print("\nSaved: pca_loadings_features_x_components.csv, pca_contribution_pct_by_feature_and_pc.csv, pca_contrib_long.csv, pca_explained_variance.csv")

# 7) Heatmap of contribution percentages (features on y, PCs on x)
plt.figure(figsize=(max(6, pca.n_components_*1.2), max(6, len(num_features)*0.25)))
sns.heatmap(contrib_pct, annot=True, fmt=".2f", cmap='viridis', cbar_kws={'label': '% contribution'})
plt.title('Variable % contribution to each Principal Component')
plt.ylabel('Feature')
plt.xlabel('Principal Component')
plt.tight_layout()
plt.show()

# compute elbow on PCA space
ks_pca, inertias_pca, suggested_k_pca = compute_elbow_and_suggest_k(X_pca, k_min=min_k, k_max=min(max_k, len(df)-1))
print(f"[info] elbow suggested k (PCA space) = {suggested_k_pca}")

chosen_k_pca = int(force_k) if force_k else int(suggested_k_pca)
print(f"[info] chosen k for PCA = {chosen_k_pca}")

# fit KMeans on PCA components and append cluster column
# We'll append label column named 'cluster_pca_elbow'
km_pca = KMeans(n_clusters=chosen_k_pca, random_state=42, n_init=50)
labels_pca = km_pca.fit_predict(X_pca)
df_pca_out = df_trans.copy()
df_pca_out["cluster_pca_elbow"] = labels_pca
# distance to centroid in PCA space
dists_pca = np.linalg.norm(X_pca - km_pca.cluster_centers_[labels_pca], axis=1)
df_pca_out["cluster_pca_elbow_dist_to_centroid"] = dists_pca
sil_pca = silhouette_score(X_pca, labels_pca) if chosen_k_pca > 1 else np.nan
df_pca_out.to_csv(output_path_pca, index=False)
print(f"[info] Saved PCA clusters to {output_path_pca}")
print(f"[info] KMeans (PCA-elbow) silhouette: {sil_pca:.4f}")

# 8) final prints: cluster sizes and a quick preview
print("\nCluster sizes (elbow):")
print(df_with_clusters["cluster_elbow"].value_counts().sort_index())

print("\nCluster sizes (PCA elbow):")
print(df_pca_out["cluster_pca_elbow"].value_counts().sort_index())

print("\nPreview (first 6 rows) with appended columns (elbow):")
print(df_with_clusters.head(6).T)

print("\nPreview (first 6 rows) with appended columns (PCA elbow):")
print(df_pca_out.head(6).T)

print(pd.DataFrame({'explained_variance_ratio': pca.explained_variance_ratio_, 
                    'cumulative': np.cumsum(pca.explained_variance_ratio_)}))
