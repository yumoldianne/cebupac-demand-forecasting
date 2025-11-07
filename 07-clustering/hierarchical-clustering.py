# Hierarchical clustering: dendrogram + flat clustering
import matplotlib.pyplot as plt
import scipy.cluster.hierarchy as sch
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score
import os
from io import StringIO
import textwrap
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from statsmodels.stats.outliers_influence import variance_inflation_factor

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

# X_scaled: standardized numeric matrix (n_samples x n_features)
# df_trans, num_features already defined in your pipeline.

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

# 1) Dendrogram (linkage)
plt.figure(figsize=(10,4))
Z = sch.linkage(X_scaled, method='ward')   # 'ward' minimizes variance; try 'average' if non-euclidean
sch.dendrogram(Z, labels=df_trans['Route'].values, leaf_rotation=90, leaf_font_size=8, color_threshold=None)
plt.title('Hierarchical clustering dendrogram (Ward linkage)')
plt.xlabel('Route')
plt.ylabel('Distance')
plt.tight_layout()
plt.show()

# 2) Choose k (visual from dendrogram) or try a range and check silhouette
def try_agglomerative(Xs, ks=range(2,7)):
    results = {}
    for k in ks:
        model = AgglomerativeClustering(n_clusters=k, linkage='ward')
        labs = model.fit_predict(Xs)
        sil = silhouette_score(Xs, labs) if k>1 else float('nan')
        results[k] = {'labels': labs, 'silhouette': sil}
        print(f"k={k} silhouette={sil:.4f}")
    return results

results = try_agglomerative(X_scaled, ks=range(2,6))
# pick k after inspection (or use highest silhouette)
best_k = max(results.keys(), key=lambda k: results[k]['silhouette'])
labels = results[best_k]['labels']
print(f"Chosen k (hierarchical by silhouette): {best_k}")

# 3) attach and save
df_hier = df_trans.copy()
df_hier[f'cluster_hier_{best_k}'] = labels
df_hier.to_csv(f"routes_hier_{best_k}.csv", index=False)
print(f"Saved routes_hier_{best_k}.csv")
