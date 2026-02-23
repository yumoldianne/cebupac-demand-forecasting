"""
routes_kmeans_save_appended.py

What it does:
- Loads a routes dataset (from file or inline sample)
- Standardizes numeric features
- Plots elbow + prints silhouette scores (k=1..max_k)
- Chooses k automatically by highest silhouette (or you can set manual_k)
- Fits KMeans, computes extra columns and appends them to original dataframe
- Saves appended dataframe to CSV and also saves centroids CSV
"""

from io import StringIO
import textwrap
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.decomposition import PCA
import os

# ---------------- USER SETTINGS ----------------
# If you have an input CSV, set input_path to its path (e.g. "data/routes.csv").
# If input_path is None, the script uses the inline sample dataset.
input_path = "1104-probit.csv"
output_path = "routes_with_clusters_appended.csv"
centroids_path = "routes_cluster_centroids.csv"

# If you prefer to force a k, set manual_k to an integer (>=2). Otherwise set manual_k=None for automatic selection.
manual_k = None  # e.g., 3

# Maximum k to search when auto-selecting (keeps runtime bounded)
max_k_search = 6
# ------------------------------------------------

# ---------- load data ----------
if input_path and os.path.exists(input_path):
    df = pd.read_csv(input_path)

# ---------- choose numeric features ----------
numeric_cols = [
    'RouteHHI','AirHHI_From','AirHHI_To','AirFli_From','AirFli_To',
    'Business_From','Business_To','Accommodation_From','Accommodation_To',
    'Port_From','Port_To','EGrowth_From','EGrowth_To','OFW_From','OFW_To',
    'Travelers_From','Travelers_To','Establishment_From','Establishment_To'
]

# sanity check: ensure numeric columns exist
missing = [c for c in numeric_cols if c not in df.columns]
if missing:
    raise ValueError(f"The following numeric columns are missing from the dataframe: {missing}")

# extract numeric matrix
X = df[numeric_cols].astype(float).values

# ---------- standardize ----------
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# ---------- elbow + silhouette ----------
n_samples = len(df)
max_k = min(max_k_search, n_samples - 1)
inertias = []
sil_scores = {}
ks = list(range(1, max_k + 1))
for k in ks:
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    km.fit(X_scaled)
    inertias.append(km.inertia_)
    if k >= 2:
        sil_scores[k] = silhouette_score(X_scaled, km.labels_)

# plot elbow
plt.figure(figsize=(7,4))
plt.plot(ks, inertias, marker='o')
plt.xlabel('k')
plt.ylabel('Inertia (WCSS)')
plt.title('Elbow plot')
plt.xticks(ks)
plt.grid(True, linestyle=':', linewidth=0.5)
plt.show()

# print inertia and silhouette
print("Inertia (k -> inertia):")
for k,i in zip(ks, inertias):
    print(f"  k={k}: {i:.2f}")
print("\nSilhouette scores (k>=2):")
for k,s in sil_scores.items():
    print(f"  k={k}: {s:.4f}")

# ---------- choose k ----------
if manual_k is not None:
    best_k = int(manual_k)
    print(f"\nManual k chosen: {best_k}")
else:
    # auto select by silhouette
    if sil_scores:
        best_k = max(sil_scores, key=sil_scores.get)
        print(f"\nAuto-selected k by silhouette: {best_k} (silhouette={sil_scores[best_k]:.4f})")
    else:
        # fallback (if dataset too small)
        best_k = 2
        print("\nNot enough values for silhouette; defaulting to k=2")

# ---------- fit final KMeans ----------
kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=50)
labels = kmeans.fit_predict(X_scaled)
df['cluster'] = labels

# compute distance to assigned centroid (in scaled space)
centroids_scaled = kmeans.cluster_centers_
# For each sample, distance to its cluster centroid
dists = np.linalg.norm(X_scaled - centroids_scaled[labels], axis=1)
df['dist_to_centroid'] = dists

# cluster size mapping
cluster_sizes = pd.Series(labels).value_counts().to_dict()
df['cluster_size'] = df['cluster'].map(cluster_sizes)

# Append centroid values (original scale) as centroid_<feature> for each sample's cluster
centroids_orig = scaler.inverse_transform(centroids_scaled)  # back to original feature scale
# create a dataframe of centroids for easier mapping
centroid_df = pd.DataFrame(centroids_orig, columns=numeric_cols)
centroid_df.index.name = 'cluster'

# For each numeric feature, add 'centroid_<feature>' column to df based on assigned cluster
for col in numeric_cols:
    centroid_colname = f"centroid_{col}"
    # map cluster label -> centroid value for that feature
    mapping = centroid_df[col].to_dict()
    df[centroid_colname] = df['cluster'].map(mapping)

# Save appended dataframe to CSV
df.to_csv(output_path, index=False)
print(f"\nSaved appended dataframe to: {output_path}")

# Save centroids table (one row per cluster)
centroid_df.to_csv(centroids_path)
print(f"Saved centroids (original scale) to: {centroids_path}")

# Optional: Save a tidy cluster-membership file
members_path = "routes_by_cluster.csv"
df[['Route','cluster']].sort_values('cluster').to_csv(members_path, index=False)
print(f"Saved route->cluster lookup to: {members_path}")

# ---------- quick diagnostics ----------
print("\nCluster sizes:")
print(df['cluster'].value_counts().sort_index())

# show first rows (preview)
print("\nPreview (first 6 rows):")
print(df.head(6).T)  # transposed for compact preview

# ---------- optional: small PCA plot for 2D visualization ----------
try:
    from sklearn.decomposition import PCA
    X_pca = PCA(n_components=2, random_state=42).fit_transform(X_scaled)
    plt.figure(figsize=(7,5))
    plt.scatter(X_pca[:,0], X_pca[:,1], c=labels, s=60)
    for i, r in enumerate(df['Route']):
        plt.annotate(r, (X_pca[i,0], X_pca[i,1]), textcoords='offset points', xytext=(4,3), fontsize=9)
    plt.title(f'PCA 2D projection (k={best_k})')
    plt.xlabel('PC1'); plt.ylabel('PC2'); plt.grid(True, linestyle=':')
    plt.show()
except Exception as e:
    print("PCA visualization failed:", e)
