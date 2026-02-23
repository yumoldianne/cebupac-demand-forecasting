import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

# Load your data
df_raw = pd.read_csv('1104-probit.csv')  # Replace with your file path

print("=" * 60)
print("DATA PREPROCESSING")
print("=" * 60)
print(f"Original dataset shape: {df_raw.shape}")
print(f"Number of unique routes: {df_raw['Route'].nunique()}")
print(f"Time period: {df_raw['Year'].min()}-{df_raw['Year'].max()}, Months: {df_raw['Month'].min()}-{df_raw['Month'].max()}")

# Aggregate data by route (average across all months/years)
# Keep categorical identifiers and average all numerical variables
categorical_cols = ['Route', 'From', 'To']
temporal_cols = ['Year', 'Month']

# Group by route and calculate mean for numerical columns
df = df_raw.groupby(categorical_cols, as_index=False).mean(numeric_only=True)

print(f"\nAfter aggregation by route:")
print(f"Aggregated dataset shape: {df.shape}")
print(f"Number of unique routes: {df.shape[0]}")
print(f"✓ Each route now has averaged metrics across all time periods\n")

# Now define numerical columns (excluding temporal ones that were averaged out)
exclude_cols = categorical_cols
numerical_cols = [col for col in df.columns if col not in exclude_cols]

# ==========================================
# SCENARIO 1: Use All Numerical Variables
# ==========================================
print("=" * 60)
print("SCENARIO 1: K-Means with ALL Numerical Variables")
print("=" * 60)

# Prepare data for Scenario 1
X1 = df[numerical_cols].copy()

# Handle any missing values
X1 = X1.fillna(X1.mean())

# Standardize the features
scaler1 = StandardScaler()
X1_scaled = scaler1.fit_transform(X1)

# (a) Elbow Method for Scenario 1
inertias1 = []
silhouette_scores1 = []
K_range = range(2, 11)

for k in K_range:
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(X1_scaled)
    inertias1.append(kmeans.inertia_)
    silhouette_scores1.append(silhouette_score(X1_scaled, kmeans.labels_))

# Plot Elbow Curve for Scenario 1
plt.figure(figsize=(14, 5))

plt.subplot(1, 2, 1)
plt.plot(K_range, inertias1, 'bo-')
plt.xlabel('Number of Clusters (k)')
plt.ylabel('Inertia')
plt.title('Scenario 1: Elbow Method (All Variables)')
plt.grid(True)

plt.subplot(1, 2, 2)
plt.plot(K_range, silhouette_scores1, 'ro-')
plt.xlabel('Number of Clusters (k)')
plt.ylabel('Silhouette Score')
plt.title('Scenario 1: Silhouette Scores (All Variables)')
plt.grid(True)

plt.tight_layout()
plt.savefig('scenario1_elbow_silhouette.png', dpi=300, bbox_inches='tight')
plt.show()

# (b) Apply K-Means with optimal k (let's use k=3 as example, adjust based on elbow)
optimal_k1 = 3  # Adjust this based on your elbow plot
kmeans1 = KMeans(n_clusters=optimal_k1, random_state=42, n_init=10)
df['Cluster_Scenario1'] = kmeans1.fit_predict(X1_scaled)

# (c) Calculate Silhouette Score for Scenario 1
silhouette1 = silhouette_score(X1_scaled, df['Cluster_Scenario1'])

print(f"\nOptimal k chosen: {optimal_k1}")
print(f"Silhouette Score (Scenario 1): {silhouette1:.4f}")
print(f"\nCluster Distribution (Scenario 1):")
print(df['Cluster_Scenario1'].value_counts().sort_index())

# ==========================================
# SCENARIO 2: Use Selected Variables
# ==========================================
print("\n" + "=" * 60)
print("SCENARIO 2: K-Means with SELECTED Variables")
print("=" * 60)

# Define strategically selected variables based on airline business priorities
# BUSINESS REASONING:
# 1. Route Performance Metrics: Passenger demand and market share
# 2. Market Structure: Competition intensity 
# 3. Demand Drivers: Business activity and economic growth (BOTH origin & destination)
# 4. Travel Infrastructure: Airport connectivity and accommodation (BOTH ends)
# 5. Market Potential: Traveler population and business establishments (BOTH ends)
# 
# SYMMETRIC APPROACH: Include both origin and destination for all paired variables
# This captures the FULL origin-destination dynamics

selected_vars = [
    # Core Route Performance (most critical for revenue)
    'Passenger',           # Direct measure of demand and route success
    'OwnShFli',           # Market share - competitive positioning
    'RouteHHI',           # Route competition level (monopoly vs competitive)
    
    # Origin-Destination Market Characteristics (SYMMETRIC)
    'Business_From',      # Business activity at origin (outbound corporate travel)
    'Business_To',        # Business activity at destination (inbound corporate travel)
    'EGrowth_From',       # Economic growth at origin (market expansion potential)
    'EGrowth_To',         # Economic growth at destination (market expansion potential)
    
    # Infrastructure & Capacity (SYMMETRIC)
    'AirFli_From',        # Airport connectivity at origin (hub strength, connections)
    'AirFli_To',          # Airport connectivity at destination (hub strength, connections)
    'Accommodation_From', # Tourism/lodging infrastructure at origin
    'Accommodation_To',   # Tourism/lodging infrastructure at destination
    
    # Market Size Potential (SYMMETRIC)
    'Travelers_From',     # Travel-prone population at origin (outbound demand pool)
    'Travelers_To',       # Travel-prone population at destination (inbound demand pool)
    'Establishment_From', # Business infrastructure at origin
    'Establishment_To'    # Business infrastructure at destination
]

print(f"\nSelected variables ({len(selected_vars)} features):")
print("  → Route Performance: Passenger, OwnShFli, RouteHHI (3 vars)")
print("  → Economic Indicators: Business & EGrowth at BOTH ends (4 vars)")
print("  → Infrastructure: Airport connectivity & Accommodation at BOTH ends (4 vars)")
print("  → Market Potential: Travelers & Establishments at BOTH ends (4 vars)")
print("  → TOTAL: 15 strategically important variables with symmetric origin-destination coverage")

# Prepare data for Scenario 2
X2 = df[selected_vars].copy()

# Handle any missing values
X2 = X2.fillna(X2.mean())

# Standardize the features
scaler2 = StandardScaler()
X2_scaled = scaler2.fit_transform(X2)

# (a) Elbow Method for Scenario 2
inertias2 = []
silhouette_scores2 = []

for k in K_range:
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(X2_scaled)
    inertias2.append(kmeans.inertia_)
    silhouette_scores2.append(silhouette_score(X2_scaled, kmeans.labels_))

# Plot Elbow Curve for Scenario 2
plt.figure(figsize=(14, 5))

plt.subplot(1, 2, 1)
plt.plot(K_range, inertias2, 'bo-')
plt.xlabel('Number of Clusters (k)')
plt.ylabel('Inertia')
plt.title('Scenario 2: Elbow Method (Selected Variables)')
plt.grid(True)

plt.subplot(1, 2, 2)
plt.plot(K_range, silhouette_scores2, 'ro-')
plt.xlabel('Number of Clusters (k)')
plt.ylabel('Silhouette Score')
plt.title('Scenario 2: Silhouette Scores (Selected Variables)')
plt.grid(True)

plt.tight_layout()
plt.savefig('scenario2_elbow_silhouette.png', dpi=300, bbox_inches='tight')
plt.show()

# (b) Apply K-Means with optimal k
optimal_k2 = 3  # Adjust this based on your elbow plot
kmeans2 = KMeans(n_clusters=optimal_k2, random_state=42, n_init=10)
df['Cluster_Scenario2'] = kmeans2.fit_predict(X2_scaled)

# (c) Calculate Silhouette Score for Scenario 2
silhouette2 = silhouette_score(X2_scaled, df['Cluster_Scenario2'])

print(f"\nOptimal k chosen: {optimal_k2}")
print(f"Silhouette Score (Scenario 2): {silhouette2:.4f}")
print(f"\nCluster Distribution (Scenario 2):")
print(df['Cluster_Scenario2'].value_counts().sort_index())

# ==========================================
# COMPARISON
# ==========================================
print("\n" + "=" * 60)
print("COMPARISON OF BOTH SCENARIOS")
print("=" * 60)

comparison_df = pd.DataFrame({
    'Scenario': ['All Variables', 'Selected Variables'],
    'Number of Features': [len(numerical_cols), len(selected_vars)],
    'Optimal k': [optimal_k1, optimal_k2],
    'Silhouette Score': [silhouette1, silhouette2]
})

print("\n", comparison_df.to_string(index=False))

# Visualize comparison
plt.figure(figsize=(10, 6))
scenarios = ['All Variables', 'Selected Variables']
scores = [silhouette1, silhouette2]
colors = ['#3498db', '#e74c3c']

bars = plt.bar(scenarios, scores, color=colors, alpha=0.7, edgecolor='black')
plt.ylabel('Silhouette Score')
plt.title('Comparison of Silhouette Scores')
plt.ylim([0, 1])
plt.grid(axis='y', alpha=0.3)

# Add value labels on bars
for bar, score in zip(bars, scores):
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'{score:.4f}',
             ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig('scenario_comparison.png', dpi=300, bbox_inches='tight')
plt.show()

# Save results to CSV
df.to_csv('flight_data_with_clusters.csv', index=False)
print("\n✓ Results saved to 'flight_data_with_clusters.csv'")

# Display sample of clustered data
print("\n" + "=" * 60)
print("SAMPLE OF CLUSTERED DATA")
print("=" * 60)
print(df[['Route', 'Passenger', 'Cluster_Scenario1', 'Cluster_Scenario2']].head(15))