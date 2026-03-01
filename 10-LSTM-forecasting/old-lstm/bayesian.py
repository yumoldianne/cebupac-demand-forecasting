"""
Poisson-Gaussian Bayesian Forecasting for Airline Network
Network-wide aggregate forecasting model (no route-level splits)
"""

import pandas as pd
import numpy as np
import pymc as pm
import arviz as az
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# ==================== LOAD DATA ====================
# Replace 'your_data.csv' with your actual file path
df = pd.read_csv('1104-probit.csv')

# ==================== PREPROCESS ====================
# Create datetime
df['Date'] = pd.to_datetime(df['Year'].astype(str) + '-' + df['Month'].astype(str) + '-01')
df = df.sort_values('Date').reset_index(drop=True)

# Time index (months from start)
min_date = df['Date'].min()
df['time_idx'] = ((df['Date'].dt.year - min_date.year) * 12 + 
                   df['Date'].dt.month - min_date.month)

# Aggregate to network-wide monthly totals
network_df = df[df['Entry'] == 1].groupby(['Year', 'Month', 'Date', 'time_idx']).agg({
    'Passenger': 'sum',
    'OwnShFli': 'mean',
    'RouteHHI': 'mean',
    'AirHHI_From': 'mean',
    'AirHHI_To': 'mean',
    'Business_From': 'mean',
    'Business_To': 'mean',
    'EGrowth_From': 'mean',
    'EGrowth_To': 'mean',
    'Travelers_From': 'mean',
    'Travelers_To': 'mean'
}).reset_index()

# Features to use
features = ['OwnShFli', 'RouteHHI', 'AirHHI_From', 'AirHHI_To',
            'Business_From', 'Business_To', 'EGrowth_From', 'EGrowth_To',
            'Travelers_From', 'Travelers_To']

# Normalize features
for col in features:
    mean, std = network_df[col].mean(), network_df[col].std()
    network_df[f'{col}_norm'] = (network_df[col] - mean) / std if std > 0 else 0

print(f"Network-wide training samples: {len(network_df)} months")

# ==================== BUILD MODEL ====================
time_idx = network_df['time_idx'].values
month = network_df['Month'].values
passengers = network_df['Passenger'].values.astype(int)
X = network_df[[f'{col}_norm' for col in features]].values
n_features = len(features)

with pm.Model() as model:
    # Network-level parameters
    intercept = pm.Normal('intercept', mu=10, sigma=3)
    beta_trend = pm.Normal('beta_trend', mu=0, sigma=0.5)
    
    # Seasonality (12 months)
    sigma_season = pm.HalfNormal('sigma_season', sigma=1)
    seasonal = pm.Normal('seasonal', mu=0, sigma=sigma_season, shape=12)
    
    # Feature coefficients
    beta = pm.Normal('beta', mu=0, sigma=0.5, shape=n_features)
    
    # Linear predictor
    mu = intercept + beta_trend * time_idx + seasonal[month - 1]
    
    # Add features
    for i in range(n_features):
        mu += beta[i] * X[:, i]
    
    # Poisson likelihood
    lambda_ = pm.math.exp(mu)
    y = pm.Poisson('y', mu=lambda_, observed=passengers)

print("Network-wide model built")

# ==================== FIT MODEL ====================
with model:
    trace = pm.sample(2000, tune=1000, chains=2, target_accept=0.9, 
                      return_inferencedata=True, random_seed=42)

print("\nSampling complete!")
print(az.summary(trace, var_names=['intercept', 'beta_trend', 'sigma_season']))

# ==================== FORECAST ====================
max_time = network_df['time_idx'].max()
max_date = network_df['Date'].max()

# Create forecast dataframe
forecasts = []
for m in range(1, 13):
    forecast_time = max_time + m
    forecast_date = max_date + pd.DateOffset(months=m)
    
    # Use last observed feature values
    last_features = {col: network_df[col].iloc[-1] for col in features}
    
    forecasts.append({
        'time_idx': forecast_time,
        'Date': forecast_date,
        'Year': forecast_date.year,
        'Month': forecast_date.month,
        **last_features
    })

forecast_df = pd.DataFrame(forecasts)

# Normalize forecast features
for col in features:
    mean, std = network_df[col].mean(), network_df[col].std()
    forecast_df[f'{col}_norm'] = (forecast_df[col] - mean) / std if std > 0 else 0

# Generate predictions
time_idx_f = forecast_df['time_idx'].values
month_f = forecast_df['Month'].values
X_f = forecast_df[[f'{col}_norm' for col in features]].values

# Extract posterior samples
intercept_post = trace.posterior['intercept'].values.flatten()
beta_trend_post = trace.posterior['beta_trend'].values.flatten()
seasonal_post = trace.posterior['seasonal'].values.reshape(-1, 12)
beta_post = trace.posterior['beta'].values.reshape(-1, n_features)

n_samples = len(intercept_post)
n_pred = len(forecast_df)
predictions = np.zeros((n_samples, n_pred))

for s in range(n_samples):
    mu_pred = intercept_post[s] + beta_trend_post[s] * time_idx_f + seasonal_post[s, month_f - 1]
    
    for i in range(n_features):
        mu_pred += beta_post[s, i] * X_f[:, i]
    
    predictions[s, :] = np.random.poisson(np.exp(mu_pred))

# Add predictions to forecast
forecast_df['Passenger_Mean'] = predictions.mean(axis=0)
forecast_df['Passenger_Median'] = np.median(predictions, axis=0)
forecast_df['Passenger_Lower'] = np.percentile(predictions, 2.5, axis=0)
forecast_df['Passenger_Upper'] = np.percentile(predictions, 97.5, axis=0)

print(f"\nGenerated 12-month network-wide forecast")

# ==================== RESULTS ====================
print("\n" + "="*60)
print("NETWORK-WIDE MONTHLY FORECAST")
print("="*60)
print(forecast_df[['Year', 'Month', 'Passenger_Mean', 'Passenger_Lower', 'Passenger_Upper']].to_string(index=False))

print(f"\n12-Month Total: {forecast_df['Passenger_Mean'].sum():,.0f} passengers")
print(f"95% CI: [{forecast_df['Passenger_Lower'].sum():,.0f}, {forecast_df['Passenger_Upper'].sum():,.0f}]")

# ==================== SAVE & VISUALIZE ====================
forecast_df.to_csv('network_forecast.csv', index=False)

# Plot network-wide forecast
fig, ax = plt.subplots(1, 1, figsize=(12, 6))

# Historical
ax.plot(network_df['Date'], network_df['Passenger'], 'o-', 
        label='Historical', linewidth=2, markersize=6)

# Forecast
ax.plot(forecast_df['Date'], forecast_df['Passenger_Mean'], 's-', 
        label='Forecast (Mean)', linewidth=2, markersize=6, color='orangered')
ax.fill_between(forecast_df['Date'], 
                forecast_df['Passenger_Lower'], 
                forecast_df['Passenger_Upper'],
                alpha=0.3, color='orangered', label='95% Prediction Interval')

ax.set_xlabel('Date', fontsize=12)
ax.set_ylabel('Total Network Passengers', fontsize=12)
ax.set_title('Network-Wide Passenger Forecast', fontsize=14, fontweight='bold')
ax.legend(fontsize=10)
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('network_forecast.png', dpi=150)

print("\nFiles saved: network_forecast.csv, network_forecast.png")