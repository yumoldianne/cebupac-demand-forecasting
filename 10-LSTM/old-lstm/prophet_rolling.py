"""
Prophet: Train on 2022-2024 and forecast 2025 (monthly network-level passengers)

Requirements:
  pip install prophet
Input CSV: same format as before with columns: Year, Month, Passenger

Outputs:
  - prophet_2025_forecast.csv
  - prophet_2025_forecast_plot.png
  - If actual 2025 values exist in CSV: prints evaluation metrics and saves them.
"""

import os
import math
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import warnings
warnings.filterwarnings('ignore')

# reproducibility
SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# -----------------------------
# CONFIG
# -----------------------------
CSV_PATH = '1104-probit.csv'
TRAIN_YEARS = [2022, 2023, 2024]   # <-- train on these years
PREDICT_YEAR = 2025
PREDICT_MONTHS = 12                # months in 2025
OUTPUT_DIR = 'prophet_output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -----------------------------
# helpers
# -----------------------------
def safe_mape(a, p):
    a = np.array(a, dtype=float)
    p = np.array(p, dtype=float)
    eps = 1e-10
    return np.mean(np.abs((a - p) / (a + eps))) * 100

# -----------------------------
# load & prepare
# -----------------------------
df_raw = pd.read_csv(CSV_PATH)
# Ensure Year and Month exist and coerce to datetime
df_raw['Date'] = pd.to_datetime(df_raw[['Year', 'Month']].assign(DAY=1))
# Filter relevant years for analysis (keeps 2022-2025 if present)
df = df_raw.copy()
network_data = df.groupby('Date', as_index=False)['Passenger'].sum().sort_values('Date').reset_index(drop=True)
network_data = network_data.rename(columns={'Date': 'ds', 'Passenger': 'y'})

print("Full data range in file:", network_data['ds'].min().strftime('%Y-%m'), "->", network_data['ds'].max().strftime('%Y-%m'))
print("Observations:", len(network_data))

# -----------------------------
# build training set (2022-2024)
# -----------------------------
train_final = network_data[network_data['ds'].dt.year.isin(TRAIN_YEARS)].reset_index(drop=True)
if train_final.empty:
    raise ValueError(f"No training data found for years {TRAIN_YEARS}. Check CSV or adjust TRAIN_YEARS.")
print(f"Training on years {TRAIN_YEARS} -> {len(train_final)} observations from {train_final['ds'].min().strftime('%Y-%m')} to {train_final['ds'].max().strftime('%Y-%m')}")

# -----------------------------
# fit Prophet model
# -----------------------------
model = Prophet(
    yearly_seasonality=True,
    weekly_seasonality=False,
    daily_seasonality=False,
    seasonality_mode='additive',
    interval_width=0.95
)

# You can add holidays or regressors here if you have them:
# model.add_country_holidays(country_name='PH')
# model.add_regressor('some_regressor')

try:
    model.fit(train_final)
except Exception as e:
    raise RuntimeError(f"Prophet model fitting failed: {e}")

# -----------------------------
# create 2025 monthly ds for prediction
# -----------------------------
start_2025 = pd.Timestamp(f'{PREDICT_YEAR}-01-01')
predict_ds = pd.date_range(start=start_2025, periods=PREDICT_MONTHS, freq='MS')
future_df = pd.DataFrame({'ds': predict_ds})

# Forecast (predict only the required future ds)
fcst = model.predict(future_df)

# Collect results
forecast_df = fcst[['ds','yhat','yhat_lower','yhat_upper']].copy().reset_index(drop=True)
forecast_df = forecast_df.rename(columns={'yhat':'Prophet', 'yhat_lower':'Prophet_lower', 'yhat_upper':'Prophet_upper'})

# Save forecast
forecast_csv_path = os.path.join(OUTPUT_DIR, f'prophet_{PREDICT_YEAR}_forecast.csv')
forecast_df.to_csv(forecast_csv_path, index=False)
print(f"Saved forecast to: {forecast_csv_path}")

# -----------------------------
# If actual 2025 is present in the CSV, evaluate
# -----------------------------
actual_2025 = network_data[network_data['ds'].dt.year == PREDICT_YEAR].reset_index(drop=True)
if not actual_2025.empty:
    # align on ds
    merged = forecast_df.merge(actual_2025.rename(columns={'ds':'ds','y':'Actual'}), on='ds', how='left')
    if merged['Actual'].isna().any():
        print("Warning: Some 2025 months missing actuals in CSV; metrics computed on available months only.")
    y_true = merged['Actual'].dropna().values
    y_pred = merged.loc[merged['Actual'].notna(), 'Prophet'].values

    mae_v  = mean_absolute_error(y_true, y_pred)
    rmse_v = math.sqrt(mean_squared_error(y_true, y_pred))
    r2_v   = r2_score(y_true, y_pred) if len(y_true) > 1 else float('nan')
    mape_v = safe_mape(y_true, y_pred)

    metrics = {
        'MAE': mae_v,
        'RMSE': rmse_v,
        'R2': r2_v,
        'MAPE': mape_v
    }
    print(f"\nEvaluation on actual {PREDICT_YEAR} values (where available):\n{metrics}")

    # save merged predictions + actuals
    merged_csv = os.path.join(OUTPUT_DIR, f'prophet_{PREDICT_YEAR}_forecast_with_actuals.csv')
    merged.to_csv(merged_csv, index=False)
    print(f"Saved merged forecast+actuals to: {merged_csv}")
else:
    print(f"No actual {PREDICT_YEAR} data found in the CSV. Skipping evaluation.")

# -----------------------------
# Plot forecast (with recent history tail)
# -----------------------------
plt.figure(figsize=(12,6))
# plot last 36 months of actuals if available
recent = network_data[network_data['ds'] >= (start_2025 - pd.DateOffset(months=36))].copy()
if recent.empty:
    recent = network_data.copy().tail(36)

plt.plot(recent['ds'], recent['y'], label='Actual (recent)', color='black', linewidth=2)
plt.plot(forecast_df['ds'], forecast_df['Prophet'], label=f'Prophet Forecast {PREDICT_YEAR}', marker='o', linewidth=2)
plt.fill_between(forecast_df['ds'], forecast_df['Prophet_lower'], forecast_df['Prophet_upper'], alpha=0.25, label='95% interval')
plt.axvline(x=train_final['ds'].max(), linestyle='--', color='gray', label='Train cutoff')
plt.title(f'Prophet: Train {TRAIN_YEARS[0]}-{TRAIN_YEARS[-1]} → Forecast {PREDICT_YEAR}')
plt.xlabel('Date')
plt.ylabel('Passengers')
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()

plot_path = os.path.join(OUTPUT_DIR, f'prophet_{PREDICT_YEAR}_forecast_plot.png')
plt.savefig(plot_path, dpi=150)
plt.show()
print(f"\nSaved plot to: {plot_path}")

print("\nDone.")
