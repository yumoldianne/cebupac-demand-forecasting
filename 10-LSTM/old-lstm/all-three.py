"""
Prophet vs WMA vs LSTM — Train on 2022-2024, forecast 2025, compare metrics and plot.

Notes:
 - Requires packages: prophet, torch, sklearn, pandas, matplotlib, numpy
 - Input CSV must have columns: Year, Month, Passenger (same format you used)
 - Save this script alongside your CSV (CSV_PATH variable).
 - The script will:
     * Aggregate network-level monthly passengers
     * Train Prophet on 2022-2024 and forecast Jan–Dec 2025
     * Produce recursive WMA 12-month forecast from 2024 history
     * Train an LSTM (same style as your earlier code) on 2022-2024 and recursively forecast 12 months
     * If actual 2025 values exist in the file, compute MAE / RMSE / R2 / MAPE for each model
     * Save CSVs and a combined plot showing Actual vs Prophet vs WMA vs LSTM
"""

import os
import math
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from prophet import Prophet
import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import MinMaxScaler
import warnings
warnings.filterwarnings('ignore')

# -----------------------------
# CONFIG
# -----------------------------
CSV_PATH = '1104-probit.csv'
TRAIN_YEARS = [2022, 2023, 2024]   # train on these years
PREDICT_YEAR = 2025
PREDICT_MONTHS = 12
OUTPUT_DIR = 'prophet_wma_lstm_output'
SEED = 42

# LSTM hyperparams (tune as needed)
SEQ_LEN = 6
EPOCHS = 400
BATCH_SIZE = 8
LR = 3e-4
PATIENCE = 30
HIDDEN_SIZE = 8
DROPOUT = 0.2
DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

os.makedirs(OUTPUT_DIR, exist_ok=True)
random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)
if torch.cuda.is_available():
    torch.cuda.manual_seed_all(SEED)

# -----------------------------
# helpers
# -----------------------------
def safe_mape(a, p):
    a = np.array(a, dtype=float)
    p = np.array(p, dtype=float)
    eps = 1e-10
    return np.mean(np.abs((a - p) / (a + eps))) * 100

def forecast_wma_from_series(series, steps, weights=(0.5,0.3,0.2)):
    """
    series: 1D array-like of historical values (most recent last)
    steps: number of recursive months to forecast
    """
    history = list(series)
    preds = []
    for _ in range(steps):
        if len(history) >= 3:
            w = weights
            wma = w[0]*history[-1] + w[1]*history[-2] + w[2]*history[-3]
        else:
            wma = float(np.mean(history))
        preds.append(wma)
        history.append(wma)
    return np.array(preds)

# -----------------------------
# LSTM model + training/forecasting
# -----------------------------
class LSTMModel(nn.Module):
    def __init__(self, input_size=1, hidden_size=HIDDEN_SIZE, num_layers=1, dropout=DROPOUT):
        super().__init__()
        self.lstm = nn.LSTM(input_size=input_size, hidden_size=hidden_size,
                            num_layers=num_layers, batch_first=True)
        self.dropout = nn.Dropout(dropout)
        self.fc = nn.Linear(hidden_size, 1)
    
    def forward(self, x):
        out, _ = self.lstm(x)
        out = self.dropout(out[:, -1, :])
        return self.fc(out)

def make_sequences(data, seq_len=SEQ_LEN):
    X, y = [], []
    for i in range(len(data) - seq_len):
        X.append(data[i:i+seq_len])
        y.append(data[i+seq_len])
    return np.array(X), np.array(y)

def train_and_forecast_lstm_series(series, forecast_horizon, seq_len=SEQ_LEN,
                                   epochs=EPOCHS, batch_size=BATCH_SIZE, lr=LR, patience=PATIENCE):
    """
    Train LSTM on provided series (1D numpy) and recursively forecast forecast_horizon steps.
    Returns: preds (1D numpy, length forecast_horizon) and model (trained or None)
    If training is impossible (too few samples), returns last-value repeated.
    """
    series = np.array(series, dtype=float).reshape(-1,1)
    if len(series) < max(8, seq_len+1):
        # not enough data to train reliably
        return np.repeat(series[-1,0], forecast_horizon), None

    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(series)

    X_all, y_all = make_sequences(scaled, seq_len=seq_len)
    if len(X_all) < 4:  # too few sequence samples
        return np.repeat(series[-1,0], forecast_horizon), None

    # time-based train/val split (last ~20% as val)
    val_n = max(1, int(len(X_all) * 0.2))
    train_n = len(X_all) - val_n
    X_tr = torch.tensor(X_all[:train_n], dtype=torch.float32).to(DEVICE)
    y_tr = torch.tensor(y_all[:train_n].reshape(-1,1), dtype=torch.float32).to(DEVICE)
    X_v = torch.tensor(X_all[train_n:], dtype=torch.float32).to(DEVICE)
    y_v = torch.tensor(y_all[train_n:].reshape(-1,1), dtype=torch.float32).to(DEVICE)

    train_ds = TensorDataset(X_tr, y_tr)
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)

    model = LSTMModel(input_size=1, hidden_size=HIDDEN_SIZE, dropout=DROPOUT).to(DEVICE)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(opt, mode='min', factor=0.5, patience=max(1, round(patience/3)))
    loss_fn = nn.MSELoss()

    best_val = float('inf')
    best_state = None
    patience_counter = 0

    for epoch in range(epochs):
        model.train()
        for xb, yb in train_loader:
            opt.zero_grad()
            out = model(xb)
            loss = loss_fn(out, yb)
            loss.backward()
            opt.step()

        model.eval()
        with torch.no_grad():
            v_out = model(X_v)
            val_loss = loss_fn(v_out, y_v).item()

        scheduler.step(val_loss)

        if val_loss < best_val - 1e-8:
            best_val = val_loss
            best_state = {k:v.cpu() for k,v in model.state_dict().items()}
            patience_counter = 0
        else:
            patience_counter += 1
        if patience_counter >= patience:
            break

    if best_state is not None:
        model.load_state_dict(best_state)

    # recursive forecasting in scaled space
    last_seq = scaled[-seq_len:].copy()
    preds = []
    model.eval()
    with torch.no_grad():
        for step in range(forecast_horizon):
            inp = torch.tensor(last_seq, dtype=torch.float32).unsqueeze(0).to(DEVICE)
            scaled_pred = model(inp).cpu().numpy().reshape(-1)[0]
            pred_orig = scaler.inverse_transform([[scaled_pred]])[0,0]
            preds.append(pred_orig)
            # append predicted scaled value into sequence for next step
            new_row = np.array([[scaled_pred]])
            last_seq = np.vstack([last_seq[1:], new_row])
    return np.array(preds), model

# -----------------------------
# Load data and aggregate network-level monthly passengers
# -----------------------------
df_raw = pd.read_csv(CSV_PATH)
if not {'Year','Month','Passenger'}.issubset(df_raw.columns):
    raise ValueError("CSV must include Year, Month, Passenger columns.")
df_raw['Date'] = pd.to_datetime(df_raw[['Year','Month']].assign(DAY=1))
network = df_raw.groupby('Date', as_index=False)['Passenger'].sum().sort_values('Date').reset_index(drop=True)
network = network.rename(columns={'Date':'ds','Passenger':'y'})
print("Data range:", network['ds'].min(), "->", network['ds'].max(), "| Observations:", len(network))

# -----------------------------
# Build training set (2022-2024) and test set (2025)
# -----------------------------
train_df = network[network['ds'].dt.year.isin(TRAIN_YEARS)].reset_index(drop=True)
if train_df.empty:
    raise ValueError(f"No training data found for years {TRAIN_YEARS}. Check CSV.")
print(f"Training observations: {len(train_df)} from {train_df['ds'].min().strftime('%Y-%m')} to {train_df['ds'].max().strftime('%Y-%m')}")

# Create forecast target dates (2025 Jan-Dec)
start_2025 = pd.Timestamp(f'{PREDICT_YEAR}-01-01')
future_ds = pd.date_range(start=start_2025, periods=PREDICT_MONTHS, freq='MS')
future_df = pd.DataFrame({'ds': future_ds})

# -----------------------------
# Prophet: fit on train_df and forecast 2025
# -----------------------------
prophet_model = Prophet(yearly_seasonality=True, weekly_seasonality=False, daily_seasonality=False,
                        seasonality_mode='additive', interval_width=0.95)
prophet_model.fit(train_df)
prophet_fcst = prophet_model.predict(future_df)
prophet_preds = prophet_fcst['yhat'].values
prophet_lower = prophet_fcst['yhat_lower'].values
prophet_upper = prophet_fcst['yhat_upper'].values

prophet_out = pd.DataFrame({
    'ds': future_ds,
    'Prophet': prophet_preds,
    'Prophet_lower': prophet_lower,
    'Prophet_upper': prophet_upper
})
prophet_out.to_csv(os.path.join(OUTPUT_DIR, f'prophet_{PREDICT_YEAR}_forecast.csv'), index=False)
print("Saved Prophet forecast.")

# -----------------------------
# WMA: recursive forecast from end of train series
# -----------------------------
hist_series = train_df['y'].values
wma_preds = forecast_wma_from_series(hist_series, PREDICT_MONTHS)
wma_out = pd.DataFrame({'ds': future_ds, 'WMA': wma_preds})
wma_out.to_csv(os.path.join(OUTPUT_DIR, f'wma_{PREDICT_YEAR}_forecast.csv'), index=False)
print("Saved WMA forecast.")

# -----------------------------
# LSTM: train on train_df and recursively forecast 12 months
# -----------------------------
lstm_preds, lstm_model = train_and_forecast_lstm_series(train_df['y'].values, PREDICT_MONTHS, seq_len=SEQ_LEN,
                                                       epochs=EPOCHS, batch_size=BATCH_SIZE, lr=LR, patience=PATIENCE)
lstm_out = pd.DataFrame({'ds': future_ds, 'LSTM': lstm_preds})
lstm_out.to_csv(os.path.join(OUTPUT_DIR, f'lstm_{PREDICT_YEAR}_forecast.csv'), index=False)
print("Saved LSTM forecast (or fallback).")

# -----------------------------
# Combine forecasts & optionally evaluate against actual 2025 values if present
# -----------------------------
combined = prophet_out.merge(wma_out, on='ds').merge(lstm_out, on='ds')

actual_2025 = network[network['ds'].dt.year == PREDICT_YEAR].reset_index(drop=True)
metrics = {}
if not actual_2025.empty:
    merged = combined.merge(actual_2025.rename(columns={'ds':'ds','y':'Actual'}), on='ds', how='left')
    # drop missing Actual rows if any
    valid = merged.dropna(subset=['Actual']).reset_index(drop=True)
    if valid.empty:
        print("2025 actuals present but all NaN after merge. Skipping metrics.")
    else:
        for col in ['Prophet','WMA','LSTM']:
            y_true = valid['Actual'].values
            y_pred = valid[col].values
            mae_v = mean_absolute_error(y_true, y_pred)
            rmse_v = math.sqrt(mean_squared_error(y_true, y_pred))
            r2_v = r2_score(y_true, y_pred) if len(y_true) > 1 else float('nan')
            mape_v = safe_mape(y_true, y_pred)
            metrics[col] = {'MAE': mae_v, 'RMSE': rmse_v, 'R2': r2_v, 'MAPE': mape_v}
        metrics_df = pd.DataFrame.from_dict(metrics, orient='index')
        metrics_df.to_csv(os.path.join(OUTPUT_DIR, f'model_metrics_{PREDICT_YEAR}.csv'))
        print("\nEvaluation metrics on 2025 actuals:")
        print(metrics_df)
else:
    print("No actual 2025 values found in CSV. Skipping evaluation metrics. Forecasts saved.")

# Save combined forecasts (with upper/lower from Prophet)
combined.to_csv(os.path.join(OUTPUT_DIR, f'combined_forecasts_{PREDICT_YEAR}.csv'), index=False)

# -----------------------------
# Plot: actuals (recent), plus 2025 forecasts
# -----------------------------
plt.figure(figsize=(12,6))

# show recent actuals up to end of 2024 (or available)
end_of_train = train_df['ds'].max()
start_plot = end_of_train - pd.DateOffset(months=36)
recent_actuals = network[network['ds'] >= start_plot].copy()
plt.plot(recent_actuals['ds'], recent_actuals['y'], label='Actual (recent)', color='black', linewidth=2)

# plot forecasts (2025)
plt.plot(combined['ds'], combined['Prophet'], label='Prophet (2025)', marker='o', linewidth=2)
plt.fill_between(combined['ds'], combined['Prophet_lower'], combined['Prophet_upper'], alpha=0.25, label='Prophet 95% CI')
plt.plot(combined['ds'], combined['WMA'], label='WMA (50/30/20)', marker='s', linewidth=2)
plt.plot(combined['ds'], combined['LSTM'], label='LSTM', marker='^', linewidth=2)

plt.axvline(x=end_of_train, linestyle='--', color='gray', label='Train cutoff (end 2024)')

plt.title(f'Forecast comparison for {PREDICT_YEAR}: Prophet vs WMA vs LSTM')
plt.xlabel('Date')
plt.ylabel('Passengers')
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()

plot_path = os.path.join(OUTPUT_DIR, f'forecast_comparison_{PREDICT_YEAR}.png')
plt.savefig(plot_path, dpi=150)
plt.show()
print(f"Saved combined plot to: {plot_path}")

print("\nAll outputs saved to:", OUTPUT_DIR)
if metrics:
    print("\nMetrics (as saved):")
    print(metrics_df)
else:
    print("Run the script with actual 2025 values in the CSV to compute numeric metrics.")
