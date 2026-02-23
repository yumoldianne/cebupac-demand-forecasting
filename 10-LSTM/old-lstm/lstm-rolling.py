import os
import math
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import warnings
warnings.filterwarnings('ignore')

# reproducibility
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)
if torch.cuda.is_available():
    torch.cuda.manual_seed_all(SEED)

# -----------------------------
# CONFIG
# -----------------------------
CSV_PATH = '1104-probit.csv'
TRAIN_YEARS = [2022, 2023]
USE_EXPANDING_WINDOW = False   # If True: fold train grows each step; else fixed-length train window
TRAIN_WINDOW = 18              # months if fixed-length
TEST_WINDOW = 3
STEP_SIZE = 1                  # smaller => more folds
SEQ_LEN = 6
VAL_SIZE_MONTHS = 3            # last X months of the train window used as validation
EPOCHS = 400
BATCH_SIZE = 8
LR = 3e-4
PATIENCE = 30                  # early stopping patience on validation loss
HIDDEN_SIZE = 8
DROPOUT = 0.2
DEVICE = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

os.makedirs('rolling_preds', exist_ok=True)
os.makedirs('models', exist_ok=True)

# -----------------------------
# DATA LOAD
# -----------------------------
df_raw = pd.read_csv(CSV_PATH)
df_raw['Date'] = pd.to_datetime(df_raw[['Year', 'Month']].assign(DAY=1))
df = df_raw[df_raw['Year'].isin([2022, 2023, 2024])].copy()
network_data = df.groupby('Date', as_index=False)['Passenger'].sum().sort_values('Date').reset_index(drop=True)
print("Data range:", network_data['Date'].min(), "->", network_data['Date'].max())

# -----------------------------
# Rolling / expanding window split builder
# -----------------------------
n = len(network_data)
folds = []
if USE_EXPANDING_WINDOW:
    for train_end in range(TRAIN_WINDOW, n - TEST_WINDOW + 1, STEP_SIZE):
        train_idx = (0, train_end)
        test_idx = (train_end, train_end + TEST_WINDOW)
        folds.append((train_idx, test_idx))
else:
    for start in range(0, n - TRAIN_WINDOW - TEST_WINDOW + 1, STEP_SIZE):
        train_idx = (start, start + TRAIN_WINDOW)
        test_idx = (start + TRAIN_WINDOW, start + TRAIN_WINDOW + TEST_WINDOW)
        folds.append((train_idx, test_idx))

print(f"Built {len(folds)} folds (TRAIN_WINDOW={TRAIN_WINDOW}, TEST_WINDOW={TEST_WINDOW}, STEP_SIZE={STEP_SIZE}, expanding={USE_EXPANDING_WINDOW})")

# -----------------------------
# LSTM model (univariate - passengers only)
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

# -----------------------------
# helpers
# -----------------------------
def safe_mape(a, p):
    a = np.array(a, dtype=float)
    p = np.array(p, dtype=float)
    eps = 1e-10
    return np.mean(np.abs((a - p) / (a + eps))) * 100

def make_sequences(data, seq_len=SEQ_LEN):
    """Create sequences from univariate time series"""
    X, y = [], []
    for i in range(len(data) - seq_len):
        X.append(data[i:i+seq_len])
        y.append(data[i+seq_len])
    return np.array(X), np.array(y)

# -----------------------------
# LSTM training + recursive forecast (with early stopping)
# -----------------------------
def train_and_forecast_lstm(train_df, test_steps, seq_len=SEQ_LEN,
                            val_size_months=VAL_SIZE_MONTHS,
                            hidden_size=HIDDEN_SIZE, dropout=DROPOUT,
                            epochs=EPOCHS, batch_size=BATCH_SIZE, lr=LR, patience=PATIENCE):
    # scale passenger data
    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(train_df[['Passenger']].values.astype(float))
    
    X_all, y_all = make_sequences(scaled_data, seq_len=seq_len)
    if len(X_all) < max(6, seq_len):
        return None, None
    
    # validation split (time-based)
    val_n = min(max(1, val_size_months), max(1, int(len(X_all)*0.2)))
    train_n = len(X_all) - val_n
    X_train, y_train = X_all[:train_n], y_all[:train_n]
    X_val, y_val = X_all[train_n:], y_all[train_n:]
    
    X_tr = torch.tensor(X_train, dtype=torch.float32).to(DEVICE)
    y_tr = torch.tensor(y_train.reshape(-1,1), dtype=torch.float32).to(DEVICE)
    X_v = torch.tensor(X_val, dtype=torch.float32).to(DEVICE)
    y_v = torch.tensor(y_val.reshape(-1,1), dtype=torch.float32).to(DEVICE)
    
    train_ds = TensorDataset(X_tr, y_tr)
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    
    model = LSTMModel(input_size=1, hidden_size=hidden_size, dropout=dropout).to(DEVICE)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(opt, mode='min', factor=0.5, patience=max(1, round(patience/3)))
    loss_fn = nn.MSELoss()
    
    best_val = float('inf')
    best_state = None
    patience_counter = 0
    
    for epoch in range(epochs):
        model.train()
        train_losses = []
        for xb, yb in train_loader:
            opt.zero_grad()
            out = model(xb)
            loss = loss_fn(out, yb)
            loss.backward()
            opt.step()
            train_losses.append(loss.item())
        
        # validation
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
    
    # Recursive forecasting (passengers only)
    last_seq = scaled_data[-seq_len:].copy()
    
    preds = []
    model.eval()
    with torch.no_grad():
        for step in range(test_steps):
            inp = torch.tensor(last_seq, dtype=torch.float32).unsqueeze(0).to(DEVICE)
            scaled_pred = model(inp).cpu().numpy().reshape(-1)[0]
            pred_orig = scaler.inverse_transform([[scaled_pred]])[0,0]
            preds.append(pred_orig)
            
            # Update sequence with prediction
            new_row = np.array([[scaled_pred]])
            last_seq = np.vstack([last_seq[1:], new_row])
    
    return np.array(preds), model

# -----------------------------
# WMA recursive forecast
# -----------------------------
def forecast_wma(train_df, steps):
    history = train_df['Passenger'].values.tolist()
    preds = []
    for _ in range(steps):
        if len(history) >= 3:
            wma = 0.5 * history[-1] + 0.3 * history[-2] + 0.2 * history[-3]
        else:
            wma = float(np.mean(history))
        preds.append(wma)
        history.append(wma)
    return np.array(preds)

# -----------------------------
# Run rolling evaluation
# -----------------------------
all_metrics = []

for fold_num, (train_idx, test_idx) in enumerate(folds, start=1):
    ti, te = train_idx
    tsi, tei = test_idx
    train_df = network_data.iloc[ti:te].reset_index(drop=True)
    test_df = network_data.iloc[tsi:tei].reset_index(drop=True)
    
    y_true = test_df['Passenger'].values
    lstm_preds, lstm_model = train_and_forecast_lstm(train_df, len(test_df))
    if lstm_preds is None:
        lstm_preds = np.repeat(train_df['Passenger'].values[-1], len(test_df))
    
    wma_preds = forecast_wma(train_df, len(test_df))
    
    fold_pred_df = pd.DataFrame({
        'Date': test_df['Date'],
        'Actual': y_true,
        'WMA_Pred': wma_preds,
        'LSTM_Pred': lstm_preds
    })
    fold_pred_df.to_csv(f'rolling_preds/fold_{fold_num}_preds.csv', index=False)
    
    for name, preds in [('WMA', wma_preds), ('LSTM', lstm_preds)]:
        mae_v = mean_absolute_error(y_true, preds)
        rmse_v = np.sqrt(mean_squared_error(y_true, preds))
        r2_v = r2_score(y_true, preds)
        mape_v = safe_mape(y_true, preds)
        all_metrics.append({'Fold': fold_num, 'Method': name, 'MAE': mae_v, 'RMSE': rmse_v, 'R2': r2_v, 'MAPE': mape_v})
    
    print(f"Fold {fold_num} done. Train {train_df['Date'].iloc[0].strftime('%Y-%m')} to {train_df['Date'].iloc[-1].strftime('%Y-%m')} | Test {test_df['Date'].iloc[0].strftime('%Y-%m')} to {test_df['Date'].iloc[-1].strftime('%Y-%m')}")

metrics_df = pd.DataFrame(all_metrics)
metrics_df.to_csv('rolling_wma_lstm_metrics.csv', index=False)
summary = metrics_df.groupby('Method').agg(['mean','std','count'])
print("\nAggregate summary:")
print(summary)

# -----------------------------
# Final 12-month forecast (train on 2022-2023)
# -----------------------------
train_full = network_data[network_data['Date'].dt.year.isin(TRAIN_YEARS)].reset_index(drop=True)
wma_12 = forecast_wma(train_full, 12)
lstm_12, lstm_final_model = train_and_forecast_lstm(train_full, 12)
if lstm_12 is None:
    lstm_12 = np.repeat(train_full['Passenger'].values[-1], 12)

forecast_dates = pd.date_range(start=train_full['Date'].max() + pd.offsets.MonthBegin(1), periods=12, freq='MS')
forecast_df = pd.DataFrame({'Date': forecast_dates, 'WMA': wma_12, 'LSTM': lstm_12})
forecast_df.to_csv('12m_forecasts.csv', index=False)
print("\nSaved 12m_forecasts.csv")

# -----------------------------
# Plot: Clean line chart
# -----------------------------
plt.figure(figsize=(12,6))
recent_actuals = network_data.tail(24)
plt.plot(recent_actuals['Date'], recent_actuals['Passenger'], label='Actual (recent 24 mo)', color='black', linewidth=2)
plt.plot(forecast_df['Date'], forecast_df['WMA'], label='WMA (50/30/20)', marker='o', linewidth=2)
plt.plot(forecast_df['Date'], forecast_df['LSTM'], label='LSTM', marker='s', linewidth=2)
plt.axvline(x=train_full['Date'].max(), linestyle='--', color='gray')
plt.title('12-Month Forecast: WMA vs LSTM')
plt.xlabel('Date')
plt.ylabel('Passengers')
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()
plt.savefig('12m_forecast_plot.png', dpi=150)
plt.show()

print("\nDone! Check rolling_preds/ for per-fold predictions and 12m_forecasts.csv for final forecast.")