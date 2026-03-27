"""
LSTM 12-MONTH PREDICTION WITH LOSS CURVES
==========================================
Uses best model per airport from best_per_airport.csv
Generates:
1. 12-month forward predictions with historical context
2. Training/validation loss curves per airport

Requirements:
    pip install tensorflow pandas numpy matplotlib seaborn scikit-learn

Run:
    python lstm_predict_12months.py
"""

import os
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, Bidirectional
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau, History

np.random.seed(42)
tf.random.set_seed(42)

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════
DATA_PATH = '1104-probit.csv'
BEST_MODELS_PATH = 'best_per_airport.csv'
OUT_DIR = 'lstm_12month_predictions'
os.makedirs(OUT_DIR, exist_ok=True)

TARGET = 'Passenger'
TRAIN_SPLIT = 0.80
VAL_SPLIT = 0.20
ES_PATIENCE = 15
LR_PATIENCE = 5

# Model configurations
VARIANTS = {
    'V01_Simple': {
        'layers': [dict(t='LSTM', u=50, rs=False, d=0.20)],
        'lr': 0.001, 'batch': 16, 'epochs': 150,
    },
    'V02_Deep': {
        'layers': [dict(t='LSTM', u=100, rs=True, d=0.30),
                   dict(t='LSTM', u=50, rs=False, d=0.30)],
        'lr': 0.001, 'batch': 16, 'epochs': 150,
    },
    'V03_Wide': {
        'layers': [dict(t='LSTM', u=128, rs=False, d=0.30)],
        'lr': 0.0005, 'batch': 32, 'epochs': 150,
    },
    'V04_Triple': {
        'layers': [dict(t='LSTM', u=80, rs=True, d=0.20),
                   dict(t='LSTM', u=60, rs=True, d=0.20),
                   dict(t='LSTM', u=40, rs=False, d=0.20)],
        'lr': 0.001, 'batch': 16, 'epochs': 200,
    },
    'V05_Bidir': {
        'layers': [dict(t='Bidir', u=64, rs=False, d=0.20)],
        'lr': 0.001, 'batch': 16, 'epochs': 150,
    },
    'V06_Pyramid': {
        'layers': [dict(t='LSTM', u=120, rs=True, d=0.25),
                   dict(t='LSTM', u=80, rs=True, d=0.25),
                   dict(t='LSTM', u=40, rs=False, d=0.25)],
        'lr': 0.001, 'batch': 16, 'epochs': 200,
    },
    'V07_DenseLSTM': {
        'layers': [dict(t='LSTM', u=100, rs=True, d=0.30),
                   dict(t='LSTM', u=100, rs=False, d=0.30)],
        'lr': 0.0008, 'batch': 24, 'epochs': 150,
    },
    'V08_Lite': {
        'layers': [dict(t='LSTM', u=32, rs=False, d=0.20)],
        'lr': 0.001, 'batch': 8, 'epochs': 100,
    },
    'V09_BiDeep': {
        'layers': [dict(t='Bidir', u=64, rs=True, d=0.30),
                   dict(t='Bidir', u=32, rs=False, d=0.30)],
        'lr': 0.001, 'batch': 16, 'epochs': 150,
    },
    'V10_Mixed': {
        'layers': [dict(t='Bidir', u=80, rs=True, d=0.25),
                   dict(t='LSTM', u=60, rs=False, d=0.25)],
        'lr': 0.001, 'batch': 16, 'epochs': 150,
    },
}

# ═══════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def load_and_aggregate(path: str) -> pd.DataFrame:
    """Load CSV and aggregate by origin airport."""
    df = pd.read_csv(path)
    df['Date'] = pd.to_datetime(
        df['Year'].astype(str) + '-' +
        df['Month'].astype(str).str.zfill(2) + '-01'
    )
    df = df.dropna(subset=['Date', TARGET])
    
    agg = (
        df.groupby(['From', 'Date'], sort=True)[TARGET]
          .sum()
          .reset_index()
          .rename(columns={'From': 'Airport'})
    )
    return agg.sort_values(['Airport', 'Date']).reset_index(drop=True)


def make_windows(series: np.ndarray, w: int, h: int = 1):
    """Create rolling windows."""
    X, y = [], []
    for i in range(len(series) - w - h + 1):
        X.append(series[i : i + w].reshape(-1, 1))
        y.append(series[i + w + h - 1])
    return np.array(X, dtype=np.float32), np.array(y, dtype=np.float32)


def build_model(cfg: dict, w: int) -> Sequential:
    """Build LSTM model."""
    model = Sequential()
    for i, lc in enumerate(cfg['layers']):
        kw = dict(return_sequences=lc['rs'])
        if i == 0:
            kw['input_shape'] = (w, 1)
        if lc['t'] == 'LSTM':
            layer = LSTM(lc['u'], **kw)
        else:
            layer = Bidirectional(LSTM(lc['u'], **kw))
        model.add(layer)
        model.add(Dropout(lc['d']))
    model.add(Dense(1))
    model.compile(optimizer=Adam(cfg['lr']), loss='mse', metrics=['mae'])
    return model


def predict_future(model, last_window, scaler, n_steps=12):
    """
    Iteratively predict n_steps into the future.
    Each prediction becomes part of the input for the next prediction.
    """
    predictions = []
    current_window = last_window.copy()
    
    for _ in range(n_steps):
        # Predict next step
        pred_scaled = model.predict(current_window.reshape(1, -1, 1), verbose=0)[0, 0]
        
        # Inverse transform to get actual passenger count
        pred_actual = scaler.inverse_transform([[pred_scaled]])[0, 0]
        predictions.append(pred_actual)
        
        # Update window: drop oldest, add newest prediction
        current_window = np.append(current_window[1:], pred_scaled)
    
    return np.array(predictions)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "="*80)
    print("LSTM 12-MONTH PREDICTION WITH LOSS CURVES")
    print("="*80)
    
    # Load data
    print("\n► Loading data...")
    agg = load_and_aggregate(DATA_PATH)
    best_models = pd.read_csv(BEST_MODELS_PATH)
    
    # Filter out airports with insufficient data (R²=1.0)
    best_models = best_models[best_models['R2'] < 0.99].copy()
    
    print(f"  Loaded {len(agg)} rows")
    print(f"  Best models for {len(best_models)} airports")
    
    # Process each airport
    results = []
    
    for idx, row in best_models.iterrows():
        airport = row['Airport']
        model_name = row['Model']
        window_size = int(row['Window'])
        
        print(f"\n{'─'*80}")
        print(f"[{idx+1}/{len(best_models)}] {airport} — {model_name} — W={window_size}")
        
        # Get airport data
        airport_df = (agg[agg['Airport'] == airport]
                         .sort_values('Date')
                         .reset_index(drop=True))
        
        if len(airport_df) < window_size + 5:
            print(f"  ⚠ Insufficient data ({len(airport_df)} months)")
            continue
        
        raw = airport_df[TARGET].values.astype(np.float64)
        dates = airport_df['Date'].values
        
        # Scale data
        n_tr_raw = int(len(raw) * TRAIN_SPLIT)
        scaler = MinMaxScaler()
        scaled = np.concatenate([
            scaler.fit_transform(raw[:n_tr_raw].reshape(-1, 1)).flatten(),
            scaler.transform(raw[n_tr_raw:].reshape(-1, 1)).flatten(),
        ])
        
        # Create windows
        X, y = make_windows(scaled, window_size)
        
        if len(X) < 5:
            print(f"  ⚠ Too few samples ({len(X)})")
            continue
        
        n_tr = int(len(X) * TRAIN_SPLIT)
        X_train, X_test = X[:n_tr], X[n_tr:]
        y_train, y_test = y[:n_tr], y[n_tr:]
        
        print(f"  Training samples: {len(X_train)}, Test samples: {len(X_test)}")
        
        # Get model config
        cfg = VARIANTS.get(model_name)
        if cfg is None:
            print(f"  ✗ Model {model_name} not found in config")
            continue
        
        # Build and train model
        print(f"  Building model...")
        model = build_model(cfg, window_size)
        
        callbacks = [
            EarlyStopping(monitor='val_loss', patience=ES_PATIENCE,
                          restore_best_weights=True, verbose=0),
            ReduceLROnPlateau(monitor='val_loss', factor=0.5,
                              patience=LR_PATIENCE, min_lr=1e-6, verbose=0),
        ]
        
        print(f"  Training...")
        history = model.fit(
            X_train, y_train,
            validation_split=VAL_SPLIT,
            epochs=cfg['epochs'],
            batch_size=cfg['batch'],
            callbacks=callbacks,
            verbose=0,
        )
        
        # Get last window for future prediction
        last_window = scaled[-window_size:]
        
        # Predict 12 months into future
        print(f"  Predicting 12 months ahead...")
        future_predictions = predict_future(model, last_window, scaler, n_steps=12)
        
        # Get historical predictions for plotting
        y_test_actual = scaler.inverse_transform(y_test.reshape(-1, 1)).flatten()
        y_pred_test = model.predict(X_test, verbose=0).flatten()
        y_pred_test_actual = scaler.inverse_transform(y_pred_test.reshape(-1, 1)).flatten()
        
        # Calculate the correct date indices for test predictions
        # The windowing process creates samples where:
        # - Sample i uses data[i:i+w] to predict data[i+w]
        # - Total samples = len(raw) - window_size
        # - We split samples 80/20, so test starts at sample n_tr
        # - In date space, sample n_tr corresponds to date[n_tr + window_size]
        test_start_idx_in_dates = n_tr + window_size
        test_dates_for_plot = dates[test_start_idx_in_dates : test_start_idx_in_dates + len(y_test_actual)]
        
        # Debug output to verify alignment
        print(f"  Debug: len(dates)={len(dates)}, len(y_test_actual)={len(y_test_actual)}, len(test_dates)={len(test_dates_for_plot)}")
        
        # Store results
        results.append({
            'airport': airport,
            'model': model_name,
            'window': window_size,
            'dates': dates,
            'raw': raw,
            'y_test_actual': y_test_actual,
            'y_pred_test_actual': y_pred_test_actual,
            'test_dates': test_dates_for_plot,
            'future_predictions': future_predictions,
            'history': history.history,
            'n_train': n_tr_raw,
        })
        
        print(f"  ✓ Complete")
    
    # Generate plots
    print("\n" + "="*80)
    print("GENERATING PLOTS")
    print("="*80)
    
    # Plot predictions for each airport
    print("\n► Creating prediction plots...")
    for res in results:
        airport = res['airport']
        
        fig, ax = plt.subplots(figsize=(14, 6))
        
        # Historical data
        ax.plot(res['dates'], res['raw'], 'o-', label='Historical', 
                color='#34495e', linewidth=2, markersize=4, alpha=0.7)
        
        # Test predictions (overlay on historical)
        ax.plot(res['test_dates'], res['y_test_actual'], 'o', label='Actual (Test)', 
                color='#e74c3c', markersize=6, alpha=0.8)
        ax.plot(res['test_dates'], res['y_pred_test_actual'], 's--', label='Predicted (Test)', 
                color='#3498db', markersize=6, alpha=0.8, linewidth=2)
        
        # Future predictions (12 months ahead)
        last_date = pd.to_datetime(res['dates'][-1])
        future_dates = [last_date + pd.DateOffset(months=i+1) for i in range(12)]
        ax.plot(future_dates, res['future_predictions'], 'd-', label='12-Month Forecast', 
                color='#2ecc71', markersize=7, linewidth=2.5, alpha=0.9)
        
        # Formatting
        ax.axvline(x=res['dates'][res['n_train']], color='red', linestyle='--', 
                   linewidth=1.5, alpha=0.5, label='Train/Test Split')
        ax.axvline(x=last_date, color='orange', linestyle='--', 
                   linewidth=1.5, alpha=0.5, label='Historical/Forecast Split')
        
        ax.set_title(f'{airport} — 12-Month Passenger Forecast\n{res["model"]} (Window={res["window"]})', 
                     fontsize=14, fontweight='bold', pad=15)
        ax.set_xlabel('Date', fontsize=11)
        ax.set_ylabel('Passengers', fontsize=11)
        ax.legend(loc='best', fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:,.0f}'))
        
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(f'{OUT_DIR}/{airport}_prediction.png', dpi=200, bbox_inches='tight')
        plt.close()
    
    print(f"  ✓ Saved {len(results)} prediction plots")
    
    # Plot loss curves for each airport
    print("\n► Creating loss curve plots...")
    for res in results:
        airport = res['airport']
        history = res['history']
        
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        
        # Loss plot
        axes[0].plot(history['loss'], label='Training Loss', color='#3498db', linewidth=2)
        axes[0].plot(history['val_loss'], label='Validation Loss', color='#e74c3c', linewidth=2)
        axes[0].set_title(f'{airport} — Loss Curve', fontsize=12, fontweight='bold')
        axes[0].set_xlabel('Epoch', fontsize=10)
        axes[0].set_ylabel('Loss (MSE)', fontsize=10)
        axes[0].legend(fontsize=9)
        axes[0].grid(True, alpha=0.3)
        axes[0].set_yscale('log')
        
        # MAE plot
        axes[1].plot(history['mae'], label='Training MAE', color='#3498db', linewidth=2)
        axes[1].plot(history['val_mae'], label='Validation MAE', color='#e74c3c', linewidth=2)
        axes[1].set_title(f'{airport} — MAE Curve', fontsize=12, fontweight='bold')
        axes[1].set_xlabel('Epoch', fontsize=10)
        axes[1].set_ylabel('MAE', fontsize=10)
        axes[1].legend(fontsize=9)
        axes[1].grid(True, alpha=0.3)
        
        fig.suptitle(f'{res["model"]} (Window={res["window"]})', fontsize=11, y=1.02)
        plt.tight_layout()
        plt.savefig(f'{OUT_DIR}/{airport}_loss.png', dpi=200, bbox_inches='tight')
        plt.close()
    
    print(f"  ✓ Saved {len(results)} loss curve plots")
    
    # Summary plot: All airports on one figure
    print("\n► Creating summary plots...")
    
    # Predictions summary (grid)
    n_airports = len(results)
    ncols = 3
    nrows = int(np.ceil(n_airports / ncols))
    
    fig, axes = plt.subplots(nrows, ncols, figsize=(18, nrows * 4), squeeze=False)
    
    for idx, res in enumerate(results):
        row = idx // ncols
        col = idx % ncols
        ax = axes[row, col]
        
        # Historical
        ax.plot(res['dates'], res['raw'], 'o-', label='Historical', 
                color='gray', linewidth=1.5, markersize=3, alpha=0.6)
        
        # Test
        ax.plot(res['test_dates'], res['y_pred_test_actual'], 's--', label='Test Pred', 
                color='#3498db', markersize=4, alpha=0.8)
        
        # Future
        last_date = pd.to_datetime(res['dates'][-1])
        future_dates = [last_date + pd.DateOffset(months=i+1) for i in range(12)]
        ax.plot(future_dates, res['future_predictions'], 'd-', label='12M Forecast', 
                color='#2ecc71', markersize=5, linewidth=2, alpha=0.9)
        
        ax.set_title(f"{res['airport']} ({res['model']})", fontsize=9, fontweight='bold')
        ax.legend(fontsize=7)
        ax.grid(True, alpha=0.2)
        ax.tick_params(labelsize=7)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Hide unused subplots
    for i in range(n_airports, nrows * ncols):
        axes[i // ncols, i % ncols].set_visible(False)
    
    fig.suptitle('12-Month Passenger Forecasts — All Airports', 
                 fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig(f'{OUT_DIR}/summary_all_predictions.png', dpi=200, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved summary prediction plot")
    
    # Loss summary (grid)
    fig, axes = plt.subplots(nrows, ncols, figsize=(18, nrows * 4), squeeze=False)
    
    for idx, res in enumerate(results):
        row = idx // ncols
        col = idx % ncols
        ax = axes[row, col]
        
        history = res['history']
        ax.plot(history['loss'], label='Train', color='#3498db', linewidth=1.5, alpha=0.7)
        ax.plot(history['val_loss'], label='Val', color='#e74c3c', linewidth=1.5, alpha=0.7)
        
        ax.set_title(f"{res['airport']} ({res['model']})", fontsize=9, fontweight='bold')
        ax.set_xlabel('Epoch', fontsize=8)
        ax.set_ylabel('Loss', fontsize=8)
        ax.legend(fontsize=7)
        ax.grid(True, alpha=0.2)
        ax.set_yscale('log')
        ax.tick_params(labelsize=7)
    
    for i in range(n_airports, nrows * ncols):
        axes[i // ncols, i % ncols].set_visible(False)
    
    fig.suptitle('Training/Validation Loss — All Airports', 
                 fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig(f'{OUT_DIR}/summary_all_losses.png', dpi=200, bbox_inches='tight')
    plt.close()
    
    print(f"  ✓ Saved summary loss plot")
    
    # Save predictions to CSV
    print("\n► Saving predictions to CSV...")
    
    for res in results:
        airport = res['airport']
        last_date = pd.to_datetime(res['dates'][-1])
        future_dates = [last_date + pd.DateOffset(months=i+1) for i in range(12)]
        
        forecast_df = pd.DataFrame({
            'Date': future_dates,
            'Predicted_Passengers': res['future_predictions'],
            'Model': res['model'],
            'Window': res['window']
        })
        
        forecast_df.to_csv(f'{OUT_DIR}/{airport}_forecast.csv', index=False)
    
    print(f"  ✓ Saved {len(results)} forecast CSVs")
    
    # Final summary
    print("\n" + "="*80)
    print("COMPLETE")
    print("="*80)
    print(f"\nGenerated outputs in: ./{OUT_DIR}/")
    print(f"  • {len(results)} individual prediction plots")
    print(f"  • {len(results)} individual loss curve plots")
    print(f"  • {len(results)} forecast CSV files")
    print(f"  • 1 summary prediction plot (all airports)")
    print(f"  • 1 summary loss plot (all airports)")
    print("\n" + "="*80 + "\n")


if __name__ == '__main__':
    main()