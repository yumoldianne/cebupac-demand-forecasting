"""
VERSION 1: LSTM FOR ALL AIRPORTS (UNIVARIATE)
==============================================
Runs all 10 LSTM variants on EVERY airport in the dataset (not just top 10).
Tests window sizes 3, 6, 12 months per airport.
Target: Passenger counts (univariate time series)

Run:
    python lstm_v1_all_airports.py

Requires:
    pip install tensorflow pandas numpy scikit-learn matplotlib seaborn
"""

import os, warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

warnings.filterwarnings('ignore')
os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, Bidirectional
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau

np.random.seed(42)
tf.random.set_seed(42)

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════
DATA_PATH    = '1104-probit.csv'
OUT_DIR      = 'lstm_v1_all_airports'
os.makedirs(OUT_DIR, exist_ok=True)

TARGET       = 'Passenger'
WINDOW_SIZES = [3, 6, 12]
MIN_MONTHS   = 15              # minimum months of data required per airport
TRAIN_SPLIT  = 0.80
VAL_SPLIT    = 0.20
ES_PATIENCE  = 15
LR_PATIENCE  = 5

# ═══════════════════════════════════════════════════════════════════════════
# 10 LSTM VARIANTS
# ═══════════════════════════════════════════════════════════════════════════
VARIANTS = {
    'V01_Simple': {
        'layers': [dict(t='LSTM',  u=50,  rs=False, d=0.20)],
        'lr': 0.001,  'batch': 16, 'epochs': 150,
    },
    'V02_Deep': {
        'layers': [dict(t='LSTM',  u=100, rs=True,  d=0.30),
                   dict(t='LSTM',  u=50,  rs=False, d=0.30)],
        'lr': 0.001,  'batch': 16, 'epochs': 150,
    },
    'V03_Wide': {
        'layers': [dict(t='LSTM',  u=128, rs=False, d=0.30)],
        'lr': 0.0005, 'batch': 32, 'epochs': 150,
    },
    'V04_Triple': {
        'layers': [dict(t='LSTM',  u=80,  rs=True,  d=0.20),
                   dict(t='LSTM',  u=60,  rs=True,  d=0.20),
                   dict(t='LSTM',  u=40,  rs=False, d=0.20)],
        'lr': 0.001,  'batch': 16, 'epochs': 200,
    },
    'V05_Bidir': {
        'layers': [dict(t='Bidir', u=64,  rs=False, d=0.20)],
        'lr': 0.001,  'batch': 16, 'epochs': 150,
    },
    'V06_Pyramid': {
        'layers': [dict(t='LSTM',  u=120, rs=True,  d=0.25),
                   dict(t='LSTM',  u=80,  rs=True,  d=0.25),
                   dict(t='LSTM',  u=40,  rs=False, d=0.25)],
        'lr': 0.001,  'batch': 16, 'epochs': 200,
    },
    'V07_DenseLSTM': {
        'layers': [dict(t='LSTM',  u=100, rs=True,  d=0.30),
                   dict(t='LSTM',  u=100, rs=False, d=0.30)],
        'lr': 0.0008, 'batch': 24, 'epochs': 150,
    },
    'V08_Lite': {
        'layers': [dict(t='LSTM',  u=32,  rs=False, d=0.20)],
        'lr': 0.001,  'batch': 8,  'epochs': 100,
    },
    'V09_BiDeep': {
        'layers': [dict(t='Bidir', u=64,  rs=True,  d=0.30),
                   dict(t='Bidir', u=32,  rs=False, d=0.30)],
        'lr': 0.001,  'batch': 16, 'epochs': 150,
    },
    'V10_Mixed': {
        'layers': [dict(t='Bidir', u=80,  rs=True,  d=0.25),
                   dict(t='LSTM',  u=60,  rs=False, d=0.25)],
        'lr': 0.001,  'batch': 16, 'epochs': 150,
    },
}

# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def load_and_aggregate(path: str) -> pd.DataFrame:
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
    agg = agg.sort_values(['Airport', 'Date']).reset_index(drop=True)
    
    print(f"  Raw rows        : {len(df):,}")
    print(f"  Total airports  : {df['From'].nunique()}")
    print(f"  Date range      : {df['Date'].min().date()} → {df['Date'].max().date()}")
    print(f"  Aggregated rows : {len(agg):,}")
    return agg


def make_windows(series: np.ndarray, w: int, h: int = 1):
    X, y = [], []
    for i in range(len(series) - w - h + 1):
        X.append(series[i : i + w].reshape(-1, 1))
        y.append(series[i + w + h - 1])
    return np.array(X, dtype=np.float32), np.array(y, dtype=np.float32)


def build_model(cfg: dict, w: int) -> Sequential:
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


def fit_model(model, X_tr, y_tr, cfg: dict):
    cb = [
        EarlyStopping(monitor='val_loss', patience=ES_PATIENCE,
                      restore_best_weights=True, verbose=0),
        ReduceLROnPlateau(monitor='val_loss', factor=0.5,
                          patience=LR_PATIENCE, min_lr=1e-6, verbose=0),
    ]
    return model.fit(
        X_tr, y_tr,
        validation_split=VAL_SPLIT,
        epochs=cfg['epochs'],
        batch_size=cfg['batch'],
        callbacks=cb,
        verbose=0,
    )


def calc_metrics(y_true, y_pred, airport, model, window, split):
    yt   = y_true.flatten()
    yp   = y_pred.flatten()
    rmse = float(np.sqrt(mean_squared_error(yt, yp)))
    mae  = float(mean_absolute_error(yt, yp))
    r2   = float(r2_score(yt, yp))
    nz   = yt != 0
    mape = (float(np.abs((yt[nz] - yp[nz]) / yt[nz]).mean() * 100)
            if nz.sum() else float('nan'))
    return dict(Airport=airport, Model=model, Window=window,
                Split=split, RMSE=rmse, MAE=mae, R2=r2, MAPE=mape)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    W = 80
    print('\n' + '═'*W)
    print('  VERSION 1: LSTM FOR ALL AIRPORTS (UNIVARIATE)')
    print('  10 variants × ALL airports × 3 window sizes')
    print('═'*W + '\n')

    # ── Load & aggregate ──────────────────────────────────────────────────────
    print('► Step 1 — Load & aggregate')
    agg = load_and_aggregate(DATA_PATH)

    # ── Filter airports with sufficient data ─────────────────────────────────
    airport_counts = agg.groupby('Airport').size()
    valid_airports = airport_counts[airport_counts >= MIN_MONTHS].index.tolist()
    
    print(f'\n► Step 2 — Filter airports (minimum {MIN_MONTHS} months)')
    print(f'  Total airports       : {len(airport_counts)}')
    print(f'  Airports with ≥{MIN_MONTHS}m : {len(valid_airports)}')
    print(f'  Excluded             : {len(airport_counts) - len(valid_airports)}')
    
    if len(valid_airports) == 0:
        print('\n⚠ No airports meet minimum data requirement. Exiting.')
        return
    
    # Sort by total passenger volume
    apt_totals = agg[agg['Airport'].isin(valid_airports)].groupby('Airport')[TARGET].sum()
    valid_airports = apt_totals.sort_values(ascending=False).index.tolist()
    
    print(f'\n  Processing {len(valid_airports)} airports (sorted by total passengers)...')

    # ── Training loop ─────────────────────────────────────────────────────────
    print(f'\n► Step 3 — Training\n')

    all_records = []
    airports_processed = 0
    airports_skipped = 0

    for a_idx, airport in enumerate(valid_airports, 1):
        if a_idx % 10 == 1 or a_idx == len(valid_airports):
            print(f'{"─"*W}')
            print(f'  Progress: [{a_idx}/{len(valid_airports)}]  {airport}')
        
        series_df = (agg[agg['Airport'] == airport]
                        .sort_values('Date')
                        .reset_index(drop=True))
        raw = series_df[TARGET].values.astype(np.float64)

        if len(raw) < max(WINDOW_SIZES) + 3:
            airports_skipped += 1
            continue

        # Scale
        n_tr_raw = int(len(raw) * TRAIN_SPLIT)
        scaler   = MinMaxScaler()
        scaled   = np.concatenate([
            scaler.fit_transform(raw[:n_tr_raw].reshape(-1, 1)).flatten(),
            scaler.transform(raw[n_tr_raw:].reshape(-1, 1)).flatten(),
        ])

        for v_name, v_cfg in VARIANTS.items():
            for ws in WINDOW_SIZES:
                X, y = make_windows(scaled, ws)
                
                if len(X) < 5:
                    continue
                n_tr = int(len(X) * TRAIN_SPLIT)
                if n_tr < 3 or (len(X) - n_tr) < 1:
                    continue

                X_tr, X_te = X[:n_tr], X[n_tr:]
                y_tr, y_te = y[:n_tr], y[n_tr:]

                try:
                    model = build_model(v_cfg, ws)
                    fit_model(model, X_tr, y_tr, v_cfg)

                    p_tr = model.predict(X_tr, verbose=0).flatten()
                    p_te = model.predict(X_te, verbose=0).flatten()

                    # Inverse transform
                    y_tr_inv = scaler.inverse_transform(y_tr.reshape(-1, 1)).flatten()
                    y_te_inv = scaler.inverse_transform(y_te.reshape(-1, 1)).flatten()
                    p_tr_inv = scaler.inverse_transform(p_tr.reshape(-1, 1)).flatten()
                    p_te_inv = scaler.inverse_transform(p_te.reshape(-1, 1)).flatten()

                    m_tr = calc_metrics(y_tr_inv, p_tr_inv, airport, v_name, ws, 'Train')
                    m_te = calc_metrics(y_te_inv, p_te_inv, airport, v_name, ws, 'Test')

                    all_records.extend([m_tr, m_te])

                except Exception as exc:
                    continue
        
        airports_processed += 1

    print(f'\n{"─"*W}')
    print(f'  Airports processed : {airports_processed}')
    print(f'  Airports skipped   : {airports_skipped}')

    # ── Results ───────────────────────────────────────────────────────────────
    print('\n' + '═'*W)
    print('  RESULTS')
    print('═'*W)

    results_df = pd.DataFrame(all_records)
    if results_df.empty:
        print('No results collected.')
        return

    results_df.to_csv(f'{OUT_DIR}/all_results.csv', index=False)
    test_df = results_df[results_df['Split'] == 'Test'].copy()

    # Best window per (airport, model)
    bw_idx = test_df.groupby(['Airport', 'Model'])['RMSE'].idxmin()
    best_window_df = test_df.loc[bw_idx].reset_index(drop=True)

    # Best overall per airport
    best_per_airport = (
        test_df.loc[test_df.groupby('Airport')['RMSE'].idxmin(),
                    ['Airport', 'Model', 'Window', 'RMSE', 'MAE', 'R2', 'MAPE']]
               .reset_index(drop=True)
    )

    # Model ranking
    avg_by_model = (
        best_window_df.groupby('Model')[['RMSE', 'MAE', 'R2', 'MAPE']]
                      .mean()
                      .sort_values('RMSE')
    )

    # Window ranking
    avg_by_window = (
        test_df.groupby('Window')[['RMSE', 'MAE', 'R2', 'MAPE']]
               .mean()
               .sort_values('RMSE')
    )

    print(f'\n  Total results: {len(results_df)} records')
    print(f'\n  Best model per airport (top 20):')
    print(best_per_airport.head(20).to_string(index=False))
    
    print(f'\n  Average test metrics by model:')
    print(avg_by_model.to_string())
    
    print(f'\n  Average test metrics by window:')
    print(avg_by_window.to_string())

    best_model  = avg_by_model.index[0]
    best_window = int(avg_by_window.index[0])
    print(f'\n  ★ Best overall model  : {best_model}')
    print(f'  ★ Best window size    : {best_window} months')
    print(f'  ★ Avg R² (all airports): {test_df["R2"].mean():.4f}')

    # Save CSVs
    best_per_airport.to_csv(f'{OUT_DIR}/best_per_airport.csv', index=False)
    avg_by_model.to_csv(f'{OUT_DIR}/model_ranking.csv')
    avg_by_window.to_csv(f'{OUT_DIR}/window_ranking.csv')

    # ── Plots ─────────────────────────────────────────────────────────────────
    print('\n► Step 4 — Generating summary plots...')

    # Plot 1: Distribution of R² scores
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    test_df['R2'].hist(bins=30, ax=axes[0], color='steelblue', edgecolor='black')
    axes[0].set_title('Distribution of Test R² (All Airports)', fontweight='bold')
    axes[0].set_xlabel('R²')
    axes[0].set_ylabel('Frequency')
    axes[0].axvline(0, color='red', linestyle='--', label='Zero line')
    axes[0].legend()
    axes[0].grid(alpha=0.3)

    best_window_df['R2'].hist(bins=30, ax=axes[1], color='seagreen', edgecolor='black')
    axes[1].set_title('Distribution of Test R² (Best Window per Airport)', fontweight='bold')
    axes[1].set_xlabel('R²')
    axes[1].set_ylabel('Frequency')
    axes[1].axvline(0, color='red', linestyle='--', label='Zero line')
    axes[1].legend()
    axes[1].grid(alpha=0.3)
    plt.tight_layout()
    fig.savefig(f'{OUT_DIR}/fig1_r2_distribution.png', dpi=200)
    plt.close(fig)
    print('  ✓ fig1_r2_distribution.png')

    # Plot 2: Model ranking
    fig, axes = plt.subplots(1, 2, figsize=(16, 5))
    palette = ['#27ae60' if i == 0 else '#2980b9' for i in range(len(avg_by_model))]
    avg_by_model['RMSE'].sort_values().plot(kind='barh', ax=axes[0], color=palette[::-1])
    axes[0].set_title('Avg Test RMSE by Model', fontweight='bold')
    axes[0].set_xlabel('RMSE (passengers)')
    axes[0].grid(axis='x', alpha=0.4)

    avg_by_model['R2'].sort_values(ascending=False).plot(kind='barh', ax=axes[1], color=palette)
    axes[1].set_title('Avg Test R² by Model', fontweight='bold')
    axes[1].set_xlabel('R²')
    axes[1].grid(axis='x', alpha=0.4)
    plt.tight_layout()
    fig.savefig(f'{OUT_DIR}/fig2_model_ranking.png', dpi=200)
    plt.close(fig)
    print('  ✓ fig2_model_ranking.png')

    # Plot 3: R² by airport (top 30 by passenger volume)
    top_30 = best_per_airport.head(30)
    fig, ax = plt.subplots(figsize=(14, 10))
    colors = ['#27ae60' if r2 > 0.3 else '#f39c12' if r2 > 0 else '#e74c3c' 
              for r2 in top_30['R2']]
    ax.barh(range(len(top_30)), top_30['R2'], color=colors)
    ax.set_yticks(range(len(top_30)))
    ax.set_yticklabels(top_30['Airport'])
    ax.set_xlabel('Test R²')
    ax.set_title('Test R² by Airport (Top 30 by Passenger Volume)', fontweight='bold')
    ax.axvline(0, color='black', linestyle='-', linewidth=0.8)
    ax.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    fig.savefig(f'{OUT_DIR}/fig3_r2_by_airport_top30.png', dpi=200)
    plt.close(fig)
    print('  ✓ fig3_r2_by_airport_top30.png')

    print(f'\n  All outputs → ./{OUT_DIR}/')
    print('  • all_results.csv')
    print('  • best_per_airport.csv')
    print('  • model_ranking.csv')
    print('  • window_ranking.csv')
    print('  • fig1_r2_distribution.png')
    print('  • fig2_model_ranking.png')
    print('  • fig3_r2_by_airport_top30.png')
    
    print('\n' + '═'*W)
    print('  DONE')
    print('═'*W + '\n')


if __name__ == '__main__':
    main()
