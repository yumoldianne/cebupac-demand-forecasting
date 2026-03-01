"""
AIRPORT PASSENGER PREDICTION — LSTM (univariate)
=================================================
Target   : Passenger (total passengers per airport per month)
Input    : Passenger history only — no static snapshot features
Windows  : 3, 6, 12 months — all tried and compared per airport
Airports : Top 10 by total passenger volume

Why univariate?
  All non-passenger columns (Business_From, EGrowth_From, etc.) are static
  cross-sectional snapshots — they do not vary across months for a given
  airport, so they carry zero temporal signal for an LSTM and only add noise.
  AirFli_From and OwnShFli could vary, but are route-level and unreliable
  after aggregation.  Passenger counts alone form the honest time series.

Run:
    python lstm_full.py

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

# ─────────────────────────────────────────────────────────────────────────────
# PATHS & CONFIG
# ─────────────────────────────────────────────────────────────────────────────
DATA_PATH    = '1104-probit.csv'
OUT_DIR      = 'lstm_outputs'
os.makedirs(OUT_DIR, exist_ok=True)

TARGET       = 'Passenger'
WINDOW_SIZES = [3, 6, 12]      # all tested; best chosen per airport
TOP_N        = 10
TRAIN_SPLIT  = 0.80
VAL_SPLIT    = 0.20            # fraction of training portion used for validation
ES_PATIENCE  = 15
LR_PATIENCE  = 5

# ─────────────────────────────────────────────────────────────────────────────
# 10 LSTM VARIANTS  (univariate input — 1 feature)
# ─────────────────────────────────────────────────────────────────────────────
VARIANTS = {
    'V01_Simple': {
        'desc'  : '1-layer LSTM 50 u',
        'layers': [dict(t='LSTM',  u=50,  rs=False, d=0.20)],
        'lr': 0.001,  'batch': 16, 'epochs': 150,
    },
    'V02_Deep': {
        'desc'  : '2-layer LSTM 100→50 u',
        'layers': [dict(t='LSTM',  u=100, rs=True,  d=0.30),
                   dict(t='LSTM',  u=50,  rs=False, d=0.30)],
        'lr': 0.001,  'batch': 16, 'epochs': 150,
    },
    'V03_Wide': {
        'desc'  : '1-layer LSTM 128 u',
        'layers': [dict(t='LSTM',  u=128, rs=False, d=0.30)],
        'lr': 0.0005, 'batch': 32, 'epochs': 150,
    },
    'V04_Triple': {
        'desc'  : '3-layer LSTM 80→60→40 u',
        'layers': [dict(t='LSTM',  u=80,  rs=True,  d=0.20),
                   dict(t='LSTM',  u=60,  rs=True,  d=0.20),
                   dict(t='LSTM',  u=40,  rs=False, d=0.20)],
        'lr': 0.001,  'batch': 16, 'epochs': 200,
    },
    'V05_Bidir': {
        'desc'  : 'Bidir LSTM 64 u',
        'layers': [dict(t='Bidir', u=64,  rs=False, d=0.20)],
        'lr': 0.001,  'batch': 16, 'epochs': 150,
    },
    'V06_Pyramid': {
        'desc'  : '3-layer LSTM 120→80→40 u',
        'layers': [dict(t='LSTM',  u=120, rs=True,  d=0.25),
                   dict(t='LSTM',  u=80,  rs=True,  d=0.25),
                   dict(t='LSTM',  u=40,  rs=False, d=0.25)],
        'lr': 0.001,  'batch': 16, 'epochs': 200,
    },
    'V07_DenseLSTM': {
        'desc'  : '2-layer LSTM 100→100 u',
        'layers': [dict(t='LSTM',  u=100, rs=True,  d=0.30),
                   dict(t='LSTM',  u=100, rs=False, d=0.30)],
        'lr': 0.0008, 'batch': 24, 'epochs': 150,
    },
    'V08_Lite': {
        'desc'  : '1-layer LSTM 32 u  (fast)',
        'layers': [dict(t='LSTM',  u=32,  rs=False, d=0.20)],
        'lr': 0.001,  'batch': 8,  'epochs': 100,
    },
    'V09_BiDeep': {
        'desc'  : '2-layer Bidir 64→32 u',
        'layers': [dict(t='Bidir', u=64,  rs=True,  d=0.30),
                   dict(t='Bidir', u=32,  rs=False, d=0.30)],
        'lr': 0.001,  'batch': 16, 'epochs': 150,
    },
    'V10_Mixed': {
        'desc'  : 'Bidir 80 u → LSTM 60 u',
        'layers': [dict(t='Bidir', u=80,  rs=True,  d=0.25),
                   dict(t='LSTM',  u=60,  rs=False, d=0.25)],
        'lr': 0.001,  'batch': 16, 'epochs': 150,
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def load_and_aggregate(path: str) -> pd.DataFrame:
    """
    Load CSV and collapse to one row per (Airport, Month).
    Passenger is SUMMED across all routes leaving each airport.
    Returns a DataFrame with columns [Airport, Date, Passenger].
    """
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

    print(f"  Raw rows          : {len(df):,}")
    print(f"  Airports          : {df['From'].nunique()}")
    print(f"  Date range        : {df['Date'].min().date()} → {df['Date'].max().date()}")
    print(f"  Aggregated rows   : {len(agg):,}  (airport-months)")
    return agg


def make_windows(series: np.ndarray, w: int, h: int = 1):
    """
    Sliding-window over a 1-D passenger series.
    X shape: (n, w, 1)   y shape: (n,)
    """
    X, y = [], []
    for i in range(len(series) - w - h + 1):
        X.append(series[i : i + w].reshape(-1, 1))
        y.append(series[i + w + h - 1])
    return np.array(X, dtype=np.float32), np.array(y, dtype=np.float32)


def build_model(cfg: dict, w: int) -> Sequential:
    """Build and compile a Keras LSTM model for input shape (w, 1)."""
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


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    W = 72
    print('\n' + '═'*W)
    print('  AIRPORT PASSENGER PREDICTION  —  LSTM  (univariate)')
    print('  10 variants  ×  10 airports  ×  3 window sizes (3 / 6 / 12)')
    print('  Input: passenger counts only')
    print('═'*W + '\n')

    # ── 1. Load & aggregate ───────────────────────────────────────────────────
    print('► Step 1 — Load & aggregate')
    agg = load_and_aggregate(DATA_PATH)

    # ── 2. Top airports ───────────────────────────────────────────────────────
    print(f'\n► Step 2 — Top {TOP_N} airports by total passengers')
    ranking  = agg.groupby('Airport')[TARGET].sum().nlargest(TOP_N)
    top_list = ranking.index.tolist()
    print(f'\n  {"Rank":<5} {"Airport":<10} {"Time steps":>12} {"Total Pax":>16}')
    print(f'  {"":-<4}  {"":-<8}  {"":-<11}  {"":-<15}')
    for rank, apt in enumerate(top_list, 1):
        ts  = len(agg[agg['Airport'] == apt])
        tot = ranking[apt]
        print(f'  {rank:<5} {apt:<10} {ts:>12} {tot:>16,.0f}')

    # ── 3. Training loop ──────────────────────────────────────────────────────
    print(f'\n► Step 3 — Training  '
          f'({len(VARIANTS)} models × {len(top_list)} airports × {len(WINDOW_SIZES)} windows)\n')

    all_records = []
    all_preds   = {}   # {airport: {variant: {window: {y_te, p_te, scaler}}}}

    for a_idx, airport in enumerate(top_list, 1):
        print(f'{"─"*W}')
        print(f'  [{a_idx}/{len(top_list)}]  {airport}')
        print(f'{"─"*W}')

        series_df = (agg[agg['Airport'] == airport]
                        .sort_values('Date')
                        .reset_index(drop=True))
        raw = series_df[TARGET].values.astype(np.float64)

        if len(raw) < max(WINDOW_SIZES) + 3:
            print(f'  ⚠  Only {len(raw)} months of data — skipping.\n')
            continue

        # Scale — fit on training portion only to avoid data leakage
        n_tr_raw = int(len(raw) * TRAIN_SPLIT)
        scaler   = MinMaxScaler()
        scaled   = np.concatenate([
            scaler.fit_transform(raw[:n_tr_raw].reshape(-1, 1)).flatten(),
            scaler.transform(raw[n_tr_raw:].reshape(-1, 1)).flatten(),
        ])

        all_preds[airport] = {}

        for v_name, v_cfg in VARIANTS.items():
            all_preds[airport][v_name] = {}
            window_results = []

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

                    # Inverse-transform back to raw passenger counts for metrics
                    y_tr_inv = scaler.inverse_transform(
                        y_tr.reshape(-1, 1)).flatten()
                    y_te_inv = scaler.inverse_transform(
                        y_te.reshape(-1, 1)).flatten()
                    p_tr_inv = scaler.inverse_transform(
                        p_tr.reshape(-1, 1)).flatten()
                    p_te_inv = scaler.inverse_transform(
                        p_te.reshape(-1, 1)).flatten()

                    m_tr = calc_metrics(y_tr_inv, p_tr_inv,
                                        airport, v_name, ws, 'Train')
                    m_te = calc_metrics(y_te_inv, p_te_inv,
                                        airport, v_name, ws, 'Test')

                    all_records.extend([m_tr, m_te])
                    all_preds[airport][v_name][ws] = dict(
                        y_te=y_te_inv, p_te=p_te_inv,
                        y_tr=y_tr_inv, p_tr=p_tr_inv,
                    )
                    window_results.append((ws, m_te['RMSE']))

                except Exception as exc:
                    print(f'    ✗ {v_name}  W={ws}  → {exc}')
                    continue

            # Print result for each window; mark best with ★
            if window_results:
                best_ws = min(window_results, key=lambda x: x[1])[0]
                for ws, _ in window_results:
                    rec = next(
                        (r for r in all_records
                         if r['Airport'] == airport and r['Model'] == v_name
                         and r['Window'] == ws and r['Split'] == 'Test'), None)
                    if rec:
                        star = '★' if ws == best_ws else ' '
                        print(f'  {star} {v_name:<18} W={ws:>2}  '
                              f'RMSE={rec["RMSE"]:>12,.1f}  '
                              f'MAE={rec["MAE"]:>12,.1f}  '
                              f'R²={rec["R2"]:>7.4f}  '
                              f'MAPE={rec["MAPE"]:>7.2f}%')
        print()

    # ── 4. Results summary ────────────────────────────────────────────────────
    print('═'*W)
    print('  RESULTS SUMMARY')
    print('═'*W)

    results_df = pd.DataFrame(all_records)
    if results_df.empty:
        print('No results — check DATA_PATH and that airports have enough months.')
        return

    results_df.to_csv(f'{OUT_DIR}/all_results.csv', index=False)
    test_df = results_df[results_df['Split'] == 'Test'].copy()

    # Best window per (airport, model)
    bw_idx         = test_df.groupby(['Airport', 'Model'])['RMSE'].idxmin()
    best_window_df = test_df.loc[bw_idx].reset_index(drop=True)

    # Best overall (model + window) per airport
    best_per_airport = (
        test_df.loc[test_df.groupby('Airport')['RMSE'].idxmin(),
                    ['Airport', 'Model', 'Window', 'RMSE', 'MAE', 'R2', 'MAPE']]
               .reset_index(drop=True)
    )

    # Model ranking — average over best-window-per-airport results
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

    print('\n  Best model & window per airport:')
    print(best_per_airport.to_string(index=False))
    print('\n  Average test metrics by LSTM variant (best window per airport):')
    print(avg_by_model.to_string())
    print('\n  Average test metrics by window size:')
    print(avg_by_window.to_string())

    best_model_name   = avg_by_model.index[0]
    best_window_name  = int(avg_by_window.index[0])
    print(f'\n  ★  Best overall model  : {best_model_name}')
    print(f'  ★  Best window size    : {best_window_name} months')

    best_per_airport.to_csv(f'{OUT_DIR}/best_per_airport.csv', index=False)
    avg_by_model.to_csv(f'{OUT_DIR}/model_ranking.csv')
    avg_by_window.to_csv(f'{OUT_DIR}/window_ranking.csv')

    # ── 5. Plots ──────────────────────────────────────────────────────────────
    print('\n► Step 4 — Generating plots …')

    # Fig 1 — RMSE heatmap
    pivot = best_window_df.pivot_table(index='Airport', columns='Model', values='RMSE')
    fig, ax = plt.subplots(figsize=(22, max(4, len(top_list))))
    sns.heatmap(pivot, annot=True, fmt='.0f', cmap='RdYlGn_r',
                linewidths=0.4, ax=ax, cbar_kws={'label': 'RMSE (passengers)'})
    ax.set_title('Test RMSE — Airport × LSTM Variant  (best window per cell)',
                 fontsize=13, fontweight='bold', pad=10)
    plt.tight_layout()
    fig.savefig(f'{OUT_DIR}/fig1_heatmap_rmse.png', dpi=200)
    plt.close(fig)
    print('  ✓ fig1_heatmap_rmse.png')

    # Fig 2 — R² heatmap
    pivot = best_window_df.pivot_table(index='Airport', columns='Model', values='R2')
    fig, ax = plt.subplots(figsize=(22, max(4, len(top_list))))
    sns.heatmap(pivot, annot=True, fmt='.3f', cmap='RdYlGn',
                linewidths=0.4, ax=ax, vmin=-1, vmax=1,
                cbar_kws={'label': 'R²'})
    ax.set_title('Test R² — Airport × LSTM Variant  (best window per cell)',
                 fontsize=13, fontweight='bold', pad=10)
    plt.tight_layout()
    fig.savefig(f'{OUT_DIR}/fig2_heatmap_r2.png', dpi=200)
    plt.close(fig)
    print('  ✓ fig2_heatmap_r2.png')

    # Fig 3 — MAPE heatmap
    pivot = best_window_df.pivot_table(index='Airport', columns='Model', values='MAPE')
    fig, ax = plt.subplots(figsize=(22, max(4, len(top_list))))
    sns.heatmap(pivot, annot=True, fmt='.1f', cmap='RdYlGn_r',
                linewidths=0.4, ax=ax, cbar_kws={'label': 'MAPE (%)'})
    ax.set_title('Test MAPE (%) — Airport × LSTM Variant  (best window per cell)',
                 fontsize=13, fontweight='bold', pad=10)
    plt.tight_layout()
    fig.savefig(f'{OUT_DIR}/fig3_heatmap_mape.png', dpi=200)
    plt.close(fig)
    print('  ✓ fig3_heatmap_mape.png')

    # Fig 4 — Model ranking bar charts
    fig, axes = plt.subplots(1, 2, figsize=(16, 5))
    palette = ['#27ae60' if i == 0 else '#2980b9' for i in range(len(avg_by_model))]
    avg_by_model['RMSE'].sort_values().plot(
        kind='barh', ax=axes[0], color=palette[::-1])
    axes[0].set_title('Avg Test RMSE by Model  (lower = better)', fontweight='bold')
    axes[0].set_xlabel('RMSE (passengers)')
    axes[0].grid(axis='x', alpha=0.4)
    avg_by_model['R2'].sort_values(ascending=False).plot(
        kind='barh', ax=axes[1], color=palette)
    axes[1].set_title('Avg Test R² by Model  (higher = better)', fontweight='bold')
    axes[1].set_xlabel('R²')
    axes[1].grid(axis='x', alpha=0.4)
    plt.tight_layout()
    fig.savefig(f'{OUT_DIR}/fig4_model_ranking.png', dpi=200)
    plt.close(fig)
    print('  ✓ fig4_model_ranking.png')

    # Fig 5 — Window comparison box plots
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    for metric, ax in zip(['RMSE', 'R2', 'MAPE'], axes):
        test_df.boxplot(column=metric, by='Window', ax=ax)
        ax.set_xlabel('Window (months)')
        ax.set_ylabel(metric)
        plt.sca(ax)
        plt.title(f'{metric} by Window Size', fontweight='bold')
    plt.suptitle('')
    plt.tight_layout()
    fig.savefig(f'{OUT_DIR}/fig5_window_comparison.png', dpi=200)
    plt.close(fig)
    print('  ✓ fig5_window_comparison.png')

    # Fig 6 — Actual vs Predicted (raw passenger counts, best model+window)
    valid = [a for a in top_list if a in all_preds and all_preds[a]]
    ncols = 2
    nrows = int(np.ceil(len(valid) / ncols))
    fig, axes = plt.subplots(nrows, ncols,
                             figsize=(14, nrows * 3.5), squeeze=False)
    for idx, airport in enumerate(valid):
        ax  = axes[idx // ncols][idx % ncols]
        row = best_per_airport[best_per_airport['Airport'] == airport]
        if row.empty:
            ax.set_visible(False); continue
        bm  = row.iloc[0]['Model']
        bw  = int(row.iloc[0]['Window'])
        pd_ = all_preds.get(airport, {}).get(bm, {}).get(bw, {})
        if not pd_:
            ax.set_visible(False); continue
        steps = range(len(pd_['y_te']))
        ax.plot(steps, pd_['y_te'], 'o-', lw=1.8, ms=5,
                label='Actual', alpha=0.85)
        ax.plot(steps, pd_['p_te'], 's--', lw=1.8, ms=5,
                label='Predicted', alpha=0.85)
        ax.set_title(f'{airport}  |  {bm}  |  W={bw}',
                     fontsize=8.5, fontweight='bold')
        ax.set_xlabel('Test month')
        ax.set_ylabel('Passengers')
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f'{x:,.0f}'))
        ax.legend(fontsize=7)
        ax.grid(alpha=0.3)
    for i in range(len(valid), nrows * ncols):
        axes[i // ncols][i % ncols].set_visible(False)
    fig.suptitle(
        'Actual vs Predicted Passengers — Best Model+Window per Airport (Test Set)',
        fontsize=12, fontweight='bold')
    plt.tight_layout()
    fig.savefig(f'{OUT_DIR}/fig6_predictions_best.png',
                dpi=200, bbox_inches='tight')
    plt.close(fig)
    print('  ✓ fig6_predictions_best.png')

    # ── Done ──────────────────────────────────────────────────────────────────
    print(f'\n  All outputs → ./{OUT_DIR}/')
    print('  CSVs  : all_results.csv  best_per_airport.csv  '
          'model_ranking.csv  window_ranking.csv')
    print('  Plots : fig1_heatmap_rmse.png    fig2_heatmap_r2.png')
    print('          fig3_heatmap_mape.png    fig4_model_ranking.png')
    print('          fig5_window_comparison.png  fig6_predictions_best.png')
    print('\n' + '═'*W)
    print('  DONE')
    print('═'*W + '\n')


if __name__ == '__main__':
    main()