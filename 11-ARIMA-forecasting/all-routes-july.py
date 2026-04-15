import os
import re
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pmdarima as pm
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_absolute_error, mean_squared_error

warnings.filterwarnings("ignore")

# ============================================================
# CONFIG
# ============================================================
DATA_PATH = r"combined.csv"
OUT_DIR = r"new_forecast_outputs"

FORECAST_MONTHS = 12   # future months to forecast
EVAL_MONTHS = 3        # holdout months for MAE / RMSE / MAPE
MIN_HISTORY_MONTHS = 12 # minimum months required to model a route

# Cut off anything from July 2025 onward
CUTOFF_DATE = pd.Timestamp("2025-07-01")

Path(OUT_DIR).mkdir(parents=True, exist_ok=True)


# ============================================================
# HELPERS
# ============================================================
def safe_filename(text: str) -> str:
    """Make a safe filename from route text."""
    text = str(text).strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "_", text)
    return text[:150]


def mape(y_true, y_pred):
    """Mean Absolute Percentage Error, ignoring zero actuals."""
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    mask = y_true != 0
    if mask.sum() == 0:
        return np.nan

    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100


def compute_metrics(y_true, y_pred):
    """Return MAE, RMSE, MAPE."""
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mape_val = mape(y_true, y_pred)

    return mae, rmse, mape_val


def load_and_prepare_data(path: str) -> pd.DataFrame:
    """Load CSV and prepare route-level monthly data."""
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()

    rename_map = {
        "DATE": "Date",
        "FROM": "From",
        "TO": "To",
        "AIRCRAFT": "Aircraft",
        "FLIGHT TIME": "Flight_Time",
        "STD": "STD",
        "ATD": "ATD",
        "STA": "STA",
        "STATUS": "Status",
        "FLIGHT NUMBER": "Flight_Number",
        "AIRLINE": "Airline",
        "# OF SEATS": "Seats",
        "LOAD FACTOR": "Load_Factor",
        "MONTH": "Month",
    }
    df = df.rename(columns=rename_map)

    # Parse date
    df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")

    # Parse time fields if present
    for col in ["STD", "ATD", "STA"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.time

    # Cut off data before July 2025
    df = df[df["Date"] < CUTOFF_DATE].copy()

    # Route label
    df["Route"] = df["From"].astype(str).str.strip() + "-" + df["To"].astype(str).str.strip()

    # Flag Cebu Pacific
    df["Is_CebuPac"] = df["Airline"].astype(str).str.contains("Cebu Pacific", case=False, na=False)

    # Numeric cleanup
    df["Seats"] = pd.to_numeric(df["Seats"], errors="coerce")
    df["Load_Factor"] = pd.to_numeric(df["Load_Factor"], errors="coerce")

    # Passenger proxy from load factor
    df["Passengers"] = pd.to_numeric(df["Load_Factor"], errors="coerce")

    # Monthly timestamp for route aggregation
    df["Month_Date"] = df["Date"].dt.to_period("M").dt.to_timestamp()

    # Drop unusable rows
    df = df.dropna(subset=["Date", "Route", "Passengers", "Month_Date"])

    return df


def aggregate_by_route(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate to route-month level.
    Returns:
      Route, Month_Date, Total_Passengers, CebuPac_Passengers
    """
    total_market = (
        df.groupby(["Route", "Month_Date"], as_index=False)["Passengers"]
        .sum()
        .rename(columns={"Passengers": "Total_Passengers"})
    )

    cebu_pac = (
        df[df["Is_CebuPac"]]
        .groupby(["Route", "Month_Date"], as_index=False)["Passengers"]
        .sum()
        .rename(columns={"Passengers": "CebuPac_Passengers"})
    )

    combined = total_market.merge(cebu_pac, on=["Route", "Month_Date"], how="left")
    combined["CebuPac_Passengers"] = combined["CebuPac_Passengers"].fillna(0)

    return combined


def fit_forecast_model(series: pd.Series, forecast_steps: int = 12) -> dict:
    """
    Fit auto ARIMA, fallback to manual SARIMA.
    Returns forecast and confidence intervals as numpy arrays.
    """
    try:
        y = pd.Series(series).astype(float).dropna().values

        model = pm.auto_arima(
            y,
            seasonal=True,
            m=12,
            start_p=0, start_q=0,
            max_p=3, max_q=3,
            start_P=0, start_Q=0,
            max_P=2, max_Q=2,
            d=None, D=None,
            trace=False,
            error_action="ignore",
            suppress_warnings=True,
            stepwise=True,
            n_jobs=-1,
        )

        forecast, conf_int = model.predict(n_periods=forecast_steps, return_conf_int=True)

        forecast = np.asarray(forecast).ravel()
        conf_int = np.asarray(conf_int)

        return {
            "model": model,
            "forecast": forecast,
            "lower_ci": conf_int[:, 0],
            "upper_ci": conf_int[:, 1],
            "order": model.order,
            "seasonal_order": model.seasonal_order,
            "aic": model.aic(),
            "success": True,
            "method": "auto_arima",
        }

    except Exception:
        try:
            y = pd.Series(series).astype(float).dropna().values

            model = SARIMAX(
                y,
                order=(1, 1, 1),
                seasonal_order=(1, 1, 1, 12),
                enforce_stationarity=False,
                enforce_invertibility=False,
            )

            fitted = model.fit(disp=False, maxiter=200)
            forecast_obj = fitted.get_forecast(steps=forecast_steps)
            forecast = forecast_obj.predicted_mean
            conf_int = forecast_obj.conf_int()

            return {
                "model": fitted,
                "forecast": np.asarray(forecast).ravel(),
                "lower_ci": conf_int.iloc[:, 0].to_numpy(),
                "upper_ci": conf_int.iloc[:, 1].to_numpy(),
                "order": (1, 1, 1),
                "seasonal_order": (1, 1, 1, 12),
                "aic": fitted.aic,
                "success": True,
                "method": "manual_sarima",
            }

        except Exception as e2:
            print(f"    ⚠ Model failed: {str(e2)[:80]}")
            return {"success": False, "error": str(e2), "method": "failed"}


def evaluate_holdout(series: pd.Series, eval_months: int = 3) -> dict:
    """
    Train on all but last eval_months, forecast holdout, and compute metrics.
    Returns metrics plus fitted model info.
    """
    y = pd.Series(series).astype(float).dropna().reset_index(drop=True)

    if len(y) <= eval_months:
        return {"success": False, "error": "Not enough history for holdout evaluation"}

    train = y.iloc[:-eval_months]
    test = y.iloc[-eval_months:]

    result = fit_forecast_model(train, forecast_steps=eval_months)
    if not result["success"]:
        return {"success": False, "error": "Model fitting failed during evaluation"}

    pred = np.asarray(result["forecast"]).ravel()
    mae, rmse, mape_val = compute_metrics(test.values, pred)

    return {
        "success": True,
        "mae": mae,
        "rmse": rmse,
        "mape": mape_val,
        "order": result["order"],
        "seasonal_order": result["seasonal_order"],
        "aic": result["aic"],
        "method": result["method"],
    }


def plot_route_forecast(
    route: str,
    dates: pd.Series,
    total_hist: pd.Series,
    total_forecast: np.ndarray,
    cebu_hist: pd.Series,
    cebu_forecast: np.ndarray,
    forecast_dates: list,
    output_path: str,
):
    """
    Create dual forecast plot.
    Orange = Total market
    Red = Cebu Pacific
    """
    fig, ax = plt.subplots(figsize=(14, 7))

    ax.plot(
        dates, total_hist, "o-", color="#E67E22",
        linewidth=2, markersize=4, label="Total Market (All Airlines)", alpha=0.8
    )
    ax.plot(
        dates, cebu_hist, "o-", color="#E74C3C",
        linewidth=2, markersize=4, label="Cebu Pacific", alpha=0.8
    )

    ax.plot(
        forecast_dates, total_forecast, "s--", color="#E67E22",
        linewidth=2.5, markersize=6, label="Total Market Forecast", alpha=0.9
    )
    ax.plot(
        forecast_dates, cebu_forecast, "s--", color="#E74C3C",
        linewidth=2.5, markersize=6, label="Cebu Pacific Forecast", alpha=0.9
    )

    split_date = dates.iloc[-1]
    ax.axvline(
        x=split_date, color="#3498DB", linestyle="--",
        linewidth=2, alpha=0.6, label="Forecast Start"
    )

    ax.set_title(
        f"{route} — Passenger Forecast (Total Market vs Cebu Pacific)",
        fontsize=14, fontweight="bold", pad=15
    )
    ax.set_xlabel("Date", fontsize=11)
    ax.set_ylabel("Passengers", fontsize=11)
    ax.legend(loc="best", fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:,.0f}"))

    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.xticks(rotation=45, ha="right")

    plt.tight_layout()
    plt.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close()


# ============================================================
# MAIN
# ============================================================
def main():
    print("\n" + "=" * 80)
    print("ARIMA/SARIMA ROUTE FORECASTING — CEBU PACIFIC")
    print("=" * 80)

    print("\n► Loading data...")
    df = load_and_prepare_data(DATA_PATH)

    cebu_routes = df[df["Is_CebuPac"]]["Route"].dropna().unique()
    print(f"  Found {len(cebu_routes)} routes served by Cebu Pacific")

    print("\n► Aggregating by route...")
    route_data = aggregate_by_route(df)
    route_data = route_data[route_data["Route"].isin(cebu_routes)].copy()

    results = []
    forecast_data = []

    print(f"\n► Processing {len(cebu_routes)} routes...\n")

    for idx, route in enumerate(sorted(cebu_routes), 1):
        print(f"{'─' * 80}")
        print(f"[{idx}/{len(cebu_routes)}] {route}")

        route_df = (
            route_data[route_data["Route"] == route]
            .sort_values("Month_Date")
            .reset_index(drop=True)
        )

        if len(route_df) < MIN_HISTORY_MONTHS:
            print(f"  ⚠ Insufficient data ({len(route_df)} months)")
            continue

        dates = route_df["Month_Date"]
        total_pax = route_df["Total_Passengers"].astype(float)
        cebu_pax = route_df["CebuPac_Passengers"].astype(float)

        total_sum = total_pax.sum()
        cebu_sum = cebu_pax.sum()
        share = (cebu_sum / total_sum * 100) if total_sum != 0 else 0

        print(f"  History: {len(route_df)} months")
        print(f"  Total market avg: {total_pax.mean():,.2f} pax/month")
        print(f"  Cebu Pac avg: {cebu_pax.mean():,.2f} pax/month")
        print(f"  Cebu Pac share: {share:.1f}%")

        print(f"  Evaluating holdout metrics ({EVAL_MONTHS} months)...")
        total_eval = evaluate_holdout(total_pax, EVAL_MONTHS)
        cebu_eval = evaluate_holdout(cebu_pax, EVAL_MONTHS)

        if total_eval["success"]:
            print(
                f"    Total Market — MAE: {total_eval['mae']:.2f}, "
                f"RMSE: {total_eval['rmse']:.2f}, MAPE: {total_eval['mape']:.2f}%"
            )
        else:
            print("    ⚠ Total market evaluation failed")

        if cebu_eval["success"]:
            print(
                f"    Cebu Pacific — MAE: {cebu_eval['mae']:.2f}, "
                f"RMSE: {cebu_eval['rmse']:.2f}, MAPE: {cebu_eval['mape']:.2f}%"
            )
        else:
            print("    ⚠ Cebu Pacific evaluation failed")

        print("  Forecasting total market...")
        total_result = fit_forecast_model(total_pax, FORECAST_MONTHS)
        if not total_result["success"]:
            print("  ✗ Total market forecast failed")
            continue

        print(
            f"    ✓ Model: SARIMA{total_result['order']}{total_result['seasonal_order']} "
            f"(AIC: {total_result['aic']:.0f})"
        )

        print("  Forecasting Cebu Pacific...")
        cebu_result = fit_forecast_model(cebu_pax, FORECAST_MONTHS)
        if not cebu_result["success"]:
            print("  ✗ Cebu Pacific forecast failed")
            continue

        print(
            f"    ✓ Model: SARIMA{cebu_result['order']}{cebu_result['seasonal_order']} "
            f"(AIC: {cebu_result['aic']:.0f})"
        )

        last_date = dates.iloc[-1]
        forecast_dates = [last_date + pd.DateOffset(months=i + 1) for i in range(FORECAST_MONTHS)]

        total_fc = np.asarray(total_result["forecast"]).ravel()
        total_lci = np.asarray(total_result["lower_ci"]).ravel()
        total_uci = np.asarray(total_result["upper_ci"]).ravel()

        cebu_fc = np.asarray(cebu_result["forecast"]).ravel()
        cebu_lci = np.asarray(cebu_result["lower_ci"]).ravel()
        cebu_uci = np.asarray(cebu_result["upper_ci"]).ravel()

        print("  Creating forecast plot...")
        plot_path = os.path.join(OUT_DIR, f"{safe_filename(route)}_forecast.png")
        plot_route_forecast(
            route=route,
            dates=dates,
            total_hist=total_pax,
            total_forecast=total_fc,
            cebu_hist=cebu_pax,
            cebu_forecast=cebu_fc,
            forecast_dates=forecast_dates,
            output_path=plot_path,
        )

        results.append({
            "Route": route,
            "History_Months": len(route_df),
            "Total_Avg_Historical": total_pax.mean(),
            "CebuPac_Avg_Historical": cebu_pax.mean(),
            "Market_Share_Pct": share,
            "Total_MAE": total_eval["mae"] if total_eval["success"] else np.nan,
            "Total_RMSE": total_eval["rmse"] if total_eval["success"] else np.nan,
            "Total_MAPE": total_eval["mape"] if total_eval["success"] else np.nan,
            "CebuPac_MAE": cebu_eval["mae"] if cebu_eval["success"] else np.nan,
            "CebuPac_RMSE": cebu_eval["rmse"] if cebu_eval["success"] else np.nan,
            "CebuPac_MAPE": cebu_eval["mape"] if cebu_eval["success"] else np.nan,
            "Total_Model": f"SARIMA{total_result['order']}{total_result['seasonal_order']}",
            "Total_AIC": total_result["aic"],
            "Total_Forecast_Avg": total_fc.mean(),
            "CebuPac_Model": f"SARIMA{cebu_result['order']}{cebu_result['seasonal_order']}",
            "CebuPac_AIC": cebu_result["aic"],
            "CebuPac_Forecast_Avg": cebu_fc.mean(),
        })

        for i, fc_date in enumerate(forecast_dates):
            forecast_data.append({
                "Route": route,
                "Date": fc_date,
                "Total_Market_Forecast": total_fc[i],
                "Total_Market_Lower_CI": total_lci[i],
                "Total_Market_Upper_CI": total_uci[i],
                "CebuPac_Forecast": cebu_fc[i],
                "CebuPac_Lower_CI": cebu_lci[i],
                "CebuPac_Upper_CI": cebu_uci[i],
            })

        print("  ✓ Complete")

    results_df = pd.DataFrame(results)
    forecast_df = pd.DataFrame(forecast_data)

    results_path = os.path.join(OUT_DIR, "route_forecast_summary.csv")
    forecast_path = os.path.join(OUT_DIR, "route_forecasts.csv")

    results_df.to_csv(results_path, index=False)
    forecast_df.to_csv(forecast_path, index=False)

    print("\n" + "=" * 80)
    print("DONE")
    print(f"Summary saved to: {results_path}")
    print(f"Forecasts saved to: {forecast_path}")
    print(f"Plots saved to folder: {OUT_DIR}")
    print("=" * 80)


if __name__ == "__main__":
    main()