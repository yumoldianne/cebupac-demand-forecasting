import os
import re
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error

warnings.filterwarnings("ignore")

# ============================================================
# CONFIG
# ============================================================

DATA_PATH = "combined.csv"
OUT_DIR = "linreg_forecasts_by_route"
os.makedirs(OUT_DIR, exist_ok=True)

FORECAST_MONTHS = 12
MIN_HISTORY_MONTHS = 12
TRAIN_SPLIT = 0.80

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def safe_filename(text: str) -> str:
    """Make a safe filename from route text."""
    text = str(text).strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "_", text)
    return text[:150]


def load_and_prepare_data(path: str) -> pd.DataFrame:
    """Load CSV and prepare route-level data based on the actual column names."""
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()

    # Parse actual DATE column
    df["DATE"] = pd.to_datetime(df["DATE"], dayfirst=True, errors="coerce")

    # Clean origin and destination
    df["FROM"] = df["FROM"].astype(str).str.strip()
    df["TO"] = df["TO"].astype(str).str.strip()
    df["AIRLINE"] = df["AIRLINE"].astype(str).str.strip()

    # Create route identifier
    df["Route"] = df["FROM"] + "-" + df["TO"]

    # Flag Cebu Pacific flights
    df["Is_CebuPac"] = df["AIRLINE"].str.contains("Cebu Pacific", case=False, na=False)

    # Monthly timestamp for aggregation
    df["Month_Date"] = df["DATE"].dt.to_period("M").dt.to_timestamp()

    # Use load factor as passengers, per your instruction
    df["Passengers"] = pd.to_numeric(df["LOAD FACTOR"], errors="coerce")

    # Clean route values
    df = df.dropna(subset=["DATE", "Month_Date", "Passengers", "Route"])
    df = df[df["Route"].astype(str).str.lower().ne("nan-nan")]

    return df


def aggregate_by_route(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate data by route and month.
    Returns dataframe with total market and Cebu Pacific passengers.
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


def create_time_features(dates: pd.Series, start_date: pd.Timestamp) -> pd.DataFrame:
    """Create simple time index for linear regression."""
    features = pd.DataFrame(index=range(len(dates)))
    features["time_index"] = (pd.to_datetime(dates).reset_index(drop=True) - start_date).dt.days / 30.44
    return features


def calculate_metrics(actual: np.ndarray, predicted: np.ndarray) -> dict:
    """Calculate MAE, RMSE, and MAPE for model evaluation."""
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)

    mae = mean_absolute_error(actual, predicted)
    rmse = np.sqrt(mean_squared_error(actual, predicted))

    mask = actual != 0
    if mask.sum() > 0:
        mape = np.mean(np.abs((actual[mask] - predicted[mask]) / actual[mask])) * 100
    else:
        mape = np.nan

    ss_res = np.sum((actual - predicted) ** 2)
    ss_tot = np.sum((actual - np.mean(actual)) ** 2)
    r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

    return {
        "MAE": mae,
        "RMSE": rmse,
        "MAPE": mape,
        "R2": r2,
    }


def linear_regression_forecast(
    dates: pd.Series,
    values: pd.Series,
    forecast_steps: int = 12,
    train_split: float = 0.80
) -> dict:
    """
    Fit simple linear regression model and generate forecast.
    """
    try:
        dates = pd.Series(dates).reset_index(drop=True)
        values = pd.Series(values).astype(float).reset_index(drop=True)

        if len(dates) < MIN_HISTORY_MONTHS:
            return {"success": False, "error": "Not enough history"}

        start_date = dates.iloc[0]
        X = create_time_features(dates, start_date)
        y = values.values

        # Train/test split
        n_train = max(int(len(dates) * train_split), 1)
        if n_train >= len(dates):
            n_train = len(dates) - 1

        X_train = X.iloc[:n_train]
        X_test = X.iloc[n_train:]
        y_train = y[:n_train]
        y_test = y[n_train:]

        # Fit on training set
        model = LinearRegression()
        model.fit(X_train, y_train)

        # Evaluate on test set if available
        if len(y_test) > 0:
            y_test_pred = model.predict(X_test)
            test_metrics = calculate_metrics(y_test, y_test_pred)
        else:
            test_metrics = {"MAE": np.nan, "RMSE": np.nan, "MAPE": np.nan, "R2": np.nan}

        # Refit on full data
        model_full = LinearRegression()
        model_full.fit(X, y)

        # Future dates
        last_date = dates.iloc[-1]
        future_dates = pd.Series([
            last_date + pd.DateOffset(months=i + 1)
            for i in range(forecast_steps)
        ])

        X_future = create_time_features(future_dates, start_date)

        # Forecast
        forecast = model_full.predict(X_future)
        forecast = np.maximum(forecast, 0)

        # Approximate confidence interval using residuals
        y_pred_full = model_full.predict(X)
        residuals = y - y_pred_full
        std_residual = np.std(residuals) if len(residuals) > 1 else 0

        lower_ci = np.maximum(forecast - 1.96 * std_residual, 0)
        upper_ci = forecast + 1.96 * std_residual

        return {
            "model": model_full,
            "forecast": forecast,
            "lower_ci": lower_ci,
            "upper_ci": upper_ci,
            "mae": test_metrics["MAE"],
            "rmse": test_metrics["RMSE"],
            "mape": test_metrics["MAPE"],
            "r2": test_metrics["R2"],
            "slope": float(model_full.coef_[0]),
            "intercept": float(model_full.intercept_),
            "success": True,
        }

    except Exception as e:
        print(f"    ⚠ Linear regression failed: {str(e)[:120]}")
        return {"success": False, "error": str(e)}


def plot_route_forecast(
    route: str,
    dates: pd.Series,
    total_hist: pd.Series,
    total_forecast: np.ndarray,
    cebu_hist: pd.Series,
    cebu_forecast: np.ndarray,
    forecast_dates: list,
    output_path: str
):
    """Create dual forecast plot."""
    fig, ax = plt.subplots(figsize=(14, 7))

    ax.plot(
        dates, total_hist, "o-", linewidth=2, markersize=4,
        label="Total Market (All Airlines)", alpha=0.8
    )
    ax.plot(
        dates, cebu_hist, "o-", linewidth=2, markersize=4,
        label="Cebu Pacific", alpha=0.8
    )

    ax.plot(
        forecast_dates, total_forecast, "s--", linewidth=2.5, markersize=6,
        label="Total Market Forecast (Simple LinReg)", alpha=0.9
    )
    ax.plot(
        forecast_dates, cebu_forecast, "s--", linewidth=2.5, markersize=6,
        label="Cebu Pacific Forecast (Simple LinReg)", alpha=0.9
    )

    split_date = dates.iloc[-1]
    ax.axvline(
        x=split_date, linestyle="--", linewidth=2, alpha=0.6,
        label="Forecast Start"
    )

    ax.set_title(
        f"{route} — Simple Linear Regression Forecast (Total Market vs Cebu Pacific)",
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
    print("SIMPLE LINEAR REGRESSION ROUTE FORECASTING — CEBU PACIFIC")
    print("=" * 80)

    print("\n► Loading data...")
    df = load_and_prepare_data(DATA_PATH)

    print("\n► Aggregating by route...")
    route_data = aggregate_by_route(df)

    # Routes actually served by Cebu Pacific
    cebu_routes = (
        route_data.loc[route_data["CebuPac_Passengers"] > 0, "Route"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    cebu_routes = sorted(cebu_routes, key=str)
    print(f"  Found {len(cebu_routes)} routes served by Cebu Pacific")

    # Filter to Cebu Pacific routes only
    route_data = route_data[route_data["Route"].isin(cebu_routes)].copy()

    results = []
    forecast_data = []

    print(f"\n► Processing {len(cebu_routes)} routes...\n")

    for idx, route in enumerate(cebu_routes, 1):
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
        total_pax = route_df["Total_Passengers"]
        cebu_pax = route_df["CebuPac_Passengers"]

        total_sum = total_pax.sum()
        cebu_sum = cebu_pax.sum()
        share = (cebu_sum / total_sum * 100) if total_sum != 0 else 0

        print(f"  History: {len(route_df)} months")
        print(f"  Total market avg: {total_pax.mean():,.2f} pax/month")
        print(f"  Cebu Pac avg: {cebu_pax.mean():,.2f} pax/month")
        print(f"  Cebu Pac share: {share:.1f}%")

        # Forecast total market
        print("  Forecasting total market...")
        total_result = linear_regression_forecast(
            dates, total_pax, FORECAST_MONTHS, TRAIN_SPLIT
        )

        if not total_result["success"]:
            print("  ✗ Total market forecast failed")
            continue

        print(
            f"    ✓ Simple Linear Regression (R² = {total_result['r2']:.3f}, "
            f"Slope = {total_result['slope']:.1f} pax/month)"
        )
        print(
            f"      MAE: {total_result['mae']:,.2f} | "
            f"RMSE: {total_result['rmse']:,.2f} | "
            f"MAPE: {total_result['mape']:.2f}%"
        )

        # Forecast Cebu Pacific
        print("  Forecasting Cebu Pacific...")
        cebu_result = linear_regression_forecast(
            dates, cebu_pax, FORECAST_MONTHS, TRAIN_SPLIT
        )

        if not cebu_result["success"]:
            print("  ✗ Cebu Pacific forecast failed")
            continue

        print(
            f"    ✓ Simple Linear Regression (R² = {cebu_result['r2']:.3f}, "
            f"Slope = {cebu_result['slope']:.1f} pax/month)"
        )
        print(
            f"      MAE: {cebu_result['mae']:,.2f} | "
            f"RMSE: {cebu_result['rmse']:,.2f} | "
            f"MAPE: {cebu_result['mape']:.2f}%"
        )

        # Future dates
        last_date = dates.iloc[-1]
        forecast_dates = [last_date + pd.DateOffset(months=i + 1) for i in range(FORECAST_MONTHS)]

        # Plot
        print("  Creating forecast plot...")
        plot_path = os.path.join(OUT_DIR, f"{safe_filename(route)}_forecast.png")
        plot_route_forecast(
            route=route,
            dates=dates,
            total_hist=total_pax,
            total_forecast=total_result["forecast"],
            cebu_hist=cebu_pax,
            cebu_forecast=cebu_result["forecast"],
            forecast_dates=forecast_dates,
            output_path=plot_path
        )

        # Store results
        results.append({
            "Route": route,
            "History_Months": len(route_df),
            "Total_Avg_Historical": total_pax.mean(),
            "CebuPac_Avg_Historical": cebu_pax.mean(),
            "Market_Share_Pct": share,
            "Total_R2": total_result["r2"],
            "Total_MAE": total_result["mae"],
            "Total_RMSE": total_result["rmse"],
            "Total_MAPE": total_result["mape"],
            "CebuPac_R2": cebu_result["r2"],
            "CebuPac_MAE": cebu_result["mae"],
            "CebuPac_RMSE": cebu_result["rmse"],
            "CebuPac_MAPE": cebu_result["mape"],
            "Total_Slope": total_result["slope"],
            "CebuPac_Slope": cebu_result["slope"],
            "Total_Forecast_Avg": total_result["forecast"].mean(),
            "CebuPac_Forecast_Avg": cebu_result["forecast"].mean(),
        })

        # Store forecast data
        for i, fc_date in enumerate(forecast_dates):
            forecast_data.append({
                "Route": route,
                "Date": fc_date,
                "Total_Market_Forecast": total_result["forecast"][i],
                "Total_Market_Lower_CI": total_result["lower_ci"][i],
                "Total_Market_Upper_CI": total_result["upper_ci"][i],
                "CebuPac_Forecast": cebu_result["forecast"][i],
                "CebuPac_Lower_CI": cebu_result["lower_ci"][i],
                "CebuPac_Upper_CI": cebu_result["upper_ci"][i],
            })

        print("  ✓ Complete")

    # Save results
    print("\n" + "=" * 80)
    print("SAVING RESULTS")
    print("=" * 80)

    results_df = pd.DataFrame(results)
    forecast_df = pd.DataFrame(forecast_data)

    results_path = os.path.join(OUT_DIR, "route_forecast_summary.csv")
    forecast_path = os.path.join(OUT_DIR, "route_forecasts_12months.csv")

    results_df.to_csv(results_path, index=False)
    forecast_df.to_csv(forecast_path, index=False)

    print(f"\n✓ Saved: {results_path}")
    print(f"✓ Saved: {forecast_path}")

    print("\n" + "=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)

    if not results_df.empty:
        print(f"\n📊 Routes forecasted: {len(results_df)}")
        print(f"   Average market share: {results_df['Market_Share_Pct'].mean():.1f}%")
        print(f"   Median market share: {results_df['Market_Share_Pct'].median():.1f}%")

        print(f"\n📈 Total Market Forecasts:")
        print(f"   Average forecast: {results_df['Total_Forecast_Avg'].mean():,.2f} pax/month")
        print(f"   Range: {results_df['Total_Forecast_Avg'].min():,.2f} - {results_df['Total_Forecast_Avg'].max():,.2f}")

        print(f"\n📈 Cebu Pacific Forecasts:")
        print(f"   Average forecast: {results_df['CebuPac_Forecast_Avg'].mean():,.2f} pax/month")
        print(f"   Range: {results_df['CebuPac_Forecast_Avg'].min():,.2f} - {results_df['CebuPac_Forecast_Avg'].max():,.2f}")

        print(f"\n📊 Total Market Model Performance:")
        print(f"   Average R²:   {results_df['Total_R2'].mean():.3f}")
        print(f"   Average MAE:  {results_df['Total_MAE'].mean():,.2f}")
        print(f"   Average RMSE: {results_df['Total_RMSE'].mean():,.2f}")
        print(f"   Average MAPE: {results_df['Total_MAPE'].mean():.2f}%")

        print(f"\n📊 Cebu Pacific Model Performance:")
        print(f"   Average R²:   {results_df['CebuPac_R2'].mean():.3f}")
        print(f"   Average MAE:  {results_df['CebuPac_MAE'].mean():,.2f}")
        print(f"   Average RMSE: {results_df['CebuPac_RMSE'].mean():,.2f}")
        print(f"   Average MAPE: {results_df['CebuPac_MAPE'].mean():.2f}%")

        print(f"\n🏆 TOP 10 ROUTES BY CEBU PAC FORECAST ACCURACY (Lowest MAPE):")
        best_routes = results_df.nsmallest(10, "CebuPac_MAPE")
        for i, (_, row) in enumerate(best_routes.iterrows(), 1):
            print(
                f"   {i:>2}. {row['Route']:<35} "
                f"MAPE={row['CebuPac_MAPE']:>7.2f}%  "
                f"R²={row['CebuPac_R2']:>6.3f}  "
                f"MAE={row['CebuPac_MAE']:>10,.2f}"
            )

    print("\n" + "=" * 80)
    print("COMPLETE")
    print("=" * 80)
    print(f"\nGenerated outputs in: ./{OUT_DIR}/")
    print(f"  • route_forecast_summary.csv")
    print(f"  • route_forecasts_12months.csv")
    print(f"  • route forecast plots")
    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    main()