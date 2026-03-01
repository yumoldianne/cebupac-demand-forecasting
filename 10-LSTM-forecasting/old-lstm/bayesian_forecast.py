"""
Poisson-Gaussian Bayesian Forecasting for Airline Network
Simplified network-wide model: trend + seasonality only (no features)
Uses Negative Binomial to handle overdispersion
"""

import pandas as pd
import numpy as np
import pymc as pm
import arviz as az
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

if __name__ == '__main__':
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
        'Passenger': 'sum'
    }).reset_index()

    print(f"Network-wide training samples: {len(network_df)} months")
    print(f"Date range: {network_df['Date'].min()} to {network_df['Date'].max()}")
    print(f"Passenger range: {network_df['Passenger'].min():,.0f} to {network_df['Passenger'].max():,.0f}")

    # ==================== BUILD SIMPLIFIED MODEL ====================
    time_idx = network_df['time_idx'].values
    month = network_df['Month'].values
    passengers = network_df['Passenger'].values.astype(int)

    with pm.Model() as model:
        # Simplified model: trend + seasonality only (no features)
        intercept = pm.Normal('intercept', mu=13, sigma=2)
        trend = pm.Normal('trend', mu=0, sigma=0.1)
        seasonal = pm.Normal('seasonal', mu=0, sigma=1, shape=12)
        
        # Linear predictor
        mu = intercept + trend * time_idx + seasonal[month - 1]
        
        # Negative Binomial (handles overdispersion better than Poisson)
        lambda_ = pm.math.exp(mu)
        alpha = pm.HalfNormal('alpha', sigma=10)
        y = pm.NegativeBinomial('y', mu=lambda_, alpha=alpha, observed=passengers)

    print("Simplified Bayesian model built (trend + seasonality only)")
    print("Parameters: intercept, trend, seasonal[12], alpha = 15 total")

    # ==================== FIT MODEL ====================
    with model:
        trace = pm.sample(5000, tune=2000, chains=4, target_accept=0.95, 
                          return_inferencedata=True, random_seed=42)

    print("\nSampling complete!")
    
    # Check convergence
    summary = az.summary(trace, var_names=['intercept', 'trend', 'alpha'])
    print("\nModel Summary (Key Parameters):")
    print(summary)
    
    # Convergence diagnostics
    print("\n" + "="*60)
    print("CONVERGENCE DIAGNOSTICS")
    print("="*60)
    all_summary = az.summary(trace)
    
    # Check R-hat
    rhat_ok = (all_summary['r_hat'] < 1.01).all()
    print(f"✓ R-hat < 1.01 for all parameters: {rhat_ok}")
    if not rhat_ok:
        bad_rhat = all_summary[all_summary['r_hat'] >= 1.01]
        print(f"  WARNING: {len(bad_rhat)} parameters have R-hat >= 1.01")
    
    # Check ESS
    ess_ok = (all_summary['ess_bulk'] > 400).all()
    print(f"✓ ESS (bulk) > 400 for all parameters: {ess_ok}")
    if not ess_ok:
        bad_ess = all_summary[all_summary['ess_bulk'] <= 400]
        print(f"  WARNING: {len(bad_ess)} parameters have ESS <= 400")
    
    min_ess = all_summary['ess_bulk'].min()
    print(f"  Minimum ESS: {min_ess:.0f}")

    # ==================== FORECAST ====================
    max_time = network_df['time_idx'].max()
    max_date = network_df['Date'].max()

    # Create forecast dataframe
    forecasts = []
    for m in range(1, 13):
        forecast_time = max_time + m
        forecast_date = max_date + pd.DateOffset(months=m)
        
        forecasts.append({
            'time_idx': forecast_time,
            'Date': forecast_date,
            'Year': forecast_date.year,
            'Month': forecast_date.month
        })

    forecast_df = pd.DataFrame(forecasts)

    # Generate predictions
    time_idx_f = forecast_df['time_idx'].values
    month_f = forecast_df['Month'].values

    # Extract posterior samples
    intercept_post = trace.posterior['intercept'].values.flatten()
    trend_post = trace.posterior['trend'].values.flatten()
    seasonal_post = trace.posterior['seasonal'].values.reshape(-1, 12)
    alpha_post = trace.posterior['alpha'].values.flatten()

    n_samples = len(intercept_post)
    n_pred = len(forecast_df)
    predictions = np.zeros((n_samples, n_pred))

    # Generate predictions from posterior
    for s in range(n_samples):
        mu_pred = intercept_post[s] + trend_post[s] * time_idx_f + seasonal_post[s, month_f - 1]
        lambda_pred = np.exp(mu_pred)
        
        # Sample from Negative Binomial
        # Convert mu and alpha to n and p parameterization
        # NB(mu, alpha) where var = mu + mu^2/alpha
        p = alpha_post[s] / (alpha_post[s] + lambda_pred)
        n = alpha_post[s]
        predictions[s, :] = np.random.negative_binomial(n, p)

    # Add predictions to forecast
    forecast_df['Passenger_Mean'] = predictions.mean(axis=0)
    forecast_df['Passenger_Median'] = np.median(predictions, axis=0)
    forecast_df['Passenger_Lower'] = np.percentile(predictions, 2.5, axis=0)
    forecast_df['Passenger_Upper'] = np.percentile(predictions, 97.5, axis=0)
    forecast_df['Passenger_Std'] = predictions.std(axis=0)

    print(f"\nGenerated 12-month network-wide forecast")

    # ==================== RESULTS ====================
    print("\n" + "="*60)
    print("NETWORK-WIDE MONTHLY FORECAST")
    print("="*60)
    print(forecast_df[['Year', 'Month', 'Passenger_Mean', 'Passenger_Lower', 'Passenger_Upper']].to_string(index=False))

    print(f"\n12-Month Total: {forecast_df['Passenger_Mean'].sum():,.0f} passengers")
    print(f"95% CI: [{forecast_df['Passenger_Lower'].sum():,.0f}, {forecast_df['Passenger_Upper'].sum():,.0f}]")
    
    # Uncertainty metrics
    avg_width = (forecast_df['Passenger_Upper'] - forecast_df['Passenger_Lower']).mean()
    avg_pct = (avg_width / forecast_df['Passenger_Mean'].mean()) * 100
    print(f"\nAverage 95% CI width: {avg_width:,.0f} passengers ({avg_pct:.1f}% of mean)")

    # ==================== SAVE & VISUALIZE ====================
    forecast_df.to_csv('network_forecast.csv', index=False)

    # Plot network-wide forecast
    fig, ax = plt.subplots(1, 1, figsize=(14, 7))

    # Historical
    ax.plot(network_df['Date'], network_df['Passenger'], 'o-', 
            label='Historical', linewidth=2, markersize=6, color='black')

    # Forecast
    ax.plot(forecast_df['Date'], forecast_df['Passenger_Mean'], 's-', 
            label='Forecast (Mean)', linewidth=2, markersize=6, color='orangered')
    ax.fill_between(forecast_df['Date'], 
                    forecast_df['Passenger_Lower'], 
                    forecast_df['Passenger_Upper'],
                    alpha=0.3, color='orangered', label='95% Credible Interval')

    # Add vertical line at forecast start
    ax.axvline(x=network_df['Date'].max(), linestyle='--', color='gray', alpha=0.5)

    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Total Network Passengers', fontsize=12)
    ax.set_title('Network-Wide Passenger Forecast (Bayesian: Trend + Seasonality)', 
                 fontsize=14, fontweight='bold')
    ax.legend(fontsize=10, loc='best')
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('network_forecast.png', dpi=150)
    plt.show()

    # ==================== DIAGNOSTICS PLOT ====================
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Trace plots for key parameters
    az.plot_trace(trace, var_names=['intercept', 'trend'], axes=axes)
    
    plt.tight_layout()
    plt.savefig('diagnostics.png', dpi=150)
    
    print("\nFiles saved: network_forecast.csv, network_forecast.png, diagnostics.png")
    
    # ==================== ADDITIONAL DIAGNOSTICS ====================
    print("\n" + "="*60)
    print("POSTERIOR INTERPRETATION")
    print("="*60)
    
    # Trend interpretation
    trend_mean = trace.posterior['trend'].values.mean()
    trend_lower = np.percentile(trace.posterior['trend'].values, 2.5)
    trend_upper = np.percentile(trace.posterior['trend'].values, 97.5)
    
    monthly_growth = (np.exp(trend_mean) - 1) * 100
    annual_growth = (np.exp(trend_mean * 12) - 1) * 100
    
    print(f"Trend coefficient: {trend_mean:.4f} (95% CI: [{trend_lower:.4f}, {trend_upper:.4f}])")
    print(f"Implied monthly growth: {monthly_growth:+.2f}%")
    print(f"Implied annual growth: {annual_growth:+.2f}%")
    
    # Seasonal effects
    seasonal_means = trace.posterior['seasonal'].values.mean(axis=(0,1))
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                   'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    
    print(f"\nSeasonal effects (log-scale):")
    for i, (month_name, effect) in enumerate(zip(month_names, seasonal_means)):
        pct_effect = (np.exp(effect) - 1) * 100
        print(f"  {month_name}: {effect:+.3f} ({pct_effect:+.1f}%)")
    
    # Alpha (overdispersion)
    alpha_mean = trace.posterior['alpha'].values.mean()
    print(f"\nOverdispersion parameter (alpha): {alpha_mean:.2f}")
    print(f"  (Higher alpha = less overdispersion, Poisson limit as alpha→∞)")