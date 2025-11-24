# Implied Volatility (IV) Calculation Formula

## Overview
The application calculates **True Implied Volatility** using `py_vollib` Black-Scholes model for options, or **Historical Volatility** (as a proxy) for underlying assets.

## Formula for Options (using py_vollib)

For option symbols (ending with CE/PE), the application uses the **Black-Scholes Implied Volatility** calculation from `py_vollib`:

### Black-Scholes Model
```
IV = implied_volatility(price, S, K, t, r, flag)
```

Where:
- `price` = Option price (from historical OHLC data)
- `S` = Underlying asset price (spot price)
- `K` = Strike price (extracted from option symbol)
- `t` = Time to expiration in years
- `r` = Risk-free interest rate (default: 0.07 or 7% = 91-day Indian T-Bill yield)
- `flag` = 'c' for call options, 'p' for put options

The function uses numerical methods to solve for the implied volatility that makes the Black-Scholes formula match the observed option price.

### Option Symbol Parsing
The application automatically parses Indian option symbols (e.g., `NIFTY25N1825700PE`):
- Extracts underlying symbol (NIFTY)
- Extracts expiry date (25 = 2025, N = November)
- Extracts strike price (18257.00)
- Determines option type (PE = Put, CE = Call)

## Formula for Underlying Assets (Historical Volatility)

For non-option symbols, the application calculates **Historical Volatility** as a proxy for IV:

### Step 1: Calculate Log Returns
```
r_t = ln(P_t / P_{t-1})
```
Where:
- `r_t` = log return at time t
- `P_t` = closing price at time t
- `P_{t-1}` = closing price at previous time period
- `ln` = natural logarithm

### Step 2: Calculate Rolling Standard Deviation
```
σ_rolling = std(r_t) over rolling window
```
Where:
- `std()` = standard deviation
- Rolling window = 20 periods (default)

### Step 3: Annualize the Volatility
```
IV = σ_rolling × √(periods_per_year) × 100
```

Where:
- `periods_per_year` depends on the timeframe:
  - **1 Minute**: 252 × 375 = 94,500 periods/year
  - **5 Minutes**: 252 × 75 = 18,900 periods/year
  - **15 Minutes**: 252 × 25 = 6,300 periods/year
  - **30 Minutes**: 252 × 12 = 3,024 periods/year
  - **1 Hour**: 252 × 6 = 1,512 periods/year
  - **1 Day**: 252 trading days/year

The result is multiplied by 100 to convert to percentage.

## Complete Formula
```
IV = std(ln(P_t / P_{t-1})) × √(N) × 100
```

Where:
- `N` = number of periods per year based on timeframe
- Result is in percentage (%)

## Implementation Details

### Rolling Window
- Default window size: **20 periods**
- If data has fewer than 20 periods, uses all available data
- Uses pandas `rolling().std()` for calculation

### Annualization Factor
The annualization factor (`√(periods_per_year)`) converts the volatility from the selected timeframe to an annualized measure, making it comparable across different timeframes.

### Example Calculation
For daily data (1D timeframe):
1. Calculate log returns: `ln(Close_t / Close_{t-1})`
2. Calculate 20-day rolling standard deviation
3. Annualize: `σ × √252 × 100`

Result: Annualized volatility as a percentage.

## Notes

1. **For Options**: The application uses `py_vollib.black_scholes.implied_volatility` to calculate true implied volatility from option prices using the Black-Scholes model.

2. **For Underlying Assets**: The application calculates Historical Volatility (realized volatility) as a proxy for implied volatility when option prices are not available.

3. **Automatic Detection**: The application automatically detects if a symbol is an option (ends with CE/PE) and uses the appropriate calculation method.

4. **Risk-Free Rate**: Default is 7% (0.07) = 91-day Indian T-Bill yield. This can be adjusted in the code if needed.

5. **Time to Expiry**: Calculated dynamically for each timestamp in the historical data, accounting for the option's expiry date.

