import nt_cron.kalshi_client as kalshi_client
from rich import print
import polars as pl
import uuid
import datetime as dt

def calculate_kalshi_fee_expr(contracts_col: str, price_cents_col: str) -> pl.Expr:
    """
    Calculate Kalshi trading fee as a Polars expression based on their fee formula:
    fees = round(0.07 x C x P x (1-P), 2)

    Args:
        contracts_col: Name of the column containing number of contracts (C)
        price_cents_col: Name of the column containing price in cents (P in formula is price/100)

    Returns:
        Polars expression for fee in cents, rounded to 2 decimal places
    """
    P = pl.col(price_cents_col) / 100.0  # Convert cents to dollar price
    fee = 0.07 * pl.col(contracts_col) * P * (1 - P)
    return fee.round(2)

def calculate_max_contracts_expr(dollars_col: str, price_cents_col: str) -> pl.Expr:
    """
    Calculate maximum number of contracts as a Polars expression given a budget,
    accounting for trading fees.

    Inverted formula from:
    total_cost = C × P + 0.07 × C × P × (1-P)
    C = budget / (P × (1 + 0.07 × (1-P)))

    Args:
        dollars_col: Name of the column containing budget in dollars
        price_cents_col: Name of the column containing price per contract in cents

    Returns:
        Polars expression for maximum number of contracts (floored to integer)
    """
    P = pl.col(price_cents_col) / 100.0  # Convert cents to dollar price

    # Direct calculation using inverted formula
    max_contracts = pl.col(dollars_col) / (P * (1 + 0.07 * (1 - P)))

    # Floor to get integer number of contracts
    return max_contracts.floor().cast(pl.Int64)

calculate_trade_contracts = (
    pl.col('trade_dollars') /
    (pl.col('yes_ask') / 100 * (1.07 * (1 - pl.col('yes-ask') / 100)))
)

def get_portfolio_value() -> float:
    portfolio_value = kalshi_client.get_portfolio_balance()
    return portfolio_value

def get_open_college_football_markets() -> pl.DataFrame:
    markets = kalshi_client.get_markets(series_ticker="KXNCAAFGAME", status="open")
    return markets

def get_portfolio_weights(markets: pl.DataFrame, portfolio_value: float) -> pl.DataFrame:
    today = dt.date.today()

    weights = (
        pl.DataFrame(markets)
        .select(
            "ticker",
            "expected_expiration_time",
            "yes_ask",
        )
        # Cast datetime
        .with_columns(
            pl.col("expected_expiration_time").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ").dt.convert_time_zone("utc")
        )
        # Get game day
        .with_columns(
            pl.col('expected_expiration_time').dt.convert_time_zone('America/Denver').dt.date().alias('game_day')
        )
        # Filter for games priced between 90-99 cents that happen today
        .filter(
            pl.col('yes_ask').is_between(90, 99),
            pl.col('game_day').eq(today)
        )
        # Allocate portfolio equally across all qualifying games
        .with_columns(
            pl.lit(1).truediv(pl.len()).alias('weight')
        )
        # Compute dollar allocation
        .with_columns(
            pl.col('weight').mul(portfolio_value).alias('dollars')
        )
        .select(
            'ticker',
            'yes_ask',
            'weight',
            'dollars'
        )
        .sort('ticker')
    )

    return weights

def get_trades(weights: pl.DataFrame) -> list[dict]:
    return (
        weights
        .with_columns(
            calculate_max_contracts_expr('dollars', 'yes_ask').alias('contracts'),
        )
        .select(
            'ticker',
            'yes_ask',
            'contracts',
        )
        .to_dicts()
    )

def urp_strategy_job():
    """
    URP (Underpriced Risk Premium) Strategy for College Football

    Strategy:
    - Only run on Thursday, Friday, and Saturday
    - Get current portfolio value
    - Find all active college football markets (KXCFBGAME series)
    - Buy YES contracts for games happening today that are priced between 90-99 cents
    - Allocate portfolio equally across all qualifying games
    """
    # 1. Check if today is Thursday (4), Friday (5), or Saturday (6)
    today = dt.date.today()
    if today.isoweekday() not in [4, 5, 6]:
        print(f"Today is {today.strftime('%A')} - strategy only runs on Thursday, Friday, and Saturday")
        return

    # 2. Get current portfolio balance
    portfolio_value = get_portfolio_value()

    # 3. Get active college football markets
    markets = get_open_college_football_markets()

    # 4. Process markets and identify trades
    weights = get_portfolio_weights(markets, portfolio_value)

    if len(weights) == 0:
        print("No trades to execute")
        return
    
    # 5. Get trades
    trades = get_trades(weights)

    # 6. Execute trades
    for trade in trades:
        order = kalshi_client.create_order(
            action='buy',
            count=trade['contracts'],
            side="yes",
            ticker=trade['ticker'],
            yes_price=trade['yes_ask'],
            client_order_id=str(uuid.uuid4())
        )

        print(f"Order placed: {trade['ticker']} - {trade['contracts']} contracts @ {trade['yes_ask']}¢")

if __name__ == '__main__':
    urp_strategy_job()


