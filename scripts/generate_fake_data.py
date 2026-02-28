import random
import psycopg2
from faker import Faker
from datetime import date, timedelta

fake = Faker()

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="fintrade_dw",
    user="fintrade",
    password="fintrade123"
)
cur = conn.cursor()

# =============================================
# STOCKS
# =============================================
stocks = [
    ("AAPL",  "Apple Inc.",                   "Technology",             "NASDAQ"),
    ("MSFT",  "Microsoft Corporation",         "Technology",             "NASDAQ"),
    ("GOOGL", "Alphabet Inc.",                 "Technology",             "NASDAQ"),
    ("AMZN",  "Amazon.com Inc.",               "Consumer Discretionary", "NASDAQ"),
    ("META",  "Meta Platforms Inc.",           "Technology",             "NASDAQ"),
    ("TSLA",  "Tesla Inc.",                    "Consumer Discretionary", "NASDAQ"),
    ("NVDA",  "NVIDIA Corporation",            "Technology",             "NASDAQ"),
    ("JPM",   "JPMorgan Chase & Co.",          "Finance",                "NYSE"),
    ("BAC",   "Bank of America Corporation",   "Finance",                "NYSE"),
    ("JNJ",   "Johnson & Johnson",             "Healthcare",             "NYSE"),
    ("PFE",   "Pfizer Inc.",                   "Healthcare",             "NYSE"),
    ("XOM",   "Exxon Mobil Corporation",       "Energy",                 "NYSE"),
    ("CVX",   "Chevron Corporation",           "Energy",                 "NYSE"),
    ("WMT",   "Walmart Inc.",                  "Consumer Staples",       "NYSE"),
    ("KO",    "The Coca-Cola Company",         "Consumer Staples",       "NYSE"),
]

for stock in stocks:
    cur.execute("""
        INSERT INTO raw.stocks (symbol, company_name, sector, exchange)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (symbol) DO NOTHING
    """, stock)
print(f"[+] Inserted {len(stocks)} stocks")

# =============================================
# ACCOUNTS
# =============================================
accounts = []
for i in range(50):
    account_id   = f"ACC{str(i+1).zfill(5)}"
    user_name    = fake.name()
    email        = fake.email()
    account_type = random.choice(["individual", "institutional"])
    balance      = round(random.uniform(10000, 1000000), 2)
    accounts.append((account_id, user_name, email, account_type, balance))
    cur.execute("""
        INSERT INTO raw.accounts (account_id, user_name, email, account_type, balance)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (account_id) DO NOTHING
    """, (account_id, user_name, email, account_type, balance))
print(f"[+] Inserted {len(accounts)} accounts")

# =============================================
# ORDERS & TRADES
# =============================================
stock_symbols = [s[0] for s in stocks]
account_ids   = [a[0] for a in accounts]

trade_count = 0
for i in range(500):
    order_id   = f"ORD{str(i+1).zfill(6)}"
    account_id = random.choice(account_ids)
    symbol     = random.choice(stock_symbols)
    order_type = random.choice(["buy", "sell"])
    quantity   = random.randint(1, 1000)
    price      = round(random.uniform(10, 500), 2)
    status     = random.choice(["filled", "cancelled", "partial"])
    created_at = fake.date_time_between(start_date="-1y", end_date="now")

    cur.execute("""
        INSERT INTO raw.orders (order_id, account_id, symbol, order_type, quantity, price, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING
    """, (order_id, account_id, symbol, order_type, quantity, price, status, created_at))

    if status in ["filled", "partial"]:
        trade_id       = f"TRD{str(i+1).zfill(6)}"
        trade_quantity = quantity if status == "filled" else random.randint(1, quantity)
        total_amount   = round(trade_quantity * price, 2)

        cur.execute("""
            INSERT INTO raw.trades (trade_id, order_id, account_id, symbol, trade_type, quantity, price, total_amount, traded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_id) DO NOTHING
        """, (trade_id, order_id, account_id, symbol, order_type, trade_quantity, price, total_amount, created_at))
        trade_count += 1

print(f"[+] Inserted 500 orders, {trade_count} trades")

# =============================================
# DAILY PRICES (OHLCV)
# =============================================
base_prices = {
    "AAPL": 150, "MSFT": 300, "GOOGL": 140, "AMZN": 180, "META": 350,
    "TSLA": 250, "NVDA": 500, "JPM":   150, "BAC":   35, "JNJ":  160,
    "PFE":   30, "XOM":  110, "CVX":   150, "WMT":  160, "KO":    60,
}

end_date   = date.today()
start_date = end_date - timedelta(days=365)

for symbol, base_price in base_prices.items():
    current_price = base_price
    current_date  = start_date
    while current_date <= end_date:
        if current_date.weekday() < 5:  # skip weekends
            change      = random.uniform(-0.03, 0.03)
            open_price  = round(current_price, 2)
            close_price = round(current_price * (1 + change), 2)
            high_price  = round(max(open_price, close_price) * random.uniform(1.0, 1.02), 2)
            low_price   = round(min(open_price, close_price) * random.uniform(0.98, 1.0), 2)
            volume      = random.randint(1_000_000, 50_000_000)

            cur.execute("""
                INSERT INTO raw.daily_prices
                    (symbol, price_date, open_price, high_price, low_price, close_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, price_date) DO NOTHING
            """, (symbol, current_date, open_price, high_price, low_price, close_price, volume))

            current_price = close_price
        current_date += timedelta(days=1)

print("[+] Inserted daily prices (365 days x 15 stocks)")

conn.commit()
cur.close()
conn.close()
print("\n[done] All fake data inserted successfully!")