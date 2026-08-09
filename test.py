from massive_client import MassiveClient

client = MassiveClient()

prices = client.get_historical_prices(
    "NVDA",
    "2026-07-01",
    "2026-08-08",
)

print("Number of records:", len(prices))
print(prices[:2])
print(prices[-2:])