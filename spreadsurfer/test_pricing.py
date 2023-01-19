from .balance import BalanceWatcher
from .orders import scientific_price_calculation


def test_price_stabilize_min():
    print()
    low, high = scientific_price_calculation(18182.7, 18183.32, 0.62, 'min')
    print(low)
    print(high)
    print('-----')

    low, high = scientific_price_calculation(18181.69, 18182.15, 0.46, 'min')
    print(low)
    print(high)


def test_balance():
    b = BalanceWatcher()
    b.balance_usd = 100
    b.balance_btc = 0.00028581
    print(b.sum(18100))
