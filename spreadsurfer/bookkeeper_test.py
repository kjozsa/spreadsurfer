from loguru import logger

from spreadsurfer.bookkeeper import Bookkeeper

logger.level("bookkeeper", color='<light-green><bold>', no=37)

p1 = 22998
p2 = 23002


def save_orders(bookkeeper):
    bookkeeper.save_orders([{'wave_id': 'w1', 'price': p1, 'type': 'test limit', 'amount': -0.0014}, {'wave_id': 'w1', 'price': p2, 'type': 'test limit', 'amount': 0.0017}])


def test_add_remove():
    bookkeeper = Bookkeeper()
    save_orders(bookkeeper)
    assert len(bookkeeper.df_active) == 2

    bookkeeper._remove_orders_by_price(p1)
    assert len(bookkeeper.df_active) == 1

    bookkeeper._remove_orders_by_price(p2)
    bookkeeper.report()
    assert len(bookkeeper.df_active) == 0

    save_orders(bookkeeper)
    assert len(bookkeeper.df_active) == 2

    bookkeeper.remove_orders_by_wave('w1')
    bookkeeper.report()
    assert len(bookkeeper.df_active) == 0


def test_fulfill_order():
    bookkeeper = Bookkeeper()
    save_orders(bookkeeper)
    bookkeeper.fulfill_order(p1)
    assert len(bookkeeper.df_active) == 1

    bookkeeper.fulfill_order(p2)
    assert len(bookkeeper.df_active) == 0

    bookkeeper.fulfill_order(123456)
    assert len(bookkeeper.df_active) == 0

def test_cancel_then_fulfill():
    bookkeeper = Bookkeeper()
    save_orders(bookkeeper)
    assert len(bookkeeper.df_past) == 0
    bookkeeper.remove_orders_by_wave('w1')
    assert len(bookkeeper.df_past) == 2
    bookkeeper.fulfill_order(p1)
    assert len(bookkeeper.df_past) == 1
