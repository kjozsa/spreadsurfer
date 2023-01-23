from loguru import logger

from spreadsurfer.bookkeeper import Bookkeeper

logger.level("bookkeeper", color='<light-green><bold>', no=37)


def test_add_remove():
    bookkeeper = Bookkeeper()
    bookkeeper.save_orders('w1', [{'price': 'p1'}, {'price': 'p2'}])
    assert len(bookkeeper.wave_orders) == 1
    assert len(bookkeeper.active_orders_by_price) == 2

    bookkeeper._remove_orders_by_price('p1')
    assert len(bookkeeper.wave_orders) == 1
    assert len(bookkeeper.active_orders_by_price) == 1

    bookkeeper._remove_orders_by_price('p2')
    bookkeeper.report()
    assert len(bookkeeper.wave_orders) == 0
    assert len(bookkeeper.active_orders_by_price) == 0

    bookkeeper.save_orders('w1', [{'price': 'p1'}, {'price': 'p2'}])
    assert len(bookkeeper.wave_orders) == 1
    assert len(bookkeeper.active_orders_by_price) == 2

    bookkeeper.remove_orders_by_wave('w1')
    bookkeeper.report()
    assert len(bookkeeper.wave_orders) == 0
    assert len(bookkeeper.active_orders_by_price) == 0


def test_fulfill_order():
    bookkeeper = Bookkeeper()
    bookkeeper.save_orders('w1', [{'price': 'p1'}, {'price': 'p2'}])
    bookkeeper.fulfill_order('p1')
    assert len(bookkeeper.wave_orders) == 1
    assert len(bookkeeper.active_orders_by_price) == 1

    bookkeeper.fulfill_order('p2')
    assert len(bookkeeper.wave_orders) == 0
    assert len(bookkeeper.active_orders_by_price) == 0
