from loguru import logger

from spreadsurfer.bookkeeper import Bookkeeper

logger.level("bookkeeper", color='<light-green><bold>', no=37)


def test_add_remove():
    bookkeeper = Bookkeeper()
    bookkeeper.save_orders('w1', [{'id': 'o1'}, {'id': 'o2'}])
    assert len(bookkeeper.wave_orders) == 1
    assert len(bookkeeper.active_orders) == 2

    bookkeeper.remove_orders_by_id('o1')
    assert len(bookkeeper.wave_orders) == 1
    assert len(bookkeeper.active_orders) == 1

    bookkeeper.remove_orders_by_id('o2')
    bookkeeper.report()
    assert len(bookkeeper.wave_orders) == 0
    assert len(bookkeeper.active_orders) == 0

    bookkeeper.save_orders('w1', [{'id': 'o1'}, {'id': 'o2'}])
    assert len(bookkeeper.wave_orders) == 1
    assert len(bookkeeper.active_orders) == 2

    bookkeeper.remove_orders_by_wave('w1')
    bookkeeper.report()
    assert len(bookkeeper.wave_orders) == 0
    assert len(bookkeeper.active_orders) == 0

def test_fulfill_order():
    bookkeeper = Bookkeeper()
    bookkeeper.save_orders('w1', [{'id': 'o1'}, {'id': 'o2'}])
    bookkeeper.fulfill_order('o1')
    assert len(bookkeeper.wave_orders) == 1
    assert len(bookkeeper.active_orders) == 1

    bookkeeper.fulfill_order('o2')
    assert len(bookkeeper.wave_orders) == 0
    assert len(bookkeeper.active_orders) == 0
