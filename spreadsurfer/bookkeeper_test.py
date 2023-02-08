from loguru import logger

from spreadsurfer.bookkeeper import Bookkeeper

logger.level("bookkeeper", color='<light-green><bold>', no=37)

o1 = 'NB-123'
o2 = 'FS-124'


def save_orders(bookkeeper):
    bookkeeper.save_orders(
        [
            {'timestamp_created_ms': 5, 'order_id': 123, 'client_order_id': 'c-123', 'wave_id': 'w1', 'price': 22914, 'near_far': 'test limit', 'amount': -0.0014, 'type': 'NB'},
            {'timestamp_created_ms': 35, 'order_id': 124, 'client_order_id': 'c-124', 'wave_id': 'w1', 'price': 22998, 'near_far': 'test limit', 'amount': 0.0017, 'type': 'FS'}
        ])


def test_add_remove():
    bookkeeper = Bookkeeper()
    save_orders(bookkeeper)
    assert len(bookkeeper.df) == 2

    save_orders(bookkeeper)
    assert len(bookkeeper.df) == 4

    bookkeeper.remove_orders_by_wave('w1')
    bookkeeper.report()
    assert len(bookkeeper.df) == 0


def test_fulfill_order():
    bookkeeper = Bookkeeper()
    save_orders(bookkeeper)
    bookkeeper.fulfill_order(o1)
    logger.info('df: \n{}', bookkeeper.df)
    logger.info('len {}', len(bookkeeper.df))
    # assert len(bookkeeper.df) == 1
    #
    # bookkeeper.fulfill_order(o2)
    # assert len(bookkeeper.df) == 0
    #
    # bookkeeper.fulfill_order(123456)
    # assert len(bookkeeper.df) == 0


def test_sweep_past_orders():
    bookkeeper = Bookkeeper()

    c = bookkeeper.past_orders_to_cancel()
    assert len(c) == 0

    save_orders(bookkeeper)
    assert len(bookkeeper.df) == 2
    c = bookkeeper.past_orders_to_cancel()
    assert len(c) == 2
    assert len(bookkeeper.df) == 0


# def test_orders_to_cancel():
#     bookkeeper = Bookkeeper()
#     save_orders(bookkeeper)
#     assert len(bookkeeper.df) == 0
#
#     bookkeeper.df = pd.DataFrame([{'timestamp_created_ms': 10_000, 'wave_id': 'w2', 'price': p1, 'type': 'test limit', 'amount': -0.0014}])
#     # bookkeeper.df.loc[bookkeeper.df.wave_id == 'w2', 'timestamp_created_ms'] = 1500
#     list = bookkeeper.orders_to_cancel('w1')
#     assert len(list) == 3
#
