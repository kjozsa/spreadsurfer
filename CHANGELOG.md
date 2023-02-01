# v1.1
* implement max_delta_ms_to_create_order
* implement buy/sell order change
* do not create new orders on wave change direction

# v1.2
- aim just above/below min_price/max_price
- balance watching should print total balance in USD
- balance watching should stop complete operation on configurable low limit

# v1.3
- websocket order placement and cancel

# v1.4
- collect trade waves data for ML analysis

# v1.5
- ML predicting with catboost
- restabilizing changing waves
- datacollect on changing waves

# v1.6
- bookkeeper tracking fulfilled orders

# v1.7
- collect last N waves' prices

# v1.8
- use recv_window for far order
- cancel only near order

# v1.9
- stream orderbook, calculate gasp
- use gasp for prediction
- models trained at score 161 (MAPE) and 291 (RMSE)

