# eszrevetelek

hullamok hossza ~500ms
ha min konstans -> price no a hullam vegeig
ha max konstans -> price csokken a hullam vegeig
spread folyamatosan szelesedik a hullam vegeig

# terminologia
wave == 1 hullam
frame == 1 hullam 1 sora (annyi trade ami egyszerre jott websocketrol)


# balance calculations
100 btc
100 usd
usd rate == 1.0
buy amount = 0.001 + (0.5 * 0.001)  == 0.0015
sell amount = 0.001 + (0.5 * 0.001) == 0.0015

sum 200

50 btc
150 usd
usd percentage == 0.7
buy amount = 0.001 + 0.7 * 0.001  == 0.0017
sell amount = 0.001 + 0.3 * 0.001 == 0.0013


150 btc
50 usd
usd rate == 0.3

# buy/sell
on wave MIN, price is raising -> noone wants to sell / everyone is buying
sell order will succeed, buy order needs to be MARKET

on wave MAX, price is dropping -> noone wants to buy / everyone is selling
buy order will succeed, sell order will fail (create sell MARKET order instead?)


# other servers
We've added more API clusters:
https://api1.binance.com/api/v3/ping
https://api2.binance.com/api/v3/ping
https://api3.binance.com/api/v3/ping

in addition to https://api.binance.com/api/v3/ping
