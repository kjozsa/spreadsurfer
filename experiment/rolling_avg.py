import pandas as pd

samples = [
    {'ms': 1673361751245, 'rate': 1},
    {'ms': 1673361751265, 'rate': 2},
    {'ms': 1673361751267, 'rate': 3},
]

samples2 = [
    {'ms': 1673361751273, 'rate': 10},
    {'ms': 1673361751277, 'rate': 10},
    {'ms': 1673361751279, 'rate': 10.02},
]

df = pd.DataFrame(samples)
df2 = pd.DataFrame(samples2)

df = pd.concat([df, df2], ignore_index=True)

print(df.shape)
rolling = df.rolling(window=2).mean()
print(rolling)

print(df['rate'].mean())
print('===============')
print(df.tail(2).head(1))
