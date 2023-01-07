import pandas as pd

df1 = pd.read_csv("a.csv")

df2 = pd.read_csv(
    "b.csv",
    sep=",",
    header=None
)

df3 = pd.merge(df1, df2, on="ID")

df3.to_csv("c.csv", index=False)
