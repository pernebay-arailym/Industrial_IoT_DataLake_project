import pandas as pd

files = [
    "data/lineA.csv",
    "data/lineB.csv",
    "data/lineC.csv",
    "data/lineD.csv",
    "data/lineE.csv"
]

for file in files:
    df = pd.read_csv(file)

    print(f"\n===== {file} =====")
    print(df.shape)
    print(df.columns.tolist())
    print(df.dtypes)
    print(df.head())