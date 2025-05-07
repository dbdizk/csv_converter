import pandas as pd

source = pd.read_csv("source.csv")

target = pd.DataFrame()
target["NewName"] = source["OldName"]
target["FormattedDate"] = pd.to_datetime(source["RawDate"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
target["Combined"] = source["First"] + " " + source["Last"]

target.to_csv("converted.csv", index=False)
