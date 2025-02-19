import pandas as pd
import os

raw_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/raw"))

data = {
    file: pd.read_csv(os.path.join(raw_path, file))
    for file in os.listdir(raw_path) if file.endswith(".csv")
}

print(data.keys())

