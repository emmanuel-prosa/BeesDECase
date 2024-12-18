import pandas as pd
import dfgui ### GUI found in https://github.com/bluenote10/PandasDataFrameGUI - I had to perform some fixes to this code but the version in utils seems to be working fine

path = "./virtualContainer/gold/"
file = "agg_view.parquet"

df = pd.read_parquet(path + file)
dfgui.show(df)

