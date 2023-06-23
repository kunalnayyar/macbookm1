from typing import List
#from Ipython.display import display
from tabulate import tabulate
import pandas as pd
from collections import defaultdict

df = pd.read_csv('restaurant_orders.csv')
print(df.columns)
#print(tabulate(df.iloc[:5,0:5],tablefmt = "grid",headers = "keys"))

print(df.groupby(['Item Name']).agg(Total_Orders = pd.NamedAgg(column='Quantity',aggfunc = 'sum')).sort_values('Total_Orders',ascending = False)[:10])