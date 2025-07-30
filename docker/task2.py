import calculator as c
import pandas as pd

calc = c.Calculator()

# print the 3 times table in a dataframe.
df = calc.tt_as_dataframe(3, 12) # up to 3 x 12
print(df)