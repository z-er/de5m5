class Calculator:
    def sum(self, x, y, *numbers):
        i = x + y
        for n in numbers:
            i += n
        return i
    
    def subtract(self, x, y, *numbers):
        i = x - y
        for n in numbers:
            i -= n
        return i
    
    def product(self, x, y, *numbers):
        i = x * y
        for n in numbers:
            i *= n
        return i
    
    def tt_as_dataframe(self, x, y):
        import pandas as pd

        data = {
            'Number': range(1, (y+1)),
            'Times Table': [i * x for i in range(1, y + 1)]
        }
        df = pd.DataFrame(data)
        return df
