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