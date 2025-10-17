import pandas as pd
import random
import string

# Generate 4-letter random codes
random_codes = [''.join(random.choices(string.ascii_uppercase, k=4)) for _ in range(10)]
df = pd.DataFrame({'codes': random_codes})
print(df)