import os
import pandas as pd

import haven.db as db

os.environ['HAVEN_DATABASE'] = 'my_database'

data = pd.DataFrame([
    {'name': 'Alice', 'age': 25, 'city': 'New York'},
    {'name': 'Bob', 'age': 30, 'city': 'New York'},
    {'name': 'Charlie', 'age': 35, 'city': 'Boston'},
    {'name': 'David', 'age': 40, 'city': 'Boston'},
])

db.write_data(data, 'people', ['city'])
print(
    db.read_data('select * from people')
)

db.delete_data('people', [{'city': 'New York'}])
print(
    db.read_data('select * from people')
)

db.drop_table('people')