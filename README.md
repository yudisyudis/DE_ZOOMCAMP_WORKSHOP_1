# DE_ZOOMCAMP_WORKSHOP_1

## Section 1. Use a Generator
### Question 1. What is the sum of the outputs of the generator for limit = 5?

```
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

limit = 5
generator = square_root_generator(limit)

val = 0
for sqrt_value in generator:
    val += sqrt_value
    print(val)
```
The answer is 8.382332347441762

### Question 2. What is the 13th number yielded

```
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

limit = 13
generator = square_root_generator(limit)


for sqrt_value in generator:
    print(sqrt_value)
    
```
The answer is 3.605551275463989

## Section 2 Append a generator to a table with existing data
### Question 3. After correctly appending the data, calculate the sum of all ages of people.

load the data from generator to DB with this code
```
import dlt

# connection to load data.
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='people_data')


# load the generator:
info = generators_pipeline.run(people_1(),
										table_name="people",
										write_disposition="replace")

print(info)

# load the seconde generator
info = generators_pipeline.run(people_2(),
										table_name="people",
										write_disposition="append")

print(info)
```

doing an SQL statement to sum the age with this code
```
# show all the data

import duckdb

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))


print("\n\n\n people table below:")

peoples = conn.sql("SELECT * FROM people").df()
display(peoples)

print("\n\n\n sum of age:")

age = conn.sql("SELECT SUM(age) FROM people").df()
display(age)

```
The answer is 253

## Section 3. Merge a generator
### Question 4. Merge the 2 generators using the ID column. Calculate the sum of ages of all the people loaded as described above. 

Change the load data code become this one
```
import dlt

# connection to load data.
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='people_data')


# load the generator:
info = generators_pipeline.run(people_1(),
										table_name="people_table",
										write_disposition="replace")

print(info)

# load the second generator with merge.
info = generators_pipeline.run(people_2(),
										table_name="people_table",
										write_disposition="merge",
                                        primary_key="id")

print(info)
```

and do the a query to sum the age with the same code like before:
```
# show all the data

import duckdb

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))


print("\n\n\n people table below:")

peoples = conn.sql("SELECT * FROM people_table").df()
display(peoples)

#the query

print("\n\n\n sum of age below:")

age = conn.sql("SELECT SUM(age) FROM people_table").df()
display(age)

# As you can see, the same data was loaded in both cases.
```
the answer is 266
