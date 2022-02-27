# Creating your own database like join using \{code\}

There are several tools tp combine datasets and perform different types of joins namely:

* Inner
* Outer
* Left &
* Right

Tools like Pandas, Spark and other ETL tools does this job exceptionally well, ofcourse apart form the databases themselves.

However, few years back I was working on an ETL framework which required to have a join like features; in this article I will try to discuss about the idea behind the joining algorithm that I applied to solve that problem.

Before we dive into the algorithm lets look at the data sets and try to understand the end goal.

##### Employees Data


| id | name | dept_id | salary |
| - | - | - | - |
| 1 | Justin | 101 | 7000.0 |
| 2 | Jacob | 102 | 3000.0 |
| 3 | Jolly | 103 | 3800.0 |
| 4 | Jatin | 102 | 4500.0 |
| 5 | Jacky | 101 | 5000.0 |

##### Department Data


| dept_id | dept_name |
| - | - |
| 101 | Engineering |
| 102 | Data Science |
| 103 | IT Operations |

##### Expected Data after join operation


| id | name | dept_id | salary | dept_id_ | dept_name_ |
| - | - | - | - | - | - |
| 1 | Justin | 101 | 7000.0 | 101 | Engineering |
| 2 | Jacob | 102 | 3000.0 | 102 | Data Science |
| 3 | Jolly | 103 | 3800.0 | 103 | IT Operations |
| 4 | Jatin | 102 | 4500.0 | 102 | Data Science |
| 5 | Jacky | 101 | 5000.0 | 101 | Engineering |

---

With this understanding; I will now start by using the join module for demonstrating these functionalities step by step.

There are 2 implementations that we are going to use:

1. **DataSet** : We will be using DataSet wrapper class to create a wrapper on top of "namedtuples" for each row and create a data-set for the whole chunk of data.
2. **joiner** : This method will be our joining algorithm which will actually join the data.

##### Step 1 : Import DataSet, joiner from the module

```python
from core.join import DataSet, joiner
```

##### Step 2 : Prepare the data sets

```python
emp_header = ["id", "name", "dept_id", "salary"]

emp_data = [
            (1,"Justin", 101, 7000.00),
            (2,"Jacob", 102, 3000.00),
            (3,"Jolly", 103, 3800.00),
            (4,"Jatin", 102, 4500.00),
            (5,"Jacky", 101, 5000.00)
            ]

dept_header = ["dept_id", "dept_name"]

dept_data = [
            (101, "Engineering"),
            (102, "Data Science"),
            (103, "IT Operations")
            ]

emp_data_set = DataSet(emp_header, emp_data)

dept_data_set = DataSet(dept_header, dept_data)
```

##### Step 3 : Verify the datasets using count and show methods

DataSet has implementations for the helper methods like count() for checking the number of rows and printing the dataset using show() methods which we will check later.

```python
print("Employees count : ")
emp_data_set.count()

print("Employees data : ")
emp_data_set.show()

print("Department count :")
dept_data_set.count()

print("Department data :")
dept_data_set.show()
```

```
Employees count : 
5

Employees data : 
+ | id=1  || name='Justin'|| dept_id=101 || salary=7000.0 | +
+ | id=2  || name='Jacob' || dept_id=102 || salary=3000.0 | +
+ | id=3  || name='Jolly' || dept_id=103 || salary=3800.0 | +
+ | id=4  || name='Jatin' || dept_id=102 || salary=4500.0 | +
+ | id=5  || name='Jacky' || dept_id=101 || salary=5000.0 | +

Department count :
3

Department data :
+ | dept_id=101 || dept_name='Engineering' | +
+ | dept_id=102 || dept_name='Data Science' | +
+ | dept_id=103 || dept_name='IT Operations' | +
```

##### Step 4 : Join the 2 datasets and create a joined dataset

```python
# joining the datasets using the joiner method
joined_data_set = joiner(emp_data_set, dept_data_set, ["dept_id"])
```

##### Step 5 : Check the result for the joiner

```python
print("Joined data count : ")
joined_data_set.count()

print("Joined data : ")
joined_data_set.show()
```

```
Joined data count : 
5

Joined data : 
id=1 || name='Justin'|| dept_id=101 || salary=7000.0 || dept_id_=101 || dept_name_='Engineering' 
id=5 || name='Jacky' || dept_id=101 || salary=5000.0 || dept_id_=101 || dept_name_='Engineering' 
id=2 || name='Jacob' || dept_id=102 || salary=3000.0 || dept_id_=102 || dept_name_='Data Science'
id=4 || name='Jatin' || dept_id=102 || salary=4500.0 || dept_id_=102 || dept_name_='Data Science'
id=3 || name='Jolly' || dept_id=103 || salary=3800.0 || dept_id_=103 || dept_name_='IT Operations'
```

## Implementation

### Datasets

Datasets will be a collection of Row objects which is a wrapper on top of the [namedtuple ](https://stackoverflow.com/questions/2970608/what-are-named-tuples-in-python)from pythons collection module.

The idea is to add column information for each tuple in the list, we can further add types to these objects but for now we will only stick to column names and values.

##### Row Object

```python
class Row:
    def __init__(self, header:List[str], row:tuple=None)->None:
        '''
        A wrapper on top of namedtuple to create a Row object.
            :param header - List of column names
            :param row - A tuple of row [optional]
        '''
        self.header = header
        self.schema = namedtuple("Row", [col_name.lower() for col_name in header])
        if row:
            self.row = self.schema(*row)
        else:
            self.row = self.schema(*[None]*len(self.header))
  
```

> **Note:** Without row argument object will create Row with all values as None.
> Ex : Row(id=None, name=None)

We can go ahead and write helper method to print the row with the column information in a tabular fashion and call it show()

```python
    def __repr__(self) -> str:
        return self.show()

    def show(self) -> str:
        string = "+ "
        field_width = [len(col) for col in self.header]
        for col_name, col_val, width in zip(self.row._fields, self.row, field_width):
            string += "| {i}={j!r:<{k}} |".format(i=col_name, j=col_val, k=width)
        return string+" +"
```

##### Datasets

We will use __generate_data_set method to generate a list of Rows object using a simple list of tuple containing the data.

```python
class DataSet:
    def __init__(self, header:List[str], rows:List[Tuple[Union[int, str, date, datetime, float]]]) -> None:
        '''
        A wraper on top of Row class to create a complete dataset.
            :param header - List of column names
            :param rows - List of tuples of rows
        '''
        self.header = header
        self.rows = rows
        self.data_set = self.__generate_data_set()
  
    def __generate_data_set(self)->List[NamedTuple]:
        '''
        Row object factory; generates a list of row objects

        '''
        row_set = []
        for row in self.rows:
            if not len(self.header) == len(row):
                raise Exception(f"Columns mismatched expected : [{len(self.header)}] actual : [{len(row)}]")
            else:
                row_set.append(Row(self.header, row))
        else:
            return row_set
```

Once we have this implementation we can go ahead and add some helper methods like count() and show() just like spark.

These methods will help us check the count of rows and print the data in pandas / spark like tabular manner.

```python
    def size(self)->int:
        '''
        get the number of rows
        '''
        return len(self.data_set)
  
    def show(self)->None:
        '''
        Prints dataset
        '''
        for row in self.data_set:
            print(row.show())
        else:
            print()
  
    def count(self)->None:
        '''
        Prints count
        '''
        print(self.size())
        print()
```

##### Joiner (Algorithm)

Before we go ahead and implement last and the core part i.e the joiner; lets see a simple sql statement which joins the emp_data and dept_data on the basis of the dept_id as the key.

```sql
select e.*, d.* 
from emp_data e
inner join dept_data d
on e.dept_id = d.dept_id
```

Alright; now lets define the body of the method:

```python
def joiner(left_data_set:DataSet, right_data_set:DataSet, on:List[str])->DataSet:
```

Here are some key points:

- The method will take 2 datasets and the joining key and return after the operation return a joined DataSet.
- The fundamental data structure that we will use for joining the data sets will be a hash map or in our case a python dictionary. 

  The idea is to store left and right data with the common key.

```python
joiner_dict = { "key" : ([Left DataSet] , [Right DataSet]) }
```

- If both the sets have 1:1 relationship then for each key we will have just one tuple for both list of datasets; but if we have low cardinility on the key columns in either of the datasets then we will end up having more than one tuples (cross join)

  Lets recap on the data:

```python
emp_data = [
            (1,"Justin", 101, 7000.00),
            (2,"Jacob", 102, 3000.00),
            (3,"Jolly", 103, 3800.00),
            (4,"Jatin", 102, 4500.00),
            (5,"Jacky", 101, 5000.00)
            ]

dept_data = [
            (101, "Engineering"),
            (102, "Data Science"),
            (103, "IT Operations")
            ]
```

In emp_data the departments are repeating a simple representation of how the data will be stored per key basis in the dictionary will look like below.

```python
101 -> Justing, Jacky
102 -> Jacob, Jatin
103 -> Jolly
```

Below is how our data will be stored actually.

```python
{
'101': 
 ( 
   [
     Row(id=1, name='Justin', dept_id=101, salary=7000.0), 
     Row(id=5, name='Jacky', dept_id=101, salary=5000.0)
    ], 
    [
      Row(dept_id=101, dept_name='Engineering')
    ]
 ), 

'102': 
  (
    [
      Row(id=2, name='Jacob', dept_id=102, salary=3000.0), 
      Row(id=4, name='Jatin', dept_id=102, salary=4500.0)
    ], 
    [ 
      Row(dept_id=102, dept_name='Data Science')
    ]
  ), 
'103': 
   (
    [
      Row(id=3, name='Jolly', dept_id=103, salary=3800.0)
    ], 
    [
      Row(dept_id=103, dept_name='IT Operations')
    ]
   )
}
```

Lets discuss a few more things before writing the code:

- [X] **Point 1 :** There can be some records which have orphan keys i.e these are not present in one or the other table in those scenarios we need to eliminate those records (keep this thought we will come back to this later).
- [X] **Point 2 :** So esentially our goal is to match each left dataset to the right and vice versa which boils down to cartesian product within the key(cross join within the domain of that key).
- [X] **Point 3 :** While performing a cartesian product of an empty list with a Row object we will eliminate that entry all together; so that solves the problem we discussed earlier in point 1 (None or [] * Row = None)

Now that we have all these concrete ideas in our minds lets implement a cartesian product of left and right datasets and concatenate (+) these to get joined rows with information from both the tables.

> **Note:** To do the cartesian product we will use product method from itertools library from python.

```python
from itertools import product
def joiner(left_data_set:DataSet, right_data_set:DataSet, on:List[str])->DataSet:
    '''
    Performs inner join on datasets
        :param left_data_set - dataset with left data
        :param right_data_set - dataset with right data
        :param on - join on key 

    Returns -> joined DataSet 
    '''

    left_rows = left_data_set.data_set
    right_rows = right_data_set.data_set

    # creating the main data-structure to store the Rows for the same keys
    data_set = {}
    # Final list to contain all the Joined Row objects later to be converted to a data set
    joined_set = []
  
    # Form all the left rows by appending at the 0th index
    for left_row in left_rows:
        left_key = "".join([str(getattr(left_row.row, key)) for key in on])
        data_set.setdefault(left_key, ([],[]))[0].append(left_row.row)

    # Form all the right rows by appending at the 1st index
    for right_row in right_rows:
        right_key = "".join([str(getattr(right_row.row, key)) for key in on])
        data_set.setdefault(right_key, ([],[]))[1].append(right_row.row)

    # Finally perform the cartesian product on the left and right datasets and join them
    for ds_cols in data_set.values():
        for left_cols, right_cols in product(*ds_cols):
            joined_set.append(left_cols+right_cols)

    # Get the column names from the left and right data sets for creating the DataSet for the joined data
    result_headers = list(left_rows[0].row._fields) + [col+"_" for col in right_rows[0].row._fields]

    # Finally return the DataSet object using the joined data
    return DataSet(result_headers, joined_set)
```

Thats it; finally we are able to solve the problem of performing inner joins, which can support below operations:

1. Inner join
2. Joining on multiple keys
3. Representation of data in DataSets

However we can further modify this algorithm and create the implementation for the left/right and the outer joins also add type safety for the data by adding Column object.

If you want to improve this or add more functionalities to it then I ll encourage you to contribute using this github repo->[Github_dbjoin_anisriva](https://https://github.com/anisriva/dbJoins.git)

Finally, I'd like to conclude that this is just an experimental exercise and ofcourse there are better tools to perform this operation.
