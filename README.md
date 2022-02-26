# Creating your own database like joins using \{code\}

There are several tools which can bring data sets together and perform different types of joins namely:

* Inner Join
* Outer Join
* Left Join
* Right Join

Tools like Pandas, Spark, Informatica Power Center, DataStage, Talend etc does this job exceptionally well. Ofcourse apart form the databases themselves.

However, few years back I was working on to create an ETL tool which required to have join like features in this article I will try to discuss about the idea behind the joining algorithm that I applied to solve that problem.

Before we dive into the algorithm lets look at the data sets and try to understand the end goal.

##### Employees Data


| id | name | dept_id | salary |
| - | - | - | - |
| 1 | Justin | 101 | 7000000.0 |
| 2 | Jacob | 102 | 3000000.0 |
| 3 | Jolly | 103 | 2700000.0 |
| 4 | Jatin | 102 | 2400000.0 |
| 5 | Jacky | 101 | 5000000.0 |

##### Department Data


| dept_id | dept_name |
| - | - |
| 101 | Engineering |
| 102 | Data Science |
| 103 | IT Operations |

##### Expected Data after join operation


| id | name | dept_id | salary | dept_id_ | dept_name_ |
| - | - | - | - | - | - |
| 1 | Justin | 101 | 7000000.0 | 101 | Engineering |
| 2 | Jacob | 102 | 3000000.0 | 102 | Data Science |
| 3 | Jolly | 103 | 2700000.0 | 103 | IT Operations |
| 4 | Jatin | 102 | 2400000.0 | 102 | Data Science |
| 5 | Jacky | 101 | 5000000.0 | 101 | Engineering |

---

Now that we have a clear understanding of what we have to achieve I will start by using  the module and demonstrating these functionalities step by step.

Once we see how the module works we will deep dive into the implementation of the "joiner" method.

There are 2 implementations that we are going to use:

1. **DataSet** : We will be using DataSet wrapper class to create a wrapper on top of "namedtuples" for each row and create a data-set for the whole chunk of data.
2. **joiner** : This method will be our joining algorithm which will actually join the data.

##### Step 1 : Import DataSet, joiner from the module

```python
from core.join_operator import DataSet, joiner
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

The DataSet class has implementations for showing the count() and printing the dataset using show() methods which we will check later.

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
##### Step 4 : Join the 2 datasets and check the outcome

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
Joined data count : 
5

Joined data : 
id=1 || name='Justin'|| dept_id=101 || salary=7000.0 || dept_id_=101 || dept_name_='Engineering' 
id=5 || name='Jacky' || dept_id=101 || salary=5000.0 || dept_id_=101 || dept_name_='Engineering' 
id=2 || name='Jacob' || dept_id=102 || salary=3000.0 || dept_id_=102 || dept_name_='Data Science'
id=4 || name='Jatin' || dept_id=102 || salary=4500.0 || dept_id_=102 || dept_name_='Data Science'
id=3 || name='Jolly' || dept_id=103 || salary=3800.0 || dept_id_=103 || dept_name_='IT Operations'
