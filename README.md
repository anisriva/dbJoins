# Create your own database joins using \{code\}

There are several tools which can bring data sets together and perform different types of joins namely:

* Inner Join
* Outer Join
* Left Join
* Right Join

Tools like Pandas, Spark, Informatica Power Center, DataStage, Talend etc does this job exceptionally well. Ofcourse apart form the databases themselves.

However, few years back I was working on to create an ETL tool which required to have join like features in this article I will try to discuss about the idea behind the joining algorithm that I applied to solve that problem.

Before we dive staright into the algorithm lets look at the data sets.

#### Employees Data

<script src="https://gist.github.com/anisriva/505a584d59d26d3b7e93a4ecd944af10.js"></script>



#### Department Data

<script src="https://gist.github.com/anisriva/9cd6ce1f4eea56867e9d0eb2a1468203.js"></script>
