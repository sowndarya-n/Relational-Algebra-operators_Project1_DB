# Project- 1 - Database Management (CSCI 6370)

# Project Overview:
In the context of this project, we aim to finalize the implementation of five essential Relational Algebra (RA) operators that are partially implemented in the Table.java file.
1. Project Operator:
In relational databases, the project operator extracts chosen columns from a table, eliminates duplicates, and produces new tables and relations.
The 'project' method accepts a string of attributes, uses it to create a new table by choosing and extracting the specified attributes from the original table, and goes through each row in the input table to compile a new table that exclusively contains these chosen attributes.

2. Select Operator:
The select method is used to extract a portion of data from a table or dataset by applying particular conditions or criteria for the retrieval of particular information.
For project 1, we implement two select functions within a Table class. The initial function uses a Predicate function for filtering tuples using Boolean conditions, while the second function filters tuples based on the attribute-based selection.

3. Union Operator:
In relational databases, the union operator merges rows from multiple tables, eliminating duplicates to generate a unified result. 
The 'union' method within a Table class accepts a second table ('table2') as input and calculates the union of these two tables, resulting in a new table that contains all distinct rows from both tables, with duplicates removed. This output represents the merged data from the original tables.

4. Minus Operator:
In relational databases, the minus operator removes the rows from one table when they also appear in another table. The result comprises the unique rows of the first table, excluding any shared rows.
The 'minus' method within the Table class accepts a second table 'table2' as input and calculates the difference between the current table and 'table2'. It generates a new table containing only the rows present in the current table but not in 'table2', essentially subtracting the overlapping rows between the two tables.

5. Join Operator:
In relational databases, a join operator merges rows from multiple tables by matching them using a shared column or related data, resulting in a table that consolidates information from diverse tables. 

In this project 1, we will implement three types of joins.

 (i) Equi-join:
 In this project, one of the 'join' functions executes an equijoin operation between two tables using defined attributes. It scans through both tables' rows, checks if the specified attribute values match, and combines rows with matching attributes into a new result table. This outcome merges columns from both input tables when attribute values align.

 (ii) Theta-join:
 In this project, one of the 'join' functions executes a theta join operation between two tables using a specified condition. It parses the condition to extract the operator and column positions and then proceeds to compare rows between the two tables based on this condition. When rows match the condition, they are combined into a new result table, resulting in a merged table that combines columns from both input tables as defined by the condition.

 (iii) Natural join:
 In this project, one of the 'join' functions executes a natural join operation between two tables which identifies shared attributes between the tables, checks their values for matches, and combines rows with matching attributes into a result table. Depending on whether common attributes exist, it either generates a new table without duplicate attributes or combines the attribute lists, thus effectively merging the two tables through a natural join.


# Instructions for code execution:

1. Download or clone the repository.
2. Ensure that your local system has the Java Development Kit (JDK) installed and that the Java path is correctly configured in the Environmental Variables.
3. Verify that the folder structure includes the "store" folder for saving database files.
4. To compile the code, execute the following command in the terminal:
   javac MovieDB.java
5. If compilation encounters an error, an error message will be displayed in the command prompt.
6. Upon successful compilation, run the code using the following command:
   java MovieDB
7. Successful execution will create database files within the "store" folder. In case of any issues, an IOException will be raised..
8. In the command line, the result will be displayed, showcasing the implementation of the 5 Relational Algebra (RA) Operators through various tables.

# Team Members:
V H Sowndarya Nookala (Manager)
Rohith Lingala
Krishna Chaitanya Velagapudi
Bavesh Chowdary Kamma 
Subhasree Nadimpalli

# Conclusion:
We completed the project within the deadline by following the effective task segregation, testing, and evaluation of the operators by ensuring that they perform their intended functions accurately. 
