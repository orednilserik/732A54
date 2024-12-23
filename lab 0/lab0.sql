/*
Lab 1 report Mikael Montén mikmo937, Johannes Hedström johed883
*/

/* All non code should be within SQL-comments like this */ 


/*
Drop all user created tables that have been created when solving the lab
*/

DROP TABLE IF EXISTS custom_table CASCADE;


/* Have the source scripts in the file so it is easy to recreate!*/

SOURCE company_schema.sql;
SOURCE company_data.sql;

/*
Question 1: List all employees, i.e. all tuples in the jbemployee relation.
*/

select * from jbemployee;

/*
    +------+--------------------+--------+---------+-----------+-----------+
	| id   | name               | salary | manager | birthyear | startyear |
	+------+--------------------+--------+---------+-----------+-----------+
	|   10 | Ross, Stanley      |  15908 |     199 |      1927 |      1945 |
	|   11 | Ross, Stuart       |  12067 |    NULL |      1931 |      1932 |
	|   13 | Edwards, Peter     |   9000 |     199 |      1928 |      1958 |
	|   26 | Thompson, Bob      |  13000 |     199 |      1930 |      1970 |
	|   32 | Smythe, Carol      |   9050 |     199 |      1929 |      1967 |
	|   33 | Hayes, Evelyn      |  10100 |     199 |      1931 |      1963 |
	|   35 | Evans, Michael     |   5000 |      32 |      1952 |      1974 |
	|   37 | Raveen, Lemont     |  11985 |      26 |      1950 |      1974 |
	|   55 | James, Mary        |  12000 |     199 |      1920 |      1969 |
	|   98 | Williams, Judy     |   9000 |     199 |      1935 |      1969 |
	|  129 | Thomas, Tom        |  10000 |     199 |      1941 |      1962 |
	|  157 | Jones, Tim         |  12000 |     199 |      1940 |      1960 |
	|  199 | Bullock, J.D.      |  27000 |    NULL |      1920 |      1920 |
	|  215 | Collins, Joanne    |   7000 |      10 |      1950 |      1971 |
	|  430 | Brunet, Paul C.    |  17674 |     129 |      1938 |      1959 |
	|  843 | Schmidt, Herman    |  11204 |      26 |      1936 |      1956 |
	|  994 | Iwano, Masahiro    |  15641 |     129 |      1944 |      1970 |
	| 1110 | Smith, Paul        |   6000 |      33 |      1952 |      1973 |
	| 1330 | Onstad, Richard    |   8779 |      13 |      1952 |      1971 |
	| 1523 | Zugnoni, Arthur A. |  19868 |     129 |      1928 |      1949 |
	| 1639 | Choy, Wanda        |  11160 |      55 |      1947 |      1970 |
	| 2398 | Wallace, Maggie J. |   7880 |      26 |      1940 |      1959 |
	| 4901 | Bailey, Chas M.    |   8377 |      32 |      1956 |      1975 |
	| 5119 | Bono, Sonny        |  13621 |      55 |      1939 |      1963 |
	| 5219 | Schwarz, Jason B.  |  13374 |      33 |      1944 |      1959 |
	+------+--------------------+--------+---------+-----------+-----------+

    The query selects all (using *) tuples from the jbemployee table.
*/

/*
Question 2: List the name of all departments in alphabetical order. Note: by “name” we mean the name attribute for all tuples in the jbdept relation.
*/

select * from jbdept order by name;

/*
    +----+------------------+-------+-------+---------+
	| id | name             | store | floor | manager |
	+----+------------------+-------+-------+---------+
	|  1 | Bargain          |     5 |     0 |      37 |
	| 35 | Book             |     5 |     1 |      55 |
	| 10 | Candy            |     5 |     1 |      13 |
	| 73 | Children's       |     5 |     1 |      10 |
	| 43 | Children's       |     8 |     2 |      32 |
	| 19 | Furniture        |     7 |     4 |      26 |
	| 99 | Giftwrap         |     5 |     1 |      98 |
	| 14 | Jewelry          |     8 |     1 |      33 |
	| 47 | Junior Miss      |     7 |     2 |     129 |
	| 65 | Junior's         |     7 |     3 |      37 |
	| 26 | Linens           |     7 |     3 |     157 |
	| 20 | Major Appliances |     7 |     4 |      26 |
	| 58 | Men's            |     7 |     2 |     129 |
	| 60 | Sportswear       |     5 |     1 |      10 |
	| 34 | Stationary       |     5 |     1 |      33 |
	| 49 | Toys             |     8 |     2 |      35 |
	| 63 | Women's          |     7 |     3 |      32 |
	| 70 | Women's          |     5 |     1 |      10 |
	| 28 | Women's          |     8 |     2 |      32 |
	+----+------------------+-------+-------+---------+
	
	The query choose all tuples from jdbept table and orders them alphabetically using the name attribute.
*/

/*
Question 3: What parts are not in store, i.e. qoh = 0? (qoh = Quantity On Hand)
*/

select name from jbparts where qoh = 0;

/*
    +-------------------+
	| name              |
	+-------------------+
	| card reader       |
	| card punch        |
	| paper tape reader |
	| paper tape punch  |
	+-------------------+
	
	From all available rows in the jbparts table, only names of tuples with qoh = 0 is selected. 
    The parts selected are card readers, card punch, paper tape reader and paper tape punch.
*/


/*
Question 4: Which employees have a salary between 9000 (included) and 10000 (included)?
*/

select name from jbemployee where salary >= 9000 and salary <= 10000;

/*
    +----------------+
	| name           |
	+----------------+
	| Edwards, Peter |
	| Smythe, Carol  |
	| Williams, Judy |
	| Thomas, Tom    |
	+----------------+
	
	Name of employees that has a salary in range [9000, 10000] is selected. Four employees has this salary range.
*/


/*
Question 5: What was the age of each employee when they started working (startyear)?
*/

select name, (startyear - birthyear) as age from jbemployee;

/*
    +--------------------+------+
	| name               | age  |
	+--------------------+------+
	| Ross, Stanley      |   18 |
	| Ross, Stuart       |    1 |
	| Edwards, Peter     |   30 |
	| Thompson, Bob      |   40 |
	| Smythe, Carol      |   38 |
	| Hayes, Evelyn      |   32 |
	| Evans, Michael     |   22 |
	| Raveen, Lemont     |   24 |
	| James, Mary        |   49 |
	| Williams, Judy     |   34 |
	| Thomas, Tom        |   21 |
	| Jones, Tim         |   20 |
	| Bullock, J.D.      |    0 |
	| Collins, Joanne    |   21 |
	| Brunet, Paul C.    |   21 |
	| Schmidt, Herman    |   20 |
	| Iwano, Masahiro    |   26 |
	| Smith, Paul        |   21 |
	| Onstad, Richard    |   19 |
	| Zugnoni, Arthur A. |   21 |
	| Choy, Wanda        |   23 |
	| Wallace, Maggie J. |   19 |
	| Bailey, Chas M.    |   19 |
	| Bono, Sonny        |   24 |
	| Schwarz, Jason B.  |   15 |
	+--------------------+------+

	Selects the name, birthyear and startyear columns abd creates a new temporary attribute 
    called startage which is the difference between the startyear and birthyear.
*/


/*
Question 6: Which employees have a last name ending with “son”?
*/

select name from jbemployee where substring_index(name, ',', 1) like '%son%';

/*
    +---------------+
	| name          |
	+---------------+
	| Thompson, Bob |
	+---------------+

	Splits the name attribute according to the comma and selects names where the first element before the split contains "son".
*/


/*
Question 7: Which items (note items, not parts) have been delivered by a supplier called Fisher-Price? Formulate this query using a subquery in the where-clause.
*/

select name from jbitem where supplier = (select id from jbsupplier where name = "Fisher-Price");

/*
    +-----------------+
	| name            |
	+-----------------+
	| Maze            |
	| The 'Feel' Book |
	| Squeeze Ball    |
	+-----------------+
	
	Selects the id from jdsupplier where name is "Fisher-Price", matches that id to the id in jditem and selects the name attribute.
*/


/*
Question 8: Which items (note items, not parts) have been delivered by a supplier called Fisher-Price? Formulate without a subquery.
*/

select jbitem.name from jbitem, jbsupplier where jbitem.supplier=jbsupplier.id and jbsupplier.name='Fisher-Price';

/*
    +-----------------+
	| name            |
	+-----------------+
	| Maze            |
	| The 'Feel' Book |
	| Squeeze Ball    |
	+-----------------+

	Selects the item name and shows the names for where supplier from item and id from supplier matches has correct supplier name.
*/


/*
Question 9: Show all cities that have suppliers located in them. Formulate this query using a subquery in the where-clause.
*/

select name from jbcity where id IN (SELECT city from jbsupplier);

/*
    +----------------+
	| name           |
	+----------------+
	| Amherst        |
	| Boston         |
	| New York       |
	| White Plains   |
	| Hickville      |
	| Atlanta        |
	| Madison        |
	| Paxton         |
	| Dallas         |
	| Denver         |
	| Salt Lake City |
	| Los Angeles    |
	| San Diego      |
	| San Francisco  |
	| Seattle        |
	+----------------+

	Select name from city database and shows cities where city id exists as city name in supplier
*/


/*
Question 10: What is the name and color of the parts that are heavier than a card reader? 
             Formulate this query using a subquery in the where-clause. (The SQL query must not contain the weight as a constant.)
*/

select name, color from jbparts where weight > (Select weight from jbparts where name = 'card reader');

/*
    +--------------+--------+
	| name         | color  |
	+--------------+--------+
	| disk drive   | black  |
	| tape drive   | black  |
	| line printer | yellow |
	| card punch   | gray   |
	+--------------+--------+
	
	Select names and color from parts db and conditionals weight on where weight is bigger than what it is for the card reader column
*/


/*
Question 11: Formulate the same query as above, but without a subquery. (The query must not contain the weight as a constant.)
*/

select one.name, one.color from jbparts as one, jbparts as two where one.weight > two.weight AND two.name = 'card reader';

/*
    +--------------+--------+
	| name         | color  |
	+--------------+--------+
	| disk drive   | black  |
	| tape drive   | black  |
	| line printer | yellow |
	| card punch   | gray   |
	+--------------+--------+

	Select name and color from parts db as one to show, conditions on parts db as two 
    and matches so the weights shown is larger than card reader weight in two
*/


/*
Question 12: What is the average weight of black parts?
*/

mysql> select avg(weight) from jbparts where color = "black";


/*
    +-------------+
	| AVG(weight) |
	+-------------+
	|    347.2500 |
	+-------------+

	Select the average weight from parts db where color is black
*/


/*
Question 13: What is the total weight of all parts that each supplier in Massachusetts (“Mass”) has delivered? 
             Retrieve the name and the total weight for each of these suppliers. 
             Do not forget to take the quantity of delivered parts into account. Note that one row should be returned for each supplier.
*/

    select sum(jbparts.weight * jbsupply.quan) as total_weight, jbsupplier.name
	from jbparts, jbsupplier, jbcity, jbsupply
	where jbparts.id = jbsupply.part
	and jbsupply.supplier = jbsupplier.id
	and jbsupplier.city = jbcity.id
	and jbcity.state = 'mass'
	group by jbsupplier.name;

/*
    +--------------+--------------+
    | total_weight | name         |
    +--------------+--------------+
    |         3120 | DEC          |
    |      1135000 | Fisher-Price |
    +--------------+--------------+

    Selects the sum of weight and quantity as total weight, and supplier name to be shown from the 4 databases that are relevant for the information. 
    Joins the databases by parts with supply, supply with supplier, supplier with city and finds matches for the state in city. Lastly groups by name.
*/


/*
Question 14: Create a new relation (a table), with the same attributes as the table items using the CREATE TABLE syntax where
you define every attribute explicitly (i.e. not as a copy of another table). Then fill the table with all items that cost less 
than the average price for items. Remember to define primary and foreign keys in your table!
*/

    create table newitem (
		id int primary key,
		name varchar(255),
		dept int,
		price int,
		qoh int,
		supplier int,
		foreign key (dept) references jbdept(id),
		foreign key (supplier) references jbsupplier(id)
		);

	insert into newitem (id, name, dept, price, qoh, supplier)
	select id, name, dept, price, qoh, supplier from jbitem
	where price < (select avg(price) from jbitem);

/*
Create the table according to all the ids from the item db with apt definitions. 
Define id as primary key, dept and supplier as foreign keys which references ids in other databases. 
Insert into each new item db column, each column of the old item db where the individual price is lower than the average.
*/

