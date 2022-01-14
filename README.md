# Creating a database for INPA's Genetic Resources Collection
> Developed by *Nelson Buainain & Maura Regina*

<p align="center"> <img src=/png/logo_inpa_crg.png width="250" alt="bird_pic" class="center"></p>

INPA's genetic resources collection holds over 75,000 genetic samples of birds :owl:, fishes :blowfish: and herps :frog: :snake: (Reptiles and Amphibians). It's one of the largest collections of genetic material of Amazonian vertebrates in South America, and constitutes an invaluable heritage for the Brazilian and Amazonian peoples. 

The database is currently managed in three excel files, separated by each of three large animal groups. Considering the importance of the material, it is time for the creation of a proper, safe, easy to manage and public-accessible database.

## Goals

* Process the spreadsheets's data, reviewing data entries, correcting errors, etc...

* Develop the conceptual, logical, and physical models for the data unifying all the material in a single database.

* Migrate the data to the new database.

* Make the database publicly accessible for everyone including the scientific and non-scientific communities.

## Tools

* Brmodelo to create the conceptual and logical models.

* PostgreSQL to create the physical model.

* Python to develop an ETL program in order to extract, process and load the database from the spreadsheets into the new SQL database.

* Psycopg adapter to connect to PostgreSQL database in the Python program. 
```sh
pip install psycopg2
```
```sh
pip install pandas
```
```sh
pip install numpy
```
```sh
pip install openpyxl xlsxwriter xlrd
```

## Current stage

We are currently at the third stage, developing the ETL pipeline to migrate the data to the new database. 

* The bird and herpetology database are already available for consultation in SQL format! The 'Solicita' table which records samples loans is not available yet.

## Database Model

* This is how our new database currently look like:

<p align="center"> <img src=/models/logic_model.png alt="model" class="center"></p>


