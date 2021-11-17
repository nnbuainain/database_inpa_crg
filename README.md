# Creating a database for INPA's Genetic Resources Collection
> Developed by *Nelson Buainain & Maura Regina*

<p align="center"> <img src=/png/logo_inpa_crg.png width="250" alt="bird_pic" class="center"></p>

INPA's genetic resources collection holds over 75,000 genetic samples of birds :owl:, fishes :blowfish: and herps :frog: :snake: (Reptiles and Amphibians). It's one of the largest collections of genetic material of Amazonian vertebrates in South America, and constitutes an invaluable heritage for Brazilian and Amazonian people. 

The database is currently managed in three excel files, separately, by each of three large animal groups. Considering the importance of the material, it is time for the creation of a proper, safe, easy to manage and public-accessible database.

## Goals

* Process the spreadsheets's data, reviewing data entries, correcting errors, etc...

* Develop the conceptual, logical, and physical models for the data unifying all the material in a single database.

* Migrate the data to the new database.

* Make the database publicly accessible for everyone including the scientific and non-scientific communities.

## Tools

* Brmodelo to create the conceptual and logical models.

* PostgreSQL to create the physical model.

* Python to process the database and program the migration of the data from the spreadsheets to the new database.

* Psycopg adapter to connect to PostgreSQL database in the Python program. 
```sh
pip install psycopg2
```
```sh
pip install pandas
```
```sh
pip install openpyxl xlsxwriter xlrd
```

## Current stage

We are currently at the second stage, developing the project to migrate the data to the new database.


