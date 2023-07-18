# MongoDB CRUD Operations with Python

This project demonstrates how to connect MongoDB with Python using the pymongo dependency and perform console-based user interaction to try different CRUD operations on MongoDB, leveraging aggregation functions.

## Dependencies

* Python 3.6+
* pymongo
* tabulate
* pandas

## Instructions

1. Clone the repository.
2. Install the dependencies.
3. Run the `main.py` file.

## Data
As part of this project I am using  `Art gallery` Dataset, using 5 different `.csv` files to load dummy data. Below are data schema(s) details:

`ARTIST.csv`
| aID | name | birthDate | deathDate | commission | street | city | stateAb | zipcode |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

`ARTWORK.csv`
| aID | artID | title | creationDate | acquisitionDate | price | form |
| --- | --- | --- | --- | --- | --- | --- |

`BOUGHT.csv`
| artID | cID | saleDate |
| --- | --- | --- |

`CUSTOMER.csv`
| cID | name | street | city | stateAb | zipcode |
| --- | --- | --- | --- | --- | --- |

`STATE.csv`
| ﻿stateAb | stateName |
| --- | --- |
## Usage

The `main.py` file provides a console-based interface that allows you to perform CRUD operations on a MongoDB database. To use the interface, simply type the option number from the menu given below:

![image](https://github.com/KabraKrishna/PythonMongoDb_CRUD/assets/29306513/9b195668-50ad-4af7-b9dc-d40770cb2dd9)
