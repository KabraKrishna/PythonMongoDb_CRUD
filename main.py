import constants
import pandas
from tabulate import tabulate
from pymongo import MongoClient


# Function to get database instance
def init_db():
    return MongoClient(constants.DB_URL)[constants.DB_NAME]


# Function to insert ARTIST data into database
def insert_artist_records(artist_df, artwork_df, state_df):
    artist_list = []

    for index, row in artist_df.iterrows():
        artist_document = {
            'aID': row['aID'],
            'artistName': row['name'],
            'birthDate': row['birthDate'],
            'state': state_df.loc[state_df['stateAb'].str.lower() == row['stateAb'].lower(), 'stateName'].item()
        }
        artist_list.append(artist_document)

    artist_collection = init_db()[constants.ARTIST_COLLECTION]

    artist_collection.insert_many(artist_list)

    for document in artist_collection.find():
        artwork = []
        for index, row in artwork_df.iterrows():
            if document['aID'] == row['aID']:
                artwork_document = {
                    'artID': row['artID'],
                    'title': row['title'],
                    'price': row['price'],
                    'form': row['form']
                }
                artwork.append(artwork_document)

        artist_collection.update_one({'aID': document['aID']}, {"$set": {'artwork': artwork}})

    print(f'{artist_collection.count_documents({})} documents inserted into artist collection')


# Function to insert ARTWORK data into database
def insert_artwork_records(artwork_df, artist_df, state_df, bought_df, customer_df):
    artwork_list = []

    for index, row in artwork_df.iterrows():
        artwork_document = {
            'artID': row['artID'],
            'title': row['title'],
            'creationDate': row['creationDate'],
            'price': row['price'],
            'form': row['form'],
            'artistName': artist_df.loc[artist_df['aID'] == row['aID'], 'name'].item(),
        }
        artwork_list.append(artwork_document)

    artwork_collection = init_db()[constants.ARTWORK_COLLECTION]

    artwork_collection.insert_many(artwork_list)

    for document in artwork_collection.find():
        customer = []
        customer_id_list = bought_df.loc[bought_df['artID'] == document['artID'], 'cID']
        for cid in customer_id_list:
            customer_tuple = customer_df.loc[customer_df['cID'] == cid]
            customer_document = {
                'customerName': customer_tuple.iloc[0]['name'],
                'city': customer_tuple.iloc[0]['city'],
                'state': state_df.loc[
                    state_df['stateAb'].str.lower() == customer_tuple.iloc[0]['stateAb'].lower(), 'stateName'].item(),
            }

            customer.append(customer_document)

        artwork_collection.update_one({'artID': document['artID']}, {'$set': {'customer': customer}})

    print(f'{artwork_collection.count_documents({})} documents inserted into artwork collection')


# Function to drop ARTIST collection
def drop_artist(isPrint=False):
    init_db()[constants.ARTIST_COLLECTION].drop()
    if isPrint:
        print('Artist collection was dropped successfully!')


# Function to drop ARTWORK collection
def drop_artwork(isPrint=False):
    init_db()[constants.ARTWORK_COLLECTION].drop()
    if isPrint:
        print('Artwork collection was dropped successfully!')


# Function to print output to console
def print_table(value_list):
    print(tabulate(value_list, headers='keys', tablefmt='psql'))


# Query 1: Get Artist with most expensive art
def get_query_1():
    artist_collection = init_db()[constants.ARTIST_COLLECTION]

    result = list(artist_collection.aggregate([
        {"$unwind": "$artwork"},
        {
            "$group": {
                "_id": "$artistName",
                "max_price": {
                    "$max": "$artwork.price"
                }
            }
        },
        {"$sort": {"max_price": -1}},
        {"$limit": 1},
        {
            "$project": {
                "_id": 0,
                "AtristName": "$_id",
                "ArtwrokPrice": "$max_price"
            }
        }
    ]))

    print_table(result)

# Query 2: Get Artwork details
def get_query_2():

    artwork_collection = init_db()[constants.ARTWORK_COLLECTION]

    result = list(artwork_collection.aggregate([
        {
            "$project": {
            "_id": 0,
            "title": 1,
            "artistName": 1,
            "form": 1,
            "price": 1,
            "creationDate": 1,
        }}
    ]))
    print_table(result)

# Query 3: Get count of artists from a particular state
def get_query_3():
    artist_collection = init_db()[constants.ARTIST_COLLECTION]

    result = list(artist_collection.aggregate([
        {"$unwind": "$artwork"},
        {
            "$group": {
                "_id": "$state",
                "artist_count": {
                    "$sum": 1
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "stateName": "$_id",
                "ArtistCount": "$artist_count"
            }
        }
    ]))

    print_table(result)

# Query 4: Get Artwork and customers who bought them
def get_query_4():

    artwork_collection = init_db()[constants.ARTWORK_COLLECTION]

    result = list(artwork_collection.aggregate([
        {"$unwind": "$customer"},
        {"$project": {
            "_id": 0,
            "title": 1,
            "creationDate": 1,
            "CustomerName": "$customer.customerName",
            "City": "$customer.city",
            "State": "$customer.state"
        }
        }
    ]))
    print_table(result)

# Query 5: Get count of artworks by every artist
def get_query_5():
    artist_collection = init_db()[constants.ARTIST_COLLECTION]

    result = list(artist_collection.aggregate([
        {
            "$project": {
                "_id": 0,
                "artistName": 1,
                "artworkCount": {"$size": "$artwork"}
            }
        }
    ]))

    print_table(result)


def main():

    print('\n')
    print('Welcome to MongoDB Demo')
    print('\n')
    print(tabulate(constants.MENU_LIST, headers=['Sr No.', 'Menu Item'], tablefmt='psql'))
    user_choice = int(input('Enter menu number of your choice :  '))

    if user_choice not in constants.MENU_ITEMS:
        print('Invalid input, plese input appropriate menu item number')
        print('\n\n')
        print(tabulate(constants.MENU_LIST, headers=['Sr No.', 'Menu Item'], tablefmt='psql'))
        user_choice = int(input('Enter menu number of your choice:'))
        user_choice = int(input('Enter menu number of your choice: '))
    else:

        if user_choice == 1:

            artist_df = pandas.read_csv("csv/ARTIST.csv")
            artwork_df = pandas.read_csv("csv/ARTWORK.csv")
            customer_df = pandas.read_csv("csv/CUSTOMER.csv")
            bought_df = pandas.read_csv("csv/BOUGHT.csv")
            state_df = pandas.read_csv("csv/STATE.csv")

            artist_collection_count = init_db()[constants.ARTIST_COLLECTION].count_documents({})
            artwork_collection_count = init_db()[constants.ARTWORK_COLLECTION].count_documents({})

            # Truncate collection if not empty, to avoid redundent inster
            if artist_collection_count > 0:
                drop_artist()

            insert_artist_records(artist_df, artwork_df, state_df)

            # Truncate collection if not empty, to avoid redundent inster
            if artwork_collection_count > 0:
                drop_artwork()

            insert_artwork_records(artwork_df, artist_df, state_df, bought_df, customer_df)

        elif user_choice == 2:

            drop_artist(isPrint=True)
            drop_artwork(isPrint=True)

        elif user_choice == 3:
            get_query_1()
        elif user_choice == 4:
            get_query_2()
        elif user_choice == 5:
            get_query_3()
        elif user_choice == 6:
            get_query_4()
        elif user_choice == 7:
            get_query_5()
        elif user_choice == 8:
            exit(0)

        main()

main()
