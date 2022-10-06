#  Craig Kimball 10/4/2022

import sqlite3
import requests
import json


#  Converts a meteorite's geolocation dictionary (if it has one) into plain text
def convert_dict_to_string(dict_entry):
    if dict_entry.get('geolocation', None) is None:
        return None
    return json.dumps(dict_entry['geolocation'])  # Decodes a json dictionary value into plain text


def main():
    response_obj = requests.get("https://data.nasa.gov/resource/gh4g-9sfh.json")
    # print(response_obj)  # Response code 200 means that everything is ok

    json_obj = response_obj.json()  # .json() returns a json object (all json really is, just a list of dictionaries)
    # print(json_obj[2])
    # print(json_obj[2]['geolocation'])  # First set of brackets gest the object at [index], the second pair is the specific data you want to access within the selected object
    # print(json_obj[2]['geolocation']['longitude'])  # We can even go 3 layers deep, since geolocation itself is a dictionary

    db_connnection = None  # Just a failsafe incase we don't get into the try block for whatever reason

    try:
        db_name = 'meteorites_data.db'  # db is the file extension for sql databases
        db_connection = sqlite3.connect(db_name)  # connect to a database. If it does not exist, create it

        db_cursor = db_connection.cursor()  # While not necessary, it's a better programming practice, it basically gives you multiple access to a database
        #  Kinda like how you have your functions are in another file, and you call them in a main file, you want to do everything through your cursor instead of your connection

        db_cursor.execute('''CREATE TABLE IF NOT EXISTS meteorite_data_all(
                name TEXT,
                id INTEGER,
                nametype TEXT,
                recclass TEXT,
                mass TEXT,
                fall TEXT,
                year TEXT,
                reclat TEXT,
                reclong TEXT,
                geolocation TEXT,
                states TEXT,
                counties TEXT);''')

        db_cursor.execute('''DELETE FROM meteorite_data_all''')  # This helps you from adding duplicate data

        for dict_entry in json_obj:
            db_cursor.execute('''INSERT INTO meteorite_data_all VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                              (dict_entry.get('name', None),
                               int(dict_entry.get('id', None)),
                               dict_entry.get('nametype', None),
                               dict_entry.get('recclass', None),
                               dict_entry.get('mass', None),
                               dict_entry.get('fall', None),
                               dict_entry.get('year', None),
                               dict_entry.get('reclat', None),
                               dict_entry.get('reclong', None),
                               convert_dict_to_string(dict_entry),
                               dict_entry.get(':@computed_region_cbhk_fwbd', None),
                               dict_entry.get(':@computed_region_nnqa_25f4', None)))

        db_cursor.execute('''CREATE TABLE IF NOT EXISTS filtered_data(
                        name TEXT,
                        id INTEGER,
                        nametype TEXT,
                        recclass TEXT,
                        mass TEXT,
                        fall TEXT,
                        year TEXT,
                        reclat TEXT,
                        reclong TEXT,
                        geolocation TEXT,
                        states TEXT,
                        counties TEXT);''')

        db_cursor.execute('''DELETE FROM filtered_data''')

        db_cursor.execute('''SELECT * FROM meteorite_data_all WHERE id <= 1000''')
        filter_result = db_cursor.fetchall()
        print(filter_result)

        for filtered_entry in filter_result:
            db_cursor.execute('''INSERT INTO filtered_data VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                              (filtered_entry[0], filtered_entry[1], filtered_entry[2], filtered_entry[3],
                               filtered_entry[4], filtered_entry[5], filtered_entry[6], filtered_entry[7],
                               filtered_entry[8], filtered_entry[9], filtered_entry[10], filtered_entry[11],))

        db_connection.commit()

    except sqlite3.Error as db_error:
        print(
            f'A database error has occured: {db_error}')  # The reason for the finally block is that you can return something and still proceed with the function
    finally:
        if db_connection:
            db_connection.close()
        print(
            'Job\'s done.')  # No matter what happens in the try or in the except, the code in finally block will execute


if __name__ == '__main__':
    main()
