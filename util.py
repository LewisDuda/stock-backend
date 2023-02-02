import datetime
from bson.json_util import dumps
import json
import bson
import os
import db

file_pool_path = "./file_pool/"
backup_path = "./backup/"


def year(year):
    if int(year) > 2000:
        year = str(year - 1911)
        return year


def month(month):
    if int(month) < 10:
        month = "0" + str(month)
        return month
    else:
        month = str(month)
        return month


def day(day):
    if int(day) < 10:
        day = "0" + str(day)
        return day
    else:
        day = str(day)
        return day


def date_to_timestamp(year, month, day):
    s_date = str(year) + "-" + str(month) + "-" + str(day)
    return int(datetime.datetime.strptime(s_date, "%Y-%m-%d").timestamp() * 1000)


def year_n_quater(year, month):
    quater = ""
    if month == 3 or month == 4:
        year -= 1
        quater = "4"
    elif month == 5:
        quater = "1"
    elif month == 8:
        quater = "2"
    elif month == 11:
        quater = "3"
    else:
        quater = ""

    return {"year": year, "quater": quater}


def is_eps_running(month, day):
    query_str = str(month) + str(day)
    list_pool = [
        "326", "327", "328", "329", "330", "331", "41", "42", "43", "44", "45", "46", "47", "48", "49", "410",
        "59", "510", "511", "512", "513", "514", "515", "516", "517", "518", "519", "520",
        "89", "810", "811", "812", "813", "814", "815", "816", "817", "818", "819",
        "119", "1110", "1111", "1112", "1113", "1114", "1115", "1116", "1117", "1118", "1119"
    ]
    for item in list_pool:
        if query_str == item:
            return True
    return False


def jsontodb(collection_name):
    collection = db.config(collection_name)

    file_name = './file_pool/' + collection_name + '.json'
    with open(file_name, "r", encoding="utf-8") as file:
        file_data = json.load(file)

    collection.insert_many(file_data)


def dbtojson(collection_name):
    collection = db.config(collection_name)

    # Now creating a Cursor instance
    # using find() function
    cursor = collection.find({}, {"_id": 0})

    # Converting cursor to the list of dictionaries
    list_cur = list(cursor)

    # Converting to the JSON
    json_data = dumps(list_cur, indent=2, ensure_ascii=False).encode('utf8')

    # # Writing data to file data.json
    file_name = './file_pool/' + collection_name + '.json'
    with open(file_name, 'wb') as file:
        file.write(json_data)


def dbtobson(file_name, collection_name):
    collection = db.config(collection_name)

    with open(file_name, 'wb+') as f:
        for doc in collection.find():
            f.write(bson.BSON.encode(doc))


def backup():
    date_string = datetime.datetime.now().strftime("%Y-%m-%d")
    year = date_string.split("-")[0]
    month = date_string.split("-")[1]
    day = date_string.split("-")[2]
    date = year + month + day

    collection_list = ['daily', 'eps', 'expect', 'financial', 'per', 'stockno']
    print("------ processing backup------ \n")
    if not os.path.isdir(backup_path + date):
        os.mkdir(backup_path + date)

        for collection in collection_list:
            print('Start backup ' + date + ' - ' + collection)
            file_name = backup_path + date + '/' + collection + '.bson'
            dbtobson(file_name, collection)
            print('End backup ' + date + ' - ' + collection + '\n')

        return print(date + ' backup successful. \n')

    print(date + ' backup already exists. \n')


def delete_outdated_backup():
    date_string = (datetime.datetime.now() -
                   datetime.timedelta(days=8)).strftime("%Y-%m-%d")
    year = date_string.split("-")[0]
    month = date_string.split("-")[1]
    day = date_string.split("-")[2]
    date = year + month + day
    collection_list = ['daily', 'eps', 'expect', 'financial', 'per', 'stockno']

    if os.path.exists(backup_path + date):
        for collection in collection_list:
            file_name = backup_path + date + '/' + collection + '.bson'
            os.remove(file_name)
        os.rmdir(backup_path + date)
        return print("Successfully deleted outdated backup files.")

    print(date + " backup does not exist.")
