import pandas as pd
import db
import requests
import util


per_collection = db.config('per')
daily_collection = db.config('daily')
file_pool = util.file_pool_path

def run(year, month, day):
    crawling(year, month, day)
    process(year, month, day)


def crawling(year, month, day):
    date_f = str(year) + util.month(month) + util.day(day)

    url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date=" + \
        date_f + "&type=ALLBUT0999&_=1630244648174"

    res = requests.get(url)
    data = res.json()

    data_list = data["data9"]
    columns = data["fields9"]

    df = pd.DataFrame(data_list, columns=columns)
    df.to_csv(file_pool + "daily.csv", index=False, encoding='utf-8-sig')


def process(year, month, day):
    file = pd.read_csv(file_pool + "stockCode.csv", encoding='utf-8')
    data = list(file.loc[:, '公司代號'])

    for i in range(0, len(data)):  # len(data)
        stock_number = str(data[i])
        print("------ processing daily data ------ \n")
        print("start:", i, "code:", stock_number)

        # 如果daily的excel檔中如果沒有這代號(stock_number)的資料則略過此代號
        if readDailyData(stock_number).empty:
            print("end:  ", i, "code:", stock_number, "\n")
            continue

        index = readDailyData(stock_number).index[0]
        goal_stock = processGoalStockFormat(readDailyData(stock_number), index)
        datetimestamp = util.date_to_timestamp(year, month, day)
        obj = {
            "date": datetimestamp,
            "tradeShares": goal_stock['成交股數'],
            "tradePieces": goal_stock['成交筆數'],
            "tradeVolumes": goal_stock['成交金額'],
            "openPrice": goal_stock['開盤價'],
            "highPrice": goal_stock['最高價'],
            "lowPrice": goal_stock['最低價'],
            "closePrice": goal_stock['收盤價'],
            "upDowns": goal_stock['漲跌'],
            "peRatio": goal_stock['本益比'],
        }

        # 如果資料庫已經有該天日期的資料，則略過不存取
        query = {"$and": [{"code": stock_number, "time.date": datetimestamp}]}
        isDailyResultsExits = daily_collection.find(query, {"_id": 0})
        if list(isDailyResultsExits) != []:
            print("end:  ", i, "code:", stock_number, "\n")
            continue
        save_daily(stock_number, obj)

        save_per(stock_number, year, obj)

        print("end:  ", i, "code:", stock_number + "\n")


def save_daily(stock_number, obj):
    daily_results = daily_collection.find({"code": stock_number}, {"_id": 0})
    targetList = list(daily_results)[0]['time']
    targetList.append(obj)
    daily_collection.update_one({"code": stock_number}, {
                                "$set": {"time": targetList}})


def save_per(stock_number, year, obj):
    year = str(year)
    per = obj['peRatio']
    per_results = per_collection.find_one(
        {"code": stock_number}, {"_id": 0})

    if per_results.get(year) == None:
        obj = {
            "year": year,
            "maxper": None,
            "minper": None,
            "avgper": None
        }
        per_results['time'].append(obj)
        per_collection.update_one({"code": stock_number}, {
            "$set": {year: [], "time":  per_results['time']}
        })
    # 新增per
    per_results = per_collection.find_one(
        {"code": stock_number}, {"_id": 0})
    per_results[year].append(per)
    per_collection.update_one(
        {"code": stock_number}, {"$set": {year: per_results[year]}})
    results = per_collection.aggregate([
        {
            "$match": {
                "code": stock_number
            }
        },
        {
            "$project": {
                "avgper": {"$avg": "$" + year},
                "maxper": {"$max": "$" + year},
                "minper": {"$min": "$" + year},
                "_id": 0
            }
        }
    ])
    statistics = list(results)[0]
    statistics['year'] = year
    target = next(
        (item for item in per_results['time'] if item["year"] == year), None)
    target['maxper'] = statistics['maxper']
    target['minper'] = statistics['minper']
    target['avgper'] = statistics['avgper']
    per_collection.update_one(
        {"code": stock_number}, {"$set": {"time": per_results['time']}})


def processGoalStockFormat(goal_stock, goal_stock_index):
    成交股數 = ''
    成交筆數 = ''
    成交金額 = ''
    開盤價 = ''
    最高價 = ''
    最低價 = ''
    收盤價 = ''
    本益比 = ''
    漲跌 = ''
    # 處理特殊狀況
    if goal_stock['成交股數'][goal_stock_index] == '--':
        成交股數 = "--"
    else:
        成交股數 = int(str(goal_stock['成交股數'][goal_stock_index]).replace(',', ''))

    if goal_stock['成交筆數'][goal_stock_index] == '--':
        成交筆數 = goal_stock['成交筆數'] = "--"
    else:
        成交筆數 = int(str(goal_stock['成交筆數'][goal_stock_index]).replace(',', ''))

    if goal_stock['成交金額'][goal_stock_index] == '--':
        成交金額 = "--"
    else:
        成交金額 = int(str(goal_stock['成交金額'][goal_stock_index]).replace(',', ''))

    if goal_stock['開盤價'][goal_stock_index] == '--':
        開盤價 = "--"
    else:
        開盤價 = float(str(goal_stock['開盤價'][goal_stock_index]).replace(',', ''))

    if goal_stock['最高價'][goal_stock_index] == '--':
        最高價 = "--"
    else:
        最高價 = float(str(goal_stock['最高價'][goal_stock_index]).replace(',', ''))

    if goal_stock['最低價'][goal_stock_index] == '--':
        最低價 = "--"
    else:
        最低價 = float(str(goal_stock['最低價'][goal_stock_index]).replace(',', ''))

    if goal_stock['收盤價'][goal_stock_index] == '--':
        收盤價 = "--"
    else:
        收盤價 = float(str(goal_stock['收盤價'][goal_stock_index]).replace(',', ''))

    if goal_stock['本益比'][goal_stock_index] == '--':
        本益比 = "--"
    else:
        本益比 = float(str(goal_stock['本益比'][goal_stock_index]).replace(',', ''))

    if 本益比 == 0.00:
        本益比 = None

    # 改寫漲跌欄位格式
    if goal_stock['漲跌(+/-)'][goal_stock_index] == "<p style= color:green>-</p>":
        goal_stock['漲跌(+/-)'] = "-"
    elif goal_stock['漲跌(+/-)'][goal_stock_index] == "<p style= color:red>+</p>":
        goal_stock['漲跌(+/-)'] = "+"
    else:
        goal_stock['漲跌(+/-)'] = "0"

    if goal_stock['漲跌(+/-)'][goal_stock_index] == "0":
        漲跌 = 0
    else:
        漲跌 = float(goal_stock['漲跌(+/-)'][goal_stock_index] +
                   str(goal_stock['漲跌價差'][goal_stock_index]))

    obj = {
        "成交股數": 成交股數,
        "成交筆數": 成交筆數,
        "成交金額": 成交金額,
        "開盤價": 開盤價,
        "最高價": 最高價,
        "最低價": 最低價,
        "收盤價": 收盤價,
        "漲跌": 漲跌,
        "本益比": 本益比,
    }
    return obj


def readDailyData(stock_number):
    df = pd.read_csv(file_pool + "daily.csv")
    df['證券代號'] = df['證券代號'].astype("string")
    goal_daily = df[df['證券代號'] == str(stock_number)]

    return goal_daily