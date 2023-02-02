import pandas as pd
import db
import util

stockno_collection = db.config('stockno')
financial_collection = db.config('financial')
per_collection = db.config('per')
eps_collection = db.config('eps')
daily_collection = db.config('daily')
expect_collection = db.config('expect')
file_pool = util.file_pool_path


def run(year, month, day):
    process(year, month, day)


# 計算最大本益比的平均、最小本益比的平均、平均本益比的平均
def count_per(stock_number):
    maxper = []
    minper = []
    avgper = []

    results = per_collection.find_one(
        {'code': stock_number}, {"_id": 0, "time": 1})['time']

    for result in results:
        if result['maxper'] != None:
            maxper.append(result['maxper'])
        if result['minper'] != None:
            minper.append(result['minper'])
        if result['avgper'] != None:
            avgper.append(result['avgper'])

    return {'maxper_avg':  Average(maxper), 'minper_avg': Average(minper), 'total_avg': Average(avgper)}


# 計算可賣出價格、合理價格、可買入價格
def count_expect_price(stock_number):
    results = eps_collection.find_one(
        {'code': stock_number}, {"_id": 0, "eps": 1})

    # 如果沒有eps直接把價格調整到會被選取得範圍之外
    if results == {}:
        return {'max_price': 1000000, 'min_price': -1000000, 'average_price': 500000}

    eps = results['eps']

    max_price = round(count_per(stock_number)['maxper_avg'] * eps, 2)
    min_price = round(count_per(stock_number)['minper_avg'] * eps, 2)
    average_price = round(count_per(stock_number)['total_avg'] * eps, 2)

    return {'max_price': max_price, 'min_price': min_price, 'average_price': average_price}


# 查找並整理財報資訊
def process_financial_standard(stock_number, year, month):
    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1
    datetimestamp = util.date_to_timestamp(year, month, 1)
    stockno = stockno_collection.find_one(
        {"code": stock_number}, {"_id": 0, "code": 0})

    financial_data = financial_collection.find_one(
        {'code': stock_number, "time.date": datetimestamp}, {'_id': 0, "code": 0})
    if financial_data == None:
        return {
            'short': stockno['short'],
            'category': stockno['category'],
            'vsLastMonthIncGrade': '',
            'lastYearThisMonthIncGrade': '',
            'accumVsToPrePeriodGrade': '',
            'financialRemark': ''
        }
    target = next(
        (item for item in financial_data['time'] if item["date"] == datetimestamp), None)

    obj = {
        'short': stockno['short'],
        'category': stockno['category'],
        'vsLastMonthIncGrade': convert_grade(target['vsLastMonthIncPct']),
        'lastYearThisMonthIncGrade': convert_grade(target['lastYearThisMonthIncPct']),
        'accumVsToPrePeriodGrade': convert_grade(target['accumVsToPrePeriodPct']),
        'financialRemark': target['financialRemark']
    }

    return obj


def filter_match_data(stock_number, year, month, day):
    datetimestamp = util.date_to_timestamp(year, month, day)

    results = daily_collection.find_one(
        {'code': stock_number, "time.date": datetimestamp}, {"_id": 0, "time": 1})

    if results == None:
        return {}
    target = next(
        (item for item in results['time'] if item['date'] == datetimestamp), {})

    if target['closePrice'] == '--':
        return {}

    financial = process_financial_standard(stock_number, year, month)

    obj = {
        'code': stock_number,
        'short': financial['short'],
        'category': financial['category'],
        'tradeShares': target['tradeShares'],
        'tradePieces': target['tradePieces'],
        'tradeVolumes': target['tradeVolumes'],
        'openPrice': target['openPrice'],
        'highPrice': target['highPrice'],
        'lowPrice': target['lowPrice'],
        'closePrice': float(target['closePrice']),
        'upDowns': target['upDowns'],
        'availablePrice': count_expect_price(stock_number)['max_price'],
        'reasonablePrice': count_expect_price(stock_number)['average_price'],
        'buyablePrice': count_expect_price(stock_number)['min_price'],
        'vsLastMonthIncGrade': financial['vsLastMonthIncGrade'],
        'lastYearThisMonthIncGrade': financial['lastYearThisMonthIncGrade'],
        'accumVsToPrePeriodGrade': financial['accumVsToPrePeriodGrade'],
        'financialRemark': financial['financialRemark']
    }

    if obj['closePrice'] > obj['buyablePrice'] + 3:
        return {}
    if obj['availablePrice'] == 0.00 or obj['reasonablePrice'] == 0.00 or obj['buyablePrice'] == 0.00:
        return {}
    # 當收盤價小於或等於可買入股價，回傳該股票資料
    return obj


def process(year, month, day):
    file = pd.read_csv(file_pool + "stockCode.csv", encoding='utf-8')
    data = list(file.loc[:, '公司代號'])

    datetimestamp = util.date_to_timestamp(year, month, day)
    eligible_arr = []
    results = expect_collection.find_one(
        {"date": datetimestamp}, {"_id": 0})
    if results != None:
        return

    for i in range(0, len(data)):  # len(data)
        stock_number = str(data[i])
        print("------ processing epxect data ------ \n")
        print("start:", i, "code:", stock_number)

        if filter_match_data(stock_number, year, month, day) == None:
            print("end:  ", i, "code:", stock_number + "\n")
            continue
        if len(filter_match_data(stock_number, year, month, day)) == 0:
            print("end:  ", i, "code:", stock_number + "\n")
            continue

        eligible_arr.append(filter_match_data(stock_number, year, month, day))
        print("end:  ", i, "code:", stock_number + "\n")

    obj = {
        'date': datetimestamp,
        'list': eligible_arr
    }

    if obj['list'] == []:
        return

    expect_collection.insert_one(obj)


# 財報分級
def convert_grade(value):
    grade = 0

    try:
        if -100.00 > value:
            grade = 1
        elif -80.00 > value >= -100.00:
            grade = 2
        elif -60.00 > value >= -80.00:
            grade = 3
        elif -40.00 > value >= -60.00:
            grade = 4
        elif -20.00 > value >= -40.00:
            grade = 5
        elif 0 > value >= -20.00:
            grade = 6
        elif 20 > value >= 0:
            grade = 7
        elif 40 > value >= 20:
            grade = 8
        elif 60 > value >= 40:
            grade = 9
        elif 80 > value >= 60:
            grade = 10
        elif 100 > value >= 80:
            grade = 11
        elif value >= 100:
            grade = 12

        return grade
    except:
        return 0


# 陣列所有數平均並四捨五入到小數點後第二位
def Average(arr):
    if len(arr) == 0:
        return 0.00
    else:
        return round((sum(arr) / len(arr)), 2)