from playwright.sync_api import sync_playwright
import pandas as pd
import db
import time
import random
import util
# 執行時間
# 去年Q4 3/26、3/27、3/28、3/29、3/30、3/31、4/1、4/2、4/3、4/4、4/5、4/6、4/7、4/8、4/9、4/10
# 當年Q1 5/9、5/10、5/12、5/13、5/14、5/15、5/16、5/17、5/18、5/19、5/20
# 當年Q2 8/9、8/10、8/11、8/12、8/13、8/14、8/15、8/16、8/17、8/18、8/19
# 當年Q3 11/9、11/10、11/11、11/12、11/13、11/14、11/15、11/16、11/17、11/18、11/19

collection = db.config('eps')
file_pool = util.file_pool_path


def run(year, month):
    process(year, month)


def crawling(stock_number):
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        # Open new page
        page = context.new_page()
        # Go to https://www.cmoney.tw/finance/1101/f00041
        url = "https://www.cmoney.tw/finance/" + stock_number + "/f00041"
        page.goto(url)
        # 找到表格
        table = page.query_selector('table')
        # 如果表格不存在關掉網頁
        if table == None:
            context.close()
            return browser.close()

        # 找到表格中的每一列
        tr = table.query_selector_all('tr')

        save_eps(tr, stock_number)  # 儲存近期的EPS

        # ---------------------
        context.close()
        browser.close()
    return tr


def process(year, month):
    file = pd.read_csv(file_pool + "stockCode.csv", encoding='utf-8')
    data = list(file.loc[:, '公司代號'])
    year = util.year_n_quater(year, month)['year']
    quater = util.year_n_quater(year, month)['quater']

    for i in range(0, len(data)):  # len(data)
        stock_number = str(data[i])

        delay_choices = [1, 2, 3]  # 延遲的秒數
        delay = random.choice(delay_choices)  # 隨機選取秒數
        time.sleep(delay)

        print("------ processing eps data ------ \n")
        print("start:", i, "code:", stock_number)

        # 確認資料庫中是否存在該年季度的資料，如果不存在就爬取資料
        result = collection.find_one(
            {'code': stock_number, "year": str(year), "quater": quater})
        if result == None:
            crawling(stock_number)

        print("end:  ", i, "code:", stock_number + "\n")


def save_eps(tr, stock_number):
    year = ''
    quater = ''
    eps = 0.00
    # 整理表頭及所需列的資料
    for i in range(0, len(tr)):
        if i == 0:
            th = tr[i].query_selector_all('th')
            for j in range(1, 2):
                year = th[j].text_content().split('Q')[0]
                quater = th[j].text_content().split('Q')[1]
        elif i == len(tr)-1:
            td = tr[i].query_selector_all('td')
            for k in range(1, 2):
                eps = float(td[k].text_content())
    collection.update_one({"code": stock_number}, {
                          "$set": {"year": year, "quater": quater, "eps": eps}})