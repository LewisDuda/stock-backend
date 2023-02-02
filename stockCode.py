from playwright.sync_api import sync_playwright
import pandas as pd
import db
import util

file_pool = util.file_pool_path


def run():
    crawling()
    processStockCode()
    saveStockCode()


# 取得現行全部的上市公司
def crawling():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        # Open new page
        page = context.new_page()
        # Go to https://mops.twse.com.tw/mops/web/t51sb01
        page.goto("https://mops.twse.com.tw/mops/web/t51sb01")
        # Select
        page.locator("select[name=\"code\"]").select_option("")
        # Click input:has-text("查詢")
        page.locator("input:has-text(\"查詢\")").click()
        page.wait_for_timeout(7000)

        # Click text=另存CSV
        with page.expect_download() as download_info:
            page.locator("form[name=\"fm\"] button").click()
        download = download_info.value
        page.wait_for_timeout(1000)

        download.save_as(file_pool + "stockCode.csv")
        # ---------------------
        context.close()
        browser.close()


# 整理現行全部的上市公司
def processStockCode():
    # 取得特定欄位
    # open(origin_file_name, errors='ignore')直接開檔案來解決解碼問題
    df = pd.read_csv(open(file_pool + "stockCode.csv",
                     errors='ignore', encoding='big5'), usecols=[0, 2, 3])
    sdf = pd.DataFrame(df)
    sdf.to_csv(file_pool + "stockCode.csv", index=False, encoding='utf8')


# 儲存現行全部的上市公司至資料庫
def saveStockCode():
    financial_collection = db.config('financial')
    per_collection = db.config('per')
    daily_collection = db.config('daily')
    eps_collection = db.config('eps')
    stocknoAttributes_collection = db.config('stockno')

    df = pd.read_csv(file_pool + "stockCode.csv", encoding='utf-8-sig')

    for i in range(0, len(df)):
        obj = {
            'code': str(df.iloc[i]['公司代號']),
        }
        obj_time = {
            'code': str(df.iloc[i]['公司代號']),
            'time': []
        }
        obj_attributes = {
            'code': str(df.iloc[i]['公司代號']),
            'short': df.iloc[i]['公司簡稱'],
            'category': df.iloc[i]['產業類別']
        }

        stocknoAttributes_results = stocknoAttributes_collection.find(
            {'code': obj_attributes['code']})
        if list(stocknoAttributes_results) == []:
            stocknoAttributes_collection.insert_one(obj_attributes)
            print('stocknoAttributes collection insert stock code: ',
                  obj_attributes['code'])

        financial_result = financial_collection.find(
            {'code': obj_time['code']})
        if list(financial_result) == []:
            financial_collection.insert_one(obj_time)
            print('Financial collection insert stock code: ', obj_time['code'])

        per_result = per_collection.find({'code': obj_time['code']})
        if list(per_result) == []:
            per_collection.insert_one(obj_time)
            print('Per collection insert stock code: ', obj_time['code'])

        daily_result = daily_collection.find({'code': obj_time['code']})
        if list(daily_result) == []:
            daily_collection.insert_one(obj_time)
            print('Daily collection insert stock code: ', obj_time['code'])

        eps_result = eps_collection.find({'code': obj['code']})
        if list(eps_result) == []:
            eps_collection.insert_one(obj)
            print('Eps collection insert stock code: ', obj['code'], '\n')

    print('Check and save stock code complete!')