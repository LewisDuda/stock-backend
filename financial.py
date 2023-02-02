# 每天執行
from playwright.sync_api import sync_playwright
import db
import pandas as pd
import util

collection = db.config('financial')
file_pool = util.file_pool_path


def run(year, month):
    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1
    crawling(year, month)
    process(year, month)


def crawling(year, month):
    with sync_playwright() as playwright:
        yearStr = util.year(year)
        monthStr = util.month(month)

        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()

        # Open new page
        page = context.new_page()

        # Go to https://mops.twse.com.tw/mops/web/t21sc04_ifrs
        page.goto("https://mops.twse.com.tw/mops/web/t21sc04_ifrs")
        page.wait_for_timeout(1000)

        # Click #PageBody td:has-text("採用IFRSs後每月營業收入彙總表 市場別 國內上市 國外上市 國內上櫃 國外上櫃 國內興櫃 國外興櫃 國內公發公司 國外公發公司 年度 月份 1 2 3 4 ")
        page.locator(
            "#PageBody td:has-text(\"採用IFRSs後每月營業收入彙總表 市場別 國內上市 國外上市 國內上櫃 國外上櫃 國內興櫃 國外興櫃 國內公發公司 國外公發公司 年度 月份 1 2 3 4 \")").click()
        page.wait_for_timeout(1000)

        # Click input[name="year"]
        page.locator("input[name=\"year\"]").click()
        page.wait_for_timeout(1000)

        # Fill input[name="year"]
        page.locator("input[name=\"year\"]").fill(yearStr)
        page.wait_for_timeout(1000)

        # Select 02
        page.locator("select[name=\"month\"]").select_option(monthStr)
        # option個位數前面要加0 eg. 01
        page.wait_for_timeout(1000)

        # Click input:has-text("查詢")
        with page.expect_popup() as popup_info:
            page.locator("input:has-text(\"查詢\")").click()
        page1 = popup_info.value
        page.wait_for_timeout(1000)

        # Click text=另存CSV
        with page1.expect_download() as download_info:
            page1.locator("text=另存CSV").click()
        download = download_info.value
        page.wait_for_timeout(1000)

        download.save_as(file_pool + "financial.csv")

        # ---------------------
        context.close()
        browser.close()


def process(year, month):
    year = str(year)
    month = str(month)  # use 8 is not 08
    file = pd.read_csv(file_pool + "stockCode.csv", encoding='utf-8')
    data = list(file.loc[:, '公司代號'])
    datetimestamp = util.date_to_timestamp(year, month, 1)

    for i in range(0, len(data)):  # len(data)
        stock_number = str(data[i])
        print("------ processing " + year + "-" +
              month + " financial data ------ \n")
        print("start:", i, "code:", stock_number)

        # 確認excel檔案中該股票代號是否存在，如果不存在就略過
        if read_excel(stock_number).empty:
            print("end:  ", i, "code:", stock_number + "\n")
            continue

        # 確認資料庫中是否存在，如果存在就略過該股票代號並且不更新資料
        results = collection.find(
            {'code': stock_number, "time.date": datetimestamp})
        if list(results) != []:
            print("end:  ", i, "code:", stock_number + "\n")
            continue

        save(stock_number, datetimestamp)
        print("end:  ", i, "code:", stock_number + "\n")


def save(stock_number, datetimestamp):
    index = read_excel(stock_number).index[0]
    obj = {
        "date": datetimestamp,
        "currentMonthInc": int(read_excel(stock_number)['營業收入-當月營收'][index]),
        "lastMonthInc": int(read_excel(stock_number)['營業收入-上月營收'][index]),
        "lastYearThisMonthInc": int(read_excel(stock_number)['營業收入-去年當月營收'][index]),
        "vsLastMonthIncPct": float(read_excel(stock_number)['營業收入-上月比較增減(%)'][index]),
        "lastYearThisMonthIncPct": float(read_excel(stock_number)['營業收入-去年同月增減(%)'][index]),
        "accumToCurrentMonth": int(read_excel(stock_number)['累計營業收入-當月累計營收'][index]),
        "accumToLastYearThisMonth": int(read_excel(stock_number)['累計營業收入-去年累計營收'][index]),
        "accumVsToPrePeriodPct": float(read_excel(stock_number)['累計營業收入-前期比較增減(%)'][index]),
        "financialRemark": read_excel(stock_number)['備註'][index],
    }
    results = collection.find({"code": stock_number})
    for result in results:
        result['time'].append(obj)
        collection.update_one({"code": stock_number}, {
                              "$set": {"time": result['time']}})


def read_excel(stock_number):
    financial_df = pd.read_csv(file_pool + "financial.csv")
    financial_df['公司代號'] = financial_df['公司代號'].astype("string")
    goal_financial = financial_df[financial_df['公司代號'] == str(stock_number)]

    return goal_financial
