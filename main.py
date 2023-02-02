from datetime import datetime
import stockCode
import financial
import eps
import daily
import expect
import util

date_string = datetime.now().strftime("%Y-%m-%d")
year = int(date_string.split("-")[0])
month = int(date_string.split("-")[1])
day = int(date_string.split("-")[2])


if __name__ == '__main__':
    stockCode.run()
    financial.run(year, month)
    if util.is_eps_running(month, day):
        eps.run(year, month)
    daily.run(year, month, day)  
    expect.run(year, month, day)  
    util.backup()
    util.delete_outdated_backup()
