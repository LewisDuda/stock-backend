# Python + Mongodb

This application collects TWSE daily after-hours information and company financials, and makes the collected information according to the expected analysis needs.

### Features of this project

**Collect data using web automation**

-   Use Playwright to collect the data needed for this application.

---

## Project Features

> -   Obtain all current listed companies
>     > Obtain the current stock codes of all listed companies every day.

> -   Obtain financial report information of listed companies
>     > Obtain current financial report information of all listed companies.

> -   Obtain the eps of listed companies
>     > Obtain the eps of all current listed companies on a specific date.

> -   Obtain daily after-hours information
>     > Obtain after-hours information on all currently listed companies.

> -   Make a list of forecasts
>     > Make a list of forecasts and store them in the database.

> -   Backup Mongodb data
>     > Backup mongodb data to a specified location.

> -   Delete expired backup data
>     > Delete the expired mongodb backup data.

---

## Install this project

If you need a copy of this project and run it locally on your computer please see the instructions below.

**Clone Project**

```
$ git clone https://github.com/LewisDuda/stock-backend.git
```

### Usage Packages

-   [pymongo](https://pypi.org/project/pymongo/)
-   [python-dotenv](https://pypi.org/project/python-dotenv/)
-   [playwright](https://playwright.bootcss.com/python/docs/intro)
-   [pandas](https://pypi.org/project/pandas/)

### Setup App

**1. Create .env file**

```
$ touch .env
```

**2. Write your MONGODB_URL and MONGODB_COLLECTION into .env file and save.**

```
// Your mongodb_url and mongodb_collection
DB_URL = YOUR_DB_URL
DB_COLLECTION = YOUR_DB_COLLECTION

```

**3. Start the App**

```
$ python main.py
```

---

## Related project

[Frontend Application](https://github.com/LewisDuda/stock)

[API Application](https://github.com/LewisDuda/stock-api-server)

**Clone Project**

```
// Frontend Application
$ git clone https://github.com/LewisDuda/stock.git

// API Application
$ git clone https://github.com/LewisDuda/stock-api-server.git
```

---

## Author

[Lewis](https://github.com/LewisDuda)
