# COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University

### Goals

1. Provide COVID19 dataset containing detailed information on Russia.
2. Maintain CSSE compatibility
3. Provide some higher level APIs for accessing the data.
4. Close the project after a more systematic approch is developed

* [Upstream issue](https://github.com/CSSEGISandData/COVID-19/issues/1262)
* [Generic local discussion (if any)](https://github.com/grwlf/COVID-19_plus_Russia/issues/1)

_Disclamer: the author doesn't have relationships with any official and
commercial organisations. The data provided there is collected from unreliable
sources and may not be accurate. Please use it at your own risk._

### Contents

* `csse_covid_19_data` contains CSV files which were released by CSSE and later
  amended by us.
* [python3](./python3) folder contains stub and development tools:
  - `covid19ru.check` module for checking certain invariants
  - `covid19ru.fetch` Yandex data fetcher

### Data

#### Data source description

* https://github.com/CSSEGISandData/COVID-19
  - Upstream world data by CSSE.
* [Rospotrebnadzor](https://www.rospotrebnadzor.ru/about/info/news/)
  - The supposedly original official data source of COVID19 data in Russia.
    Data is published in Russian as a plain text. The source provides daily
    difference per region and current total for the whole state. Example:
    <https://www.rospotrebnadzor.ru/about/info/news/news_details.php?ELEMENT_ID=14125>
* [Yandex COVID19 map](https://yandex.ru/maps/covid19)
  - The Yandex company provides current per-region numbers.
* [NovelCoronaVirusChannel at Telegram](https://t.me/NovelCoronaVirusChannel)
  - Random COVID19 news in Russian.


#### Update procedure

Originally, author filled the data on Moscow and Saint Petersburg manually,
based on `Rospotrebnadzor` and `NovelCoronaVirusChannel` data. Starting from
March, 25 we follow the below procedure:

1. Fetch hourly data from `Yandex COVID map`
    - Fetching is done by running `monitor` function of the [fetcher
      script](./python3/src/covid19ru/fetch.py)
    - The data is saved into `pending` folder, stamped with UTC time.
2. Fetch daily upstream updates by using regular `git fetch` manually.
3. If update is available,
    1. Rebase repository to `upstream/master` branch using `git rebase`
    2. For every `csse_covid_19_data/csse_covid_19_daily_reports` file which doesn't
       have russian details, do the following:
        1. Determine the update time of 'Russia' record found in the world data.
           The time is supposed to be UTC. The update time is often near `23:30`
           (supposedly UTC time).
        2. Find the russian details dump in `pending` folder which has the
           closest UTC timestamp.
        3. Update world information file by inserting russian details manually.
        4. Review the format compatibility (CSV fields order, date format, etc.).
        5. Run the [checker script](./python3/src/covid19ru/check.py).
        6. Commit the changes to this repository.

#### Related repos

* https://github.com/AlexxIT/YandexCOVID
* https://github.com/klevin92/covid19_moscow_cases
* https://github.com/wolfxyx/moscow-covid-19

Visualizations:

* https://github.com/AaronWard/covid-19-analysis

### Roadmap

* Python code to check the correctness of CSV files
  - ~~Python stub checking the validity of basic CSV structure~~ (see
    [./python3/src/covid19ru/check.py](./python3/src/covid19ru/check.py) )
  - Check less-trivial invariants
* Python API to access the CSV data. It should handle the CSV format change
  which happened around 23.03.2020
  - Pandas API
  - Provide compatibility level for data before 23.03.2020
  - ???
* Semi-automated data loader from Yandex. Ideally, we want to perform the
  following actions:
  - ~~Collect `Confirmed/Death/Recovered` info for each Russian city~~ (starting
    from `03-25-2020.csv`)
  - ~~Save this information in a temporary file to handle update gap~~
  - ~~Set correct value of Longitude/Latitude for Russian regions~~
  - Figure out what does 'Active' field mean and how to get it
  - Daily update CSSE with Russian state information
* Find data on Russian regions for pre- 25.03.2020 period.

### Log

#### 25.03.2020

* Conflict resolved. `23-22-2020.csv` file seemed to be damaged by the upstream admins.
* <https://github.com/CSSEGISandData/COVID-19/issues/1523>
* Implemented Yandex data fetcher

#### 23.03.2020

Upstream format change: now

* `,,Moscow,Russia,2020-03-24 00:00:00,55.75222,37.61556,262,1,9,,"Moscow, Russia"`
* `,,"Saint Petersburg",Russia,2020-03-22 00:00:00,59.93863,30.31413,16,0,2,,"Saint Petersburg, Russia"`

#### 21.03.2020

We augmented CSV files from `csse_covid_19_daily_reports` folder by adding lines
like:

* `Moscow,Russia,2020-03-21T00:00:00,5,0,0,55.75222,37.61556`
* `"Saint Petersburg",Russia,2020-03-21T00:00:00,4,0,2,59.93863,30.31413`

**Original README.md starts here**

# 2019 Novel Coronavirus COVID-19 (2019-nCoV) Data Repository by Johns Hopkins CSSE


This is the data repository for the 2019 Novel Coronavirus Visual Dashboard operated by the Johns Hopkins University Center for Systems Science and Engineering (JHU CSSE). Also, Supported by ESRI Living Atlas Team and the Johns Hopkins University Applied Physics Lab (JHU APL).

<br>

<b>Visual Dashboard (desktop):</b><br>
https://www.arcgis.com/apps/opsdashboard/index.html#/bda7594740fd40299423467b48e9ecf6
<br><br>
<b>Visual Dashboard (mobile):</b><br>
http://www.arcgis.com/apps/opsdashboard/index.html#/85320e2ea5424dfaaa75ae62e5c06e61
<br><br>
<b>Lancet Article:</b><br>
[An interactive web-based dashboard to track COVID-19 in real time](https://doi.org/10.1016/S1473-3099(20)30120-1)
<br><br>
<b>Provided by Johns Hopkins University Center for Systems Science and Engineering (JHU CSSE):</b><br>
https://systems.jhu.edu/
<br><br>
<b>Data Sources:</b><br>
* World Health Organization (WHO): https://www.who.int/ <br>
* DXY.cn. Pneumonia. 2020. http://3g.dxy.cn/newh5/view/pneumonia.  <br>
* BNO News: https://bnonews.com/index.php/2020/02/the-latest-coronavirus-cases/  <br>
* National Health Commission of the People’s Republic of China (NHC): <br>
 http://www.nhc.gov.cn/xcs/yqtb/list_gzbd.shtml <br>
* China CDC (CCDC): http://weekly.chinacdc.cn/news/TrackingtheEpidemic.htm <br>
* Hong Kong Department of Health: https://www.chp.gov.hk/en/features/102465.html <br>
* Macau Government: https://www.ssm.gov.mo/portal/ <br>
* Taiwan CDC: https://sites.google.com/cdc.gov.tw/2019ncov/taiwan?authuser=0 <br>
* US CDC: https://www.cdc.gov/coronavirus/2019-ncov/index.html <br>
* Government of Canada: https://www.canada.ca/en/public-health/services/diseases/coronavirus.html <br>
* Australia Government Department of Health: https://www.health.gov.au/news/coronavirus-update-at-a-glance <br>
* European Centre for Disease Prevention and Control (ECDC): https://www.ecdc.europa.eu/en/geographical-distribution-2019-ncov-cases 
* Ministry of Health Singapore (MOH): https://www.moh.gov.sg/covid-19
* Italy Ministry of Health: http://www.salute.gov.it/nuovocoronavirus
* 1Point3Arces: https://coronavirus.1point3acres.com/en
* WorldoMeters: https://www.worldometers.info/coronavirus/
* COVID Tracking Project: https://covidtracking.com/data. (US Testing and Hospitalization Data. We use the maximum reported value from "Currently" and "Cumulative" Hospitalized for our hospitalization number reported for each state.)
* French Government: https://dashboard.covid19.data.gouv.fr/
* COVID Live (Australia): https://www.covidlive.com.au/
* Washington State Department of Health: https://www.doh.wa.gov/emergencies/coronavirus
* Maryland Department of Health: https://coronavirus.maryland.gov/
* New York State Department of Health: https://health.data.ny.gov/Health/New-York-State-Statewide-COVID-19-Testing/xdss-u53e/data
* NYC Department of Health and Mental Hygiene: https://www1.nyc.gov/site/doh/covid/covid-19-data.page and https://github.com/nychealth/coronavirus-data
* Florida Department of Health Dashboard: https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/arcgis/rest/services/Florida_COVID19_Cases/FeatureServer/0
and https://fdoh.maps.arcgis.com/apps/opsdashboard/index.html#/8d0de33f260d444c852a615dc7837c86

<br>
<b>Additional Information about the Visual Dashboard:</b><br>
https://systems.jhu.edu/research/public-health/ncov/
<br><br>

<b>Contact Us: </b><br>
* Email: jhusystems@gmail.com
<br><br>

<b>Terms of Use:</b><br>

1. This website and its contents herein, including all data, mapping, and analysis (“Website”), copyright 2020 Johns Hopkins University, all rights reserved, is provided solely for non-profit public health, educational, and academic research purposes. You should not rely on this Website for medical advice or guidance.  
2. Use of the Website by commercial parties and/or in commerce is strictly prohibited.   Redistribution of the Website or the aggregated data set underlying the Website is strictly prohibited.   
3. When linking to the website, attribute the Website as the COVID-19 Dashboard by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University, or the COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University.
4. The Website relies upon publicly available data from multiple sources that do not always agree. The Johns Hopkins University hereby disclaims any and all representations and warranties with respect to the Website, including accuracy, fitness for use, reliability, completeness, and non-infringement of third party rights. 
5. Any use of the Johns Hopkins’ names, logos, trademarks, and/or trade dress in a factually inaccurate manner or for marketing, promotional or commercial purposes is strictly prohibited.  
6. These terms and conditions are subject to change.   Your use of the Website constitutes your acceptance of these terms and conditions and any future modifications thereof.
