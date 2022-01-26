import pandas as pd
from datetime import date, datetime
import requests 
import time
import sys
import os
import io

token = "8a0ff681501b0bac557bf90fe6a036f7"
#e2f053218f417ccbeb07a1e284d32dc1
#8a0ff681501b0bac557bf90fe6a036f7

def counter(func):
  def wrapper(*args, **kwargs):
    wrapper.count += 1

    if wrapper.count == 2:
        global start 
        start = time.time()

    if wrapper.count == 998:
        end = time.time()
        wait_time = end - start + 120
        print("You aproached the limit of requests per hour. The download will automatically continue after " + str(int(wait_time)) + " seconds.")
        time.sleep(wait_time)

        return func(*args, **kwargs)

    else:     
        return func(*args, **kwargs)
  wrapper.count = 1
  return wrapper


@counter
def request(token, page = 1, items_per_page = 5000, start_date = "1.1.2020", end_date = "24.12.2021", pause = 0.1):
    """Request data from API and returs the response in json. The data in API are bounded to pages, 
    one request obtains data for onepage (5000 rows)
    Parameters
    ----------
    token : str
        input token for the API - can be obatained here: https://onemocneni-aktualne.mzcr.cz/vytvorit-ucet
    page : int
        specifies page which will be downloaded (default 1)
    items_per_page : int
        number of rows per page (defualt 5000)
    start_date = str
        begining date of the dataset - datum in format "dd.mm.YYYY" (default is "1.1.2020")
    end_date = str
        end date of the dataset - datum in format "dd.mm.YYYY" (default is "24.12.2021")
    pause : int
        to not overload the API (default 0.1)
    
    Raises
    ------
    Exception
        if response of API is not 200 or 429. 
    Returns
    -------
    r
        response of the API, in json
    """

    url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page={a}&itemsPerPage={b}&datum%5Bbefore%5D={d}&datum%5Bafter%5D={c}".format(a = page, b = items_per_page, c = start_date, d = end_date)
    r = requests.get(url, {"apiToken": token})
    
    if r.status_code == 200:
        None
    
    elif r.status_code == 429:
        msg = r.json()["message"]
        t = "".join(a for a in msg if a.isdigit())
        print("Holy Moly! You exceeded the requests limit per hour, now you need to wait " + t + " seconds...")
        time.sleep(int(t)+60)
        
        request.count = 1
        start = time.time()
        
        r = request(token, page)
    else:
        raise Exception("Status code: " + r.status_code, "Error message: " + r.text, "Stopped on page: " + str(page))
    
    time.sleep(pause)
    
    return r               

def get_total_pages(token, start_date = "1.1.2020", end_date = "24.12.2021"):
    """Indetify how much pages needs to be downloaded to download whole dataset for given 
    start date and date.
    Parameters
    ----------
    token : str
        input token for the API - can be obatained here: https://onemocneni-aktualne.mzcr.cz/vytvorit-ucet
    start_date : str
        begining date of the dataset - datum in format "dd.mm.YYYY" (default is "1.1.2020")
    end_date : str
        end date of the dataset - datum in format "dd.mm.YYYY" (default is "24.12.2021")
    Returns
    -------
    total_pages : int
        total number of pages for given period
    """
    r = request(token, start_date = start_date, end_date = end_date)
    total_pages = int(r.json()["hydra:view"]["hydra:last"].split("=")[-1])
    
    return total_pages

def get_total_items(token, start_date = "1.1.2020", end_date = "24.12.2021"):
    """Indetify how much rows is in the dataset for gievn start date and end date
    Parameters
    ----------
    token : str
        input token for the API - can be obatained here: https://onemocneni-aktualne.mzcr.cz/vytvorit-ucet
    start_date = str
        begining date of the dataset - datum in format "dd.mm.YYYY" (default is "1.1.2020")
    end_date = str
        end date of the dataset - datum in format "dd.mm.YYYY" (default is "24.12.2021")
    Returns
    -------
    total_items : int
        total number of rows in dataset for given time period
    """
    r = request(token, start_date = start_date, end_date = end_date)
    total_items = int(r.json()['hydra:totalItems'])
    
    return total_items

def get_vacination():
    r = requests.get("https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-pozitivni-hospitalizovani.csv-metadata.json")
    url = "https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/" + r.json()["url"]
    csv = requests.get("url")
    csv = csv.content.decode('utf8')
    csv_df = pd.read_csv(io.StringIO(csv))
    csv_df.to_parquet('dataz.gzip',compression='gzip')
    
    return csv_df

def duplicates_handling(df, i, P, pdf, start_date = "1.1.2020", end_date = "24.12.2021"):
    """Search for values that were not downloaded due to duplicates. 
    
    The API provides data based on pages - each pages can contain only certain amount of rows (in our case 5000). But we are
    downloading dataset with more than 2mil. rows, hence we need to download about 500 pages and merge them together. 
    Unforunatelly, the data on each page are not exactly ordered and it may happend that same value is on page 1 and page 2,
    for example. In other words, the obervations are not entirely fixed to specific row, thus when we request for page 1 many time
    we do not get exactly the same results for each request. We solved it by indetifying if there was any duplicates and if yes, 
    then we iterate for multiple times in neighbourhood of the page untill we get the missed values.
    
    Parameters
    ----------
    df : dataframe
        dataframe with covid data
    i : int
        a page where we curretly are 
    P : dic
        dictionary that stores duplicates P = {page:duplicates}
    start_date = str
        begining date of the dataset - datum in format "dd.mm.YYYY" (default is "1.1.2020")
    end_date = str
        end date of the dataset - datum in format "dd.mm.YYYY" (default is "24.12.2021")
    Returns
    -------
    df
        returns the dataframe hopefully with more rows
    """
    duplicates = pdf + 10000 - len(df) 
    print("duplicates: ", duplicates)
    
    if duplicates > 0:
        print("Handling duplicates...")

        m = 1 # defiend to prevent infinite while loop
        while duplicates > 0: #should handle missing values due to duplicates
            
            if m == 8:
                print("unsuccesful", i)
                P[i] = duplicates
                break                       
            elif m % 2 == 0:
                for j in range(max(i - 2, 1), i + 1):
                    e = request(token, j, start_date = start_date, end_date = end_date)
                    df = df.merge(pd.DataFrame.from_dict(e.json()["hydra:member"]), how = "outer").drop_duplicates()
                    duplicates = pdf + 10000 - len(df)
                    print("small", duplicates)        
            else:
                for n in range(max(int(i/2) - 1, 1), int(i/2) + 1):
                    e = request(token, n, 10000, start_date = start_date, end_date = end_date)
                    df = df.merge(pd.DataFrame.from_dict(e.json()["hydra:member"]), how = "outer").drop_duplicates()
                    duplicates = pdf + 10000 - len(df)
                    print("big", duplicates)
            m += 1
        
        if m < 5:
            print("Solved!")
            P[i] = duplicates
    
    return df   

def saving_interim_results(df, i):
    """Saves partial downloads of the dataframe to your folder. The saving happens every 50 pages. 
    It enables the code to run faster as when the part of the dataset is saved it is also drop. The data are
    saved as parquet with snappy compression, b/c it is fast to load. So we maximally
    work with df of length 280 000. And if your download is interapted you then do not need to start over again and 
    can begin close to where you stoped. 
    
    Parameters
    ----------
    df : dataframe
        dataframe with covid data
    i : int
        a page on which the download is

    Returns
    -------
    df
        last 30000 rows of your dataframe (the dataframe is not drop entirely, b/c there might
        be duplicaes between pages, so 30000 rows are left) 
    """
    df.to_parquet('data{a}.parquet'.format(a = int(i/50)), compression = 'snappy')
    df = pd.read_parquet('data{a}.parquet'.format(a = int(i/50)), engine = 'fastparquet').iloc[-30000:] 
    
    return df
    
def merging_interim_results(pages_total, intial_df = "1"):
    """Merges all the interim results created by function saving_interim_results(df, i) into final data set. And attemps
    to delete the interim results from your folder. We save the fianl dataset
    with .gzip compressino, which should be the best for space limition in parquet. 
    
    Parameters
    ----------
    pages_total : int
        total number of pages for given period
    intial_df : str
        a first interim result

    Returns
    -------
    data
        the final downloaded dataset
    """
    L = list(range(2, int(pages_total/50) + 2))
    data = pd.read_parquet('data{a}.parquet'.format(a = intial_df), engine = 'fastparquet')
    
    for j in L:
        data = data.merge(pd.read_parquet('data{a}.parquet'.format(a = j), engine = 'fastparquet'), how = "outer")
        try:
            cwd = os.getcwd()
            os.remove(cwd + "/data{a}.parquet".format(a = j))
        except:
            None
    return data



class Covid_Data:
    """
    A class used to manage covid data - storing, downloading and upadating

    ...

    Attributes
    ----------
    data : pandas data frame
        data frame of the covid data
    info : dic
        dictionary of information regarding covid data in attribute data (total cases(rows), start_date, end_date)
    total_pages : int
        information regarding how pages needs to be requested from API (loads by calling method get_page(token, items_per_page = 5000))
    my_page : int
        states on what page is your data set, helpful when only fraction of data were donwloaded (loads by calling method get_page(token, items_per_page = 5000))

    Methods
    -------
    get_info()
        loads info about the covid data in attribute data
    get_page(token, items_per_page = 5000)
        obtain info about how many pages were downloaded out of total pages (API send the data in pages)
    downloader(token, start_page = 1, start_date = "1.1.2020", end_date = "24.12.2021", upd = "N")
        downloads covid data from API
    updater(token, end_date = date.today())
        updates covid data stored in attribute data
    """
    
    def __init__(self, df = 0):
        """
        Parameters
        ----------
        data : int, dataframe
            if you already downloaded covid that then input the dataframe, otherwise input 0 - the data can be donwloaded by method download
        """

        self.data = df
        self.info = {"total cases": [], 
                     "start_date": [],
                     "end_date": []}

        if type(self.data) == int:
            print("No data loaded or downloaded.")
        else:
            print("The provided data were loaded.")
            self.get_info()
    
    def get_info(self):
        """
        loads info about the covid data in attribute data

        if no data frame is loaded/downloaded yet it returns empty dictionary
        """

        self.info["total cases"] = len(self.data)
        self.data.datum = pd.to_datetime(self.data.datum)
        self.data = self.data.sort_values(by = ["datum"])
        self.info["start_date"] = str(self.data.iloc[1].datum.strftime("%d.%m.%Y"))
        self.info["end_date"] = str(self.data.iloc[-1].datum.strftime("%d.%m.%Y"))
    
    def get_page(self, token):
        """
        obtain info about how many pages were downloaded out of total pages (API send the data in pages)

        Parameters
        ----------
        token : str
            input token for the API - can be obatained here: https://onemocneni-aktualne.mzcr.cz/vytvorit-ucet
        """
        self.total_pages = get_total_pages(token, start_date = self.info["start_date"], end_date = self.info["end_date"])
        self.my_page = int(len(self.data)/5000) + 1
        print("You downloaded " + str(self.my_page) + " pages out of total " + str(self.total_pages) + " pages.")

    def downloader(self, token, start_page = 1, start_date = "1.1.2020", end_date = "24.12.2021", upd = "N"):
        """
        downloads covid data from API

        Parameters
        ----------
        token : str
            input token for the API - can be obatained here: https://onemocneni-aktualne.mzcr.cz/vytvorit-ucet
        start_page : int
            declare on page you want to start the download - if you begin then 1, if you already downloaded some part you can resume 
            but page where you stoped needs to be specialzed, it can be found out througt method get_page() (default is 1)
        start_date = str
            begining of the covid data - datum in format "dd.mm.YYYY" (default is "1.1.2020")
        end_date = str
            end of the covid data - datum in format "dd.mm.YYYY" (default is "24.12.2021")
        upd = str
            only used by updater, irelevant for you (default is "N")
        """

        if start_page == 1:
            r_0 = request(token, start_page, start_date = start_date, end_date = end_date)
            df = pd.DataFrame.from_dict(r_0.json()["hydra:member"])
            total_len = len(df)
            pdf = 0
            pr_total_len = 0
        else:
            start_page = int(start_page/50) * 50 
            df = pd.read_parquet('data1.parquet', engine='fastparquet') #estabilis df, on which will be the merge preformed
            for k in range(1, int(start_page/50) + 1):
                df = df.merge(pd.read_parquet('data{a}.parquet'.format(a = k), engine = 'fastparquet'), how = "outer").drop_duplicates()
            pr_total_len = len(df) - int(start_page/50) * 50 * 5000
            df = df.iloc[-30000:]
            pdf = len(df)

        pages_total = get_total_pages(token, start_date = start_date, end_date = end_date)
        items_total = get_total_items(token, start_date = start_date, end_date = end_date)
        
        P = {1 : 0}

        for i in range(start_page + 1, pages_total + 1):
            r = request(token, i, start_date = start_date, end_date = end_date)
            df = df.merge(pd.DataFrame.from_dict(r.json()["hydra:member"]), how = "outer").drop_duplicates()

            if i % 2 == 0 or i == pages_total:
                df = duplicates_handling(df, i, P, pdf, start_date = start_date, end_date = end_date) 
                total_len = i * 5000 - sum(P.values()) + pr_total_len
                print("Currently on page " + str(i) + ". Progress: " + str(round((total_len/items_total) * 100, 1)) + " %.") 
                print(total_len, i*5000, len(df))
                pdf = len(df)

            if i % 50 == 0:
                df = saving_interim_results(df, i)
                pdf = 30000
                print(len(df), i * 5000,  "ahead/behind: " + str(total_len - i*5000))
                print(P)

        df.to_parquet('data{a}.parquet'.format(a = int(i/50)+1), compression = 'snappy')
        
        data = merging_interim_results(pages_total)
        
        if upd == "N":
            data.to_parquet('datafinal{a}.gzip'.format(a = str("update"+upd)), compression = 'gzip')
            self.data = data
        else:
            data.to_parquet('dataupdate.gzip', compression = 'gzip')
            return data
    
    def updater(self, token, end_date = date.today()):
        """
        updates covid data from API

        Parameters
        ----------
        token : str
            input token for the API - can be obatained here: https://onemocneni-aktualne.mzcr.cz/vytvorit-ucet
        end_date : str, datetime
            until what date you want to update the date (default date.today())
        """
        if type(end_date) == str:
            try:
                end_date_dtformat = datetime.strptime(end_date, "%d.%m.%Y")
                print("updating...")
            except:
                print("Incorrect date type, should be DD.MM.YYYY")
        else:
            end_date_dtformat = end_date
            end_date = end_date.strftime("%d.%m.%Y")
            print("updating...")
        
        if end_date_dtformat < datetime.strptime(self.info["end_date"], "%d.%m.%Y"):
            raise ValueError("End date before start date!")
            
        data_new = self.downloader(token, start_page = 1, start_date = self.info["end_date"], end_date = end_date, upd = "Y")
        data_new.datum = pd.to_datetime(data_new.datum)
        self.data = self.data.append(data_new).drop_duplicates()
        self.data.to_parquet('datafinal.gzip', compression = 'gzip')
        
        self.get_info()
    

covid = Covid_Data()

covid.downloader(token)
