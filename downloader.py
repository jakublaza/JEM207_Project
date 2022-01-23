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
            raise KeyError("Status code: " + r.status_code, "Error message: " + r.text, "Stopped on page: " + str(page))
        time.sleep(pause)
        return r               

def get_total_pages(token, start_date = "1.1.2020", end_date = "24.12.2021"):
    r = request(token, start_date = start_date, end_date = end_date)
    total_pages = int(r.json()["hydra:view"]["hydra:last"].split("=")[-1])
    return total_pages

def get_total_items(token, start_date = "1.1.2020", end_date = "24.12.2021"):
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
    df.to_parquet('data{a}.parquet'.format(a = int(i/50)), compression = 'snappy')
    df = pd.read_parquet('data{a}.parquet'.format(a = int(i/50)), engine = 'fastparquet').iloc[-30000:] 
    return df
    
def merging_interim_results(pages_total, intial_df = "1"):
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
    
    def __init__(self, df = 0):
        self.data = df
        self.info = {}

        if type(self.data) == int:
            print("No data loaded or downloaded.")
        else:
            print("The provided data were loaded.")
            self.get_info()
        
        self.status = None
        self.data = df
    
    def get_info(self):
        self.info["total cases"] = len(self.data)
        self.data.datum = pd.to_datetime(self.data.datum)
        self.data = self.data.sort_values(by = ["datum"])
        self.info["start_date"] = str(self.data.iloc[1].datum.strftime("%d.%m.%Y"))
        self.info["end_date"] = str(self.data.iloc[-1].datum.strftime("%d.%m.%Y"))
    
    def get_page(self, token, items_per_page = 5000):
        self.total_pages = get_total_pages(token, start_date = self.info["start_date"], end_date = self.info["end_date"])
        self.my_page = int(len(self.data)/5000) + 1
        print("You downloaded " + str(self.my_page) + " pages out of total " + str(self.total_pages) + " pages.")

    def downloader(self, token, start_page = 1, start_date = "1.1.2020", end_date = "24.12.2021", upd = "N"):

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
        data.to_parquet('datafinal{a}.gzip'.format(a = str("update"+upd)), compression = 'gzip')
        
        return data
    
    def updater(self, token, end_date = date.today()):
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
        
        if end_date_dtformat <= datetime.strptime(self.info["end_date"], "%d.%m.%Y"):
            raise ValueError("End date before start date!")
            
        data_new = self.downloader(token, start_page = 1, start_date = self.info["end_date"], end_date = end_date, upd = "Y")
        print(len(data_new))
        data_new.datum = pd.to_datetime(data_new.datum)
        self.data = self.data.append(data_new).drop_duplicates()
        self.data.to_parquet('datafinal.gzip', compression = 'gzip')
        self.get_info()
    

covid = Covid_Data()
