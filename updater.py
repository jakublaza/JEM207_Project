import requests 
import pandas as pd
import numpy as np 
import math 
import time
from datetime import date
import fastparquet

from downloader import get_total_items

i = 1
url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page=" + str(i) +"&datum%5Bbefore%5D=01.01.2021&datum%5Bafter%5D=01.01.2020"
token = "e2f053218f417ccbeb07a1e284d32dc1"

r = requests.get(url, params = {"apiToken": token})
r

#ount = 0
#def counter():
    #global count
    #count += 1
    #return count

def counter(func):
  def wrapper(*args, **kwargs):
    wrapper.count += 1
    if wrapper.count == 999:
        time.sleep(3600)
        wrapper.count = 0
        return func(*args, **kwargs)
    else:   
        return func(*args, **kwargs)  
  wrapper.count = 0
  # Return the new decorated function
  return wrapper


#Need to load data
data = pd.read_parquet('example_fp.gzip', engine='fastparquet')

@counter
def updater(token, date = date.today()):
    df = pd.DataFrame()
    data = pd.read_parquet('data_final.gzip', engine='fastparquet')
    len_data = len(data)
    start_page = int(len_data/5000)
    start_date = data.iloc[-1].datum.strftime("%d.%m.%Y")
    end_date = date.strftime("%d.%m.%Y")
    prev_df_len = len(df) - 30000
    df = data[-30000:]
    total_pages = get_total_pages(token)
    total_items = get_total_items(token)
    for i in range(start_page + 1, total_items + 1):
        url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page=" + str(i) + "&itemsPerPage=5000&datum%5Bafter%5D={date}".format(date = start_date)
        r = request(url, token, i)
        df = df.merge(pd.DataFrame.from_dict(r.json()["hydra:member"]), how = "outer")
        if i % 2 == 0 or i == items_total:
            #os.system('clear')
            print("Currently on page " + str(i) + ". Progress: " + str(round((prev_df_len + len(df))/(r_0.json()['hydra:totalItems'])*100,1)) + " %.")
            duplicates = i*5000 - len(df) - prev_df_len
            if duplicates > 0:
                curr_len = len(df) + miss_values + prev_df_len
                expected_len = i*5000
                print(curr_len, prev_df_len, len(df))
                print("Handling duplicates...", end = "  ")
                m = 1 # defiend to prevent infinite while loop
                while curr_len < expected_len: #should handle missing values due to duplicates
                    if m == 8:
                        print("unsuccesful", i)
                        print(curr_len, expected_len)
                        miss_values = expected_len - curr_len
                        break
                    #elif m > 5:
                        #for m in range(max(int(i/2) - 2,1), int(i/2)+1):
                            #url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page={a}&itemsPerPage=10000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020".format(a = m)
                            #e = request(url, token)
                            #df = df.merge(pd.DataFrame.from_dict(e.json()["hydra:member"]), how = "outer")
                            #curr_len = len(df) + prev_df_len                        
                    elif m % 2 == 0:
                        for j in range(max(i - 3, 1), i + 1):
                            url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page={a}&itemsPerPage=5000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020".format(a = j)
                            e = request(url, token, i)
                            df = df.merge(pd.DataFrame.from_dict(e.json()["hydra:member"]), how = "outer")
                            curr_len = len(df) + prev_df_len
                    else:
                        for n in range(max(int(i/2) - 1, 1), int(i/2) + 1):
                            url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page={a}&itemsPerPage=10000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020".format(a = n)
                            e = request(url, token, i)
                            df = df.merge(pd.DataFrame.from_dict(e.json()["hydra:member"]), how = "outer")
                            curr_len = len(df) + prev_df_len
                    m += 1
                if m < 5:
                    print("Solved!", curr_len, expected_len)
                    duplicates = 0
    return df

df.to_parquet('df.parquet.gzip',compression='gzip') 