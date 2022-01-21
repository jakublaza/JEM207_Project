import pandas as pd
import requests 
import time
import sys
import os
import io


token = "e2f053218f417ccbeb07a1e284d32dc1"
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
def request(token, page = 1, items_per_page = 5000):
        url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page={a}&itemsPerPage={b}&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020".format(a = page, b = items_per_page)
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
        return r               

def get_total_pages(token):
    r = request(token)
    total_pages = int(r.json()["hydra:view"]["hydra:last"].split("=")[-1])
    return total_pages

def get_total_items(token):
    r = request(token)
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

def duplicates_handling(duplicates, df, miss_values, prev_df_len, i, P, pdf):
    duplicates = pdf + 10000 - len(df) 
    print("duplicates: ", duplicates)
    duplicates_handling.miss_values = miss_values
    if duplicates > 0:
        print("Handling duplicates...")
        m = 1 # defiend to prevent infinite while loop
        while duplicates > 0: #should handle missing values due to duplicates
            if m == 8:
                print("unsuccesful", i)
                duplicates_handling.miss_values +=  duplicates
                P[i] = duplicates
                break                       
            elif m % 2 == 0:
                for j in range(max(i - 2, 1), i + 1):
                    e = request(token, j)
                    df = df.merge(pd.DataFrame.from_dict(e.json()["hydra:member"]), how = "outer").drop_duplicates()
                    duplicates = pdf + 10000 - len(df)
                    print("small", duplicates)        
            else:
                for n in range(max(int(i/2) - 1, 1), int(i/2) + 1):
                    e = request(token, n, 10000)
                    df = df.merge(pd.DataFrame.from_dict(e.json()["hydra:member"]), how = "outer").drop_duplicates()
                    duplicates = pdf + 10000 - len(df)
                    print("big", duplicates)
            m += 1
        if m < 5:
            print("Solved!")
            duplicates_handling.miss_values += duplicates
    return df   

def downloader(token, start_page = 1):
    if start_page == 1:
        r_0 = request(token, start_page)
        df = pd.DataFrame.from_dict(r_0.json()["hydra:member"])
        prev_df_len = 0
    else:
        start_page = int(start_page/50) * 50 + 1
        df = pd.read_parquet('data1.gzip', engine='fastparquet') #estabilis df, on which will be the merge preformed
        for k in range(2, int(start_page/50)+1):
            df = df.merge(pd.read_parquet('data{a}.gzip'.format(a = k), engine='fastparquet'), how = "outer")
        prev_df_len = len(df)
        print(prev_df_len)
        df = pd.read_parquet('data{a}.gzip'.format(a = int(start_page/50)), engine='fastparquet').iloc[-30000:]
    pages_total = get_total_pages(token)
    items_total = get_total_items(token)
    duplicates = 0
    miss_values = 0
    P = {1 : 0}
    pdf = 0
    for i in range(start_page + 1, pages_total + 1):
        r = request(token, i)
        df = df.merge(pd.DataFrame.from_dict(r.json()["hydra:member"]), how = "outer").drop_duplicates()
        if i % 2 == 0 or i == pages_total:
            print(prev_df_len, len(df), i * 5000)
            #os.system('clear')
            print("Currently on page " + str(i) + ". Progress: " + str(round((prev_df_len + len(df))/(r_0.json()['hydra:totalItems'])*100,1)) + " %.")
            df = duplicates_handling(duplicates, df, miss_values, prev_df_len, i, P, pdf)
            pdf = len(df) 
            miss_values = duplicates_handling.miss_values
            print("miss values:", miss_values, "duplicates:", duplicates)
        if i % 50 == 0:
            prev_df_len = len(df) - 30000 + prev_df_len
            df.to_parquet('data{a}.gzip'.format(a = int(i/50) + 1), compression='snappy')
            df = pd.read_parquet('data{a}.gzip'.format(a = int(i/50) + 1), engine='fastparquet').iloc[-30000:] 
            print(len(df), prev_df_len, i * 50 > 250000)
            pdf = 30000
            print(P)
        #print(i*5000, prev_df_len, len(df))
    df.to_parquet('data{a}.gzip'.format(a = "last"), compression='snappy')
    L = list(range(2, int(pages_total/50) + 1))
    L.append(str("last"))
    data = pd.read_parquet('data1.gzip', engine='snappy')
    for j in L:
        data = data.merge(pd.read_parquet('data{a}.gzip'.format(a = j), engine='fastparquet'), how = "outer")
        try:
            cwd = os.getcwd()
            os.remove(cwd + "/data{a}.gzip".format(a = j))
        except:
            None
    data.to_parquet('dataz.gzip', compression='gzip')
    return data

dataset = downloader(token, start_page = 1)
dataset.to_parquet('data_final.gzip', compression='gzip')



