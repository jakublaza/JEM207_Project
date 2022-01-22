import pandas as pd
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

def duplicates_handling(df, i, P, pdf):
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
            P[i] = duplicates
    return df   

def saving_interim_results(df, i):
    df.to_parquet('data{a}.parquet'.format(a = int(i/50)), compression = 'snappy')
    df = pd.read_parquet('data{a}.parquet'.format(a = int(i/50)), engine = 'fastparquet').iloc[-30000:] 
    return df
    
def merging_interim_results(pages_total):
    L = list(range(2, int(pages_total/50) + 2))
    data = pd.read_parquet('data1.parquet', engine = 'fastparquet')
    for j in L:
        data = data.merge(pd.read_parquet('data{a}.parquet'.format(a = j), engine = 'fastparquet'), how = "outer")
        try:
            cwd = os.getcwd()
            os.remove(cwd + "/data{a}.parquet".format(a = j))
        except:
            None
    data.to_parquet('datafinal.gzip', compression = 'gzip')
    return data


def downloader(token, start_page = 1):
    if start_page == 1:
        r_0 = request(token, start_page)
        df = pd.DataFrame.from_dict(r_0.json()["hydra:member"])
        total_len = len(df)
        pdf = 0
    else:
        start_page = int(start_page/50) * 50 
        df = pd.read_parquet('data1.parquet', engine='fastparquet') #estabilis df, on which will be the merge preformed
        for k in range(1, int(start_page/50) + 1):
            df = df.merge(pd.read_parquet('data{a}.parquet'.format(a = k), engine = 'fastparquet'), how = "outer").drop_duplicates()
        pr_total_len = len(df) - int(start_page/50) * 50 * 5000
        df = df.iloc[-30000:]
        pdf = len(df)
    pages_total = get_total_pages(token)
    items_total = get_total_items(token)
    P = {1 : 0}
    for i in range(start_page + 1, pages_total + 1):
        r = request(token, i)
        df = df.merge(pd.DataFrame.from_dict(r.json()["hydra:member"]), how = "outer").drop_duplicates()
        if i % 2 == 0 or i == pages_total:
            df = duplicates_handling(df, i, P, pdf) 
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
    return data

dataset = downloader(token, start_page = 460)



