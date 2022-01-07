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
    if wrapper.count == 10:
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
def request(url, token, i = 1):
        r = requests.get(url, {"apiToken": token})
        if r.status_code == 200:
            None
        elif r.status_code == 429:
            msg = r.json()["message"]
            t = "".join(a for a in msg if a.isdigit())
            print("Holy Moly! You exceeded the requests limit per hour, now you need to wait " + t + " seconds...")
            time.sleep(int(t)+60)
            request.count = 1
            r = request(url, token)
        else:
            raise KeyError("Status code: " + r.status_code, "Error message: " + r.text, "Stopped on page: " + str(i))
        return r               

def get_total_pages(token):
    url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page=1&itemsPerPage=5000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
    r = request(url, token)
    total_pages = int(r.json()["hydra:view"]["hydra:last"].split("=")[-1])
    return total_pages

def get_total_items(token):
    url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page=1&itemsPerPage=5000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
    r = request(url, token)
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

def downloader(token, start_page = 1):
    url_0 = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page="+ str(start_page) +"&itemsPerPage=5000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
    r_0 = request(url_0, token)
    if start_page == 1:
        df = pd.DataFrame.from_dict(r_0.json()["hydra:member"])
        prev_df_len = 0
    else:
        start_page = int(start_page/50) * 50 + 1
        df = pd.DataFrame()
        for k in range(1, int(start_page/50)+1):
            df = df.append(pd.read_parquet('data{a}.gzip'.format(a = k), engine='fastparquet'))
        prev_df_len = len(df) - 30000
        print(prev_df_len)
        df = pd.read_parquet('data{a}.gzip'.format(a = int(start_page/50)), engine='fastparquet').iloc[-30000:]
    pages_total = get_total_pages(token)
    items_total = get_total_items(token)
    duplicates = 0
    miss_values = 0
    for i in range(start_page + 1, items_total + 1):
        url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page="+ str(i) +"&itemsPerPage=5000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
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
        if i % 50 == 0:
            prev_df_len = len(df) - 30000 + prev_df_len
            df.to_parquet('data{a}.gzip'.format(a = int(i/50) + 1), compression='gzip')
            df = pd.read_parquet('data{a}.gzip'.format(a = int(i/50) + 1), engine='fastparquet').iloc[-30000:] 
            print(len(df), prev_df_len)
        #print(i*5000, prev_df_len, len(df))
    df.to_parquet('data{a}.gzip'.format(a = "last"), compression='gzip')
    L = list(range(1, int(489/50) + 1))
    L.append(str("last"))
    data = pd.DataFrame()
    for j in L:
        data = data.append(pd.read_parquet('data{a}.gzip'.format(a = j), engine='fastparquet'))
        cwd = os.getcwd()
        os.remove(cwd + "/data{a}.gzip".format(a = j))
    data.to_parquet('dataz.gzip', compression='gzip')
    return data

dataset = downloader(token, start_page = 1)
dataset.to_parquet('dataz.gzip', compression='gzip')





