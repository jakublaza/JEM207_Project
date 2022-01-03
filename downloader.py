from numpy import exp
import requests 
import time
import pandas as pd
import io
import os

token = "e2f053218f417ccbeb07a1e284d32dc1"

def counter(func):
  def wrapper(*args, **kwargs):
    wrapper.count += 1
    if wrapper.count == 1:
        start = time.time()
    if wrapper.count == 999:
        end = time.time()
        wait_time = end - start + 120
        print("You aproached the limit of requests per hour. The download will automatically continue after " + str(wait_time) + " seconds.")
        time.sleep(2400)
        return func(*args, **kwargs)
    else:     
        return func(*args, **kwargs)
  wrapper.count = 1
  return wrapper


@counter
def request(url, token):
            r = requests.get(url, {"apiToken": token})
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
    i = start_page 
    url_0 = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page="+ str(i) +"&itemsPerPage=5000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
    r_0 = request(url_0, token)
    if i == 1:
        df = pd.DataFrame.from_dict(r_0.json()["hydra:member"])
        prev_df_len = 0
    else:
        i = int(start_page/50) * 50 
        df = pd.DataFrame()
        for k in range(1, int(i/100)+1):
            df = df.append(pd.read_parquet('data{a}.gzip'.format(a = k), engine='fastparquet'))
        prev_df_len = len(df)
        url_1 = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page="+ str(i) +"&itemsPerPage=5000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
        r_1 = request(url_1, token)
        df = pd.DataFrame.from_dict(r_1.json()["hydra:member"])
    pages_total = get_total_pages(token)
    items_total = get_total_items(token)
    duplicates = 0
    P = {"1": 0}
    sum_dupl = 0
    while prev_df_len + len(df) < items_total:
        url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page="+ str(i+1) +"&itemsPerPage=5000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
        r = request(url, token)
        if r.status_code == 200:
            df = df.merge(pd.DataFrame.from_dict(r.json()["hydra:member"]), how = "outer")
        elif r.status_code == 429:
            msg = r.json()["message"]
            t = "".join(a for a in msg if a.isdigit())
            print("Holy Moly! You exceeded the requests limit per hour, now you need to wait " + str(t+60)+ " seconds...")
            time.sleep(t+60)
            request.count = 1
            r = request(url, token)
            df = df.merge(pd.DataFrame.from_dict(r.json()["hydra:member"]), how = "outer")
        else:
            print(r.status_code)
            print(r.text)
            print("Stopped on page: " + str(i))
            break
        if (i+1) % 2 == 0:
            print("Currently on page " + str(i+1) + ". Progress: " + str(round((prev_df_len + len(df))/(r_0.json()['hydra:totalItems'])*100,1)) + " %.")
        if (i+1) % 50 == 0: #save by 50; drop when i = 100
            inter_df_len = len(df)
            df.to_parquet('data{a}.gzip'.format(a = int(i/50)),compression='gzip')
            if (i+1) % 100 == 0:
                df = pd.read_parquet('data{a}.gzip'.format(a = int(i/50)), engine='fastparquet')
                prev_df_len = inter_df_len - len(df)
            L = list(range(int(i/100)+1))
        sum_dupl = duplicates + sum_dupl
        duplicates = (i+1)*5000 - len(df) - sum_dupl - prev_df_len
        if (i+1) % 2 == 0:
            if duplicates > 0:
                P[i+1] = duplicates
                curr_len = len(df)
                expected_len = (i+1)*5000
                print(curr_len, expected_len)
                m = 0 # defiend to prevent infinite while loop
                while curr_len < expected_len: #should handle missing values due to duplicates
                    print("handling duplicates", i)
                    for j in range(max((i+1)-4, 1), (i+1)+1):
                        url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page={a}&itemsPerPage=5000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020".format(a = i)
                        e = request(url, token)
                        df = df.merge(pd.DataFrame.from_dict(e.json()["hydra:member"]), how = "outer")
                        curr_len = len(df)
                    if m > 1:
                        for n in range(max(int((i+1)/2) - 2,1), int((i+1)/2)+1):
                            print("changing iperpage number")
                            url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page={a}&itemsPerPage=10000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020".format(a = n)
                            e = request(url, token)
                            df = df.merge(pd.DataFrame.from_dict(e.json()["hydra:member"]), how = "outer")
                            curr_len = len(df)
                    if m == 10:
                        print("unsuccesful")
                        break
                    m += 1
                
                print(curr_len, expected_len)
        i += 1
    print(prev_df_len, len(df))   
    print("I exited loop")
    df.to_parquet('data{a}.gzip'.format(a = i/100),compression='gzip')
    L.append(i/100)
    data = pd.DataFrame()
    for j in L:
        data = data.append(pd.read_parquet('data{a}.gzip'.format(a = j), engine='fastparquet'))
        cwd = os.getcwd()
        os.remove(cwd+"/data{a}.gzip".format(a = j))
    data.to_parquet('dataz.gzip',compression='gzip')
    print(P)
    return data

dataset = downloader(token, start_page= 1)
dataset.to_parquet('dataz.gzip',compression='gzip')




