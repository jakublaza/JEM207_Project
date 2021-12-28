import requests 
import time
import pandas as pd
import io
import os

token = "8a0ff681501b0bac557bf90fe6a036f7"

def counter(func):
  def wrapper(*args, **kwargs):
    wrapper.count += 1
    #start = time.time()
    if wrapper.count == 999:
        #end = time.time()
        #wait_time = end - start + 120
        #print("You aproached the limit of requests per hour. The download will automatically continue after " + str(wait_time) + " seconds.")
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
        i = int(start_page/100) * 100
        df = pd.DataFrame()
        for k in range(1, int(i/100)+1):
            df = df.append(pd.read_parquet('data{a}.gzip'.format(a = k), engine='fastparquet'))
        prev_df_len = len(df)
        url_1 = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page="+ str(i) +"&itemsPerPage=5000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
        r_1 = request(url_1, token)
        df = pd.DataFrame.from_dict(r_1.json()["hydra:member"])
    pages_total = get_total_pages(token)
    items_total = get_total_items(token)
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
        if i % 2 == 0:
            print("Currently on page " + str(i) + ". Progress: " + str(round((prev_df_len + len(df))/(r_0.json()['hydra:totalItems'])*100,1)) + " %.", end='\r')
        if i % 100 == 0:
            prev_df_len = len(df) + prev_df_len
            df.to_parquet('data{a}.gzip'.format(a = int(i/100)),compression='gzip')
            url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page="+ str(i+1) +"&itemsPerPage=5000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
            r = request(url, token)
            df = pd.DataFrame.from_dict(r.json()["hydra:member"])
            L = list(range(int(i/100)))
        if i == pages_total-1:
            df.to_parquet('data{a}.gzip'.format(a = int(i/100)),compression='gzip')
            i = 0
            df = pd.DataFrame.from_dict(r_0.json()["hydra:member"])
            print("Starting second round")
        i += 1
    df.to_parquet('data{a}.gzip'.format(a = i/100),compression='gzip')
    L.append(i/100)
    data = pd.DataFrame()
    for j in L:
        data = data.append(pd.read_parquet('data{a}.gzip'.format(a = j), engine='fastparquet'))
        cwd = os.getcwd()
        os.remove(cwd+"/data{a}.gzip".format(a = j))
    data.to_parquet('dataz.gzip',compression='gzip')
    print("DONE")
    return data
dataset = downloader(token, start_page=350)
dataset.to_parquet('dataz.gzip',compression='gzip')

