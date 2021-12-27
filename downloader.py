import requests 
import time
import pandas as pd

token = "8a0ff681501b0bac557bf90fe6a036f7"

def counter(func):
  def wrapper(*args, **kwargs):
    wrapper.count += 1
    if wrapper.count == 998:
        print("Counter on 998!")
        time.sleep(2500)
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
    url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page=1&itemsPerPage=1000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
    r = request(url, token)
    total_pages = int(r.json()["hydra:view"]["hydra:last"].split("=")[-1])
    return total_pages

def downloader(token, start_page = 1):
    if start_page == 1:
        df = pd.DataFrame()
    else:
        df = pd.read_parquet('data.gzip', engine='fastparquet')
    pages_total = get_total_pages(token)
    for i in range(start_page, pages_total+1):
        url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page="+ str(i) +"&itemsPerPage=1000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
        r = request(url, token)
        if r.status_code == 200:
            df = df.append(pd.DataFrame.from_dict(r.json()["hydra:member"]))
        elif r.status_code == 429:
            msg = r.json()["message"]
            t = "".join(a for a in msg if a.isdigit())
            print("Now waiting for " + str(t+60)+ " seconds...")
            time.sleep(t+60)
            request.count = 1
            r = request(url, token)
            df = df.append(pd.DataFrame.from_dict(r.json()["hydra:member"]))
        else:
            print(r.status_code)
            print(r.text)
            print("Stopped on page: " + str(i))
            break
        if i % 50 == 0:
            print("Currently on page " + str(i) + ". Progress: " + str(round(i/(pages_total+1)*100,1)) + " %.", end='\r')
        if i % 200 == 0:
             df.to_parquet('data.gzip'.format(a = i),compression='gzip')
    print("DONE")
    return df

df.to_parquet('data.gzip'.format(a = i),compression='gzip')

