import requests 
import time
import pandas as pd

def counter(func):
  def wrapper(*args, **kwargs):
    wrapper.count += 1
    if wrapper.count == 998:
        print("Counter on 998!")
    else:     
    # Call the function being decorated and return the result
        return func(*args, **kwargs)
  wrapper.count = 1
  # Return the new decorated function
  return wrapper

token = "8a0ff681501b0bac557bf90fe6a036f7"
@counter
def request(url, token):
            r = requests.get(url, {"apiToken": token})
            return r 


def downloader(token, start_page = 1):
    if start_page == 1:
        df = pd.DataFrame()
    else:
        df = pd.read_parquet('data.gzip', engine='fastparquet')
    url_0 = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page=1&itemsPerPage=1000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
    req = request(url_0, token)
    pages_total = int(req.json()["hydra:view"]["hydra:last"].split("=")[-1])
    for i in range(start_page, pages_total+1):
        url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page="+ str(i) +"&itemsPerPage=1000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
        r = request(url, token)
        if r.status_code == 200:
            df = df.append(pd.DataFrame.from_dict(r.json()["hydra:member"]))
        if r.status_code == 429:
            msg = r.json()["message"]
            t = "".join(a for a in msg if a.isdigit())
            print("Now waiting for " + str(t+60)+ " seconds...")
            time.sleep(t+60)
            request.count = 1
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
data = downloader(token)