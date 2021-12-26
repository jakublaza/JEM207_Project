import requests 
import time
import pandas as pd

def counter(func):
  def wrapper(*args, **kwargs):
    wrapper.count += 1
    if wrapper.count == 999:
        print("Counter on 999!")
        try:
            msg = wrapper.json()["message"]
            print(msg)
            t = "".join(a for a in msg if a.isdigit())
            time.sleep(t+60)
            wrapper.count = 1
            return func(*args, **kwargs)
        except:
            time.sleep(3000)
            wrapper.count = 1
            return func(*args, **kwargs)
    else:     
    # Call the function being decorated and return the result
        return func(*args, **kwargs)
  wrapper.count = 1
  # Return the new decorated function
  return wrapper

token = "e2f053218f417ccbeb07a1e284d32dc1"
@counter
def request(url, token):
            r = requests.get(url, {"apiToken": token})
            return r 


def downloader(token, start_page = 0):
    df = pd.DataFrame()
    url_0 = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page=1&itemsPerPage=1000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
    req = request(url_0, token)
    pages_total = int(req.json()["hydra:view"]["hydra:last"].split("=")[-1])
    for i in range(start_page, pages_total+1):
        url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page="+ str(i) +"&itemsPerPage=1000&datum%5Bbefore%5D=24.12.2021&datum%5Bafter%5D=1.1.2020"
        r = request(url, token)
        if r.status_code == 200:
            df = df.append(pd.DataFrame.from_dict(r.json()["hydra:member"]))
        else:
            print(r.status_code)
            print(r.text)
            print("Stopped on page: " + str(i))
            break
        if i % 50 == 0:
            print("Currently on page " + str(i) + ". Progress: " + str(round(i/(pages_total+1)*100,1)) + " %.", end='\r')
        if i % 100 == 0:
             df.to_parquet('data3.gzip'.format(a = i),compression='gzip')
    return df
data = downloader(token, start_page = 2012)