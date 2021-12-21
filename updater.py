import requests 
import pandas as pd
import numpy as np 
import math 
import time
from datetime import date
import fastparquet

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
    else:     
    # Call the function being decorated and return the result
    return func(*args, **kwargs)
  wrapper.count = 0
  # Return the new decorated function
  return wrapper


@counter
def get_data(token):
    df = pd.DataFrame()
    i = 1
    start_date = data.iloc[-1].datum.strftime("%d.%m.%Y")
    end_date = date.today().strftime("%d.%m.%Y")
    while True:
        url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page=" + str(i) +"&datum%5Bbefore%5D=01.01.2021&datum%5Bafter%5D=01.01.2020"
        r = requests.get(url, params = {"apiToken": token})
        if r.status_code == 429:
            print("code 429: " + str(i))
            msg = r.json()["message"]
            t = "".join(a for a in msg if a.isdigit())
            print(t)
            break
            #time.sleep(int(t))
            #r = requests.get(url, params = {"apiToken": token})
        if len(r.json()["hydra:member"]) != 0:
            df = df.append(pd.DataFrame.from_dict(r.json()["hydra:member"]))
        else:
            print(i)
            break
        i += 1
    return df

df.to_parquet('df.parquet.gzip',compression='gzip') 