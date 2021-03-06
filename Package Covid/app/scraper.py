import requests
import pandas as pd
from bs4 import BeautifulSoup
import unicodedata


def get_district_pop():
    """
    Scrapes czech statistical office for districts popultaion (Total, Men, Woman)
    """
    url = "https://vdb.czso.cz/vdbvo2/faces/index.jsf?page=vystup-objekt&z=T&f=TABULKA&katalog=33115&pvo=DEMD130062-1-4&c=v3~11__RP2021QP1"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")
    body = soup.find("tbody")
    tr = body.find_all("tr")
    f = {}
    for i in range(len(tr)):
        legend = tr[i].find("td", attrs = {"class": "LEGENDA"}).find("span").text
        td = tr[i].find_all("td")
        values = td[7:10]
        values = [el.find("span").text for el in values]
        values = [unicodedata.normalize("NFKD", val).replace(" ", "") for val in values]
        f[legend] = values
        
    df = pd.DataFrame(f).transpose()
    df.columns = ['Total', 'Men', 'Women']

    #data = districts.merge(df, left_on = "NAMN", right_index = True, how = "left")
    
    return df

