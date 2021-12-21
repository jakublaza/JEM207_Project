
def counter(func):
  def wrapper(*args, **kwargs):
    wrapper.count += 1
    if wrapper.count == 999:
        print("Counter on 999!")
        time.sleep(3600)
        wrapper.count = 0
        return func(*args, **kwargs)
    else:     
    # Call the function being decorated and return the result
        return func(*args, **kwargs)
  wrapper.count = 0
  # Return the new decorated function
  return wrapper

token = "8a0ff681501b0bac557bf90fe6a036f7"
@counter
def request(url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page=1&datum%5Bbefore%5D=01.01.2021&datum%5Bafter%5D=01.01.2020" , params =  {"apiToken": token}):
            r = requests.get(url, params)
            return r 
r = request(url, params)


def get_data(token):
    df = pd.DataFrame()
    i = 1
    while True:
        url = "https://onemocneni-aktualne.mzcr.cz/api/v3/osoby?page=" + str(i) +"&datum%5Bbefore%5D=01.01.2021&datum%5Bafter%5D=01.01.2020"
        r = request()
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