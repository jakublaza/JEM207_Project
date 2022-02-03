import unittest
import pandas as pd
from app.downloader import get_total_pages, get_total_items, request

token = "8a0ff681501b0bac557bf90fe6a036f7"

class TestDownloader(unittest.TestCase): #inheriting from unittest.TestCase
    
    def test_get_total_pages(self):
        total_pages = get_total_pages(token, start_date = "1.1.2020", end_date = "24.12.2021")
        self.assertEqual(total_pages, 490)
    
    def test_get_total_items(self):
        total_items = get_total_items(token, start_date = "1.1.2020", end_date = "24.12.2021")
        self.assertEqual(total_items, 2446137)

    def test_request(self):
        r = request(token,items_per_page = 5000, start_date = "1.1.2020", end_date = "24.12.2021", pause = 0.1)
        df = pd.DataFrame.from_dict(r.json()["hydra:member"])

        self.assertEqual(200, r.status_code)
        self.assertEqual(5000, len(df))
        self.assertEqual(11 ,len(df.columns))

    

if __name__ == "__main__":
    unittest.main()