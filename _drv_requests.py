#!/venv/bin/python3
'''
This class is used to scrap web pages by fetching content using proxies and user agents for anonymity.
It provides methods for retrieving HTML from URLs (fetch_response_text) and saving parsed HTML (save_soup_as_html),
handling errors effectively.

v.2023-08-24 - initial version
'''

import requests
import time
import random
from bs4 import BeautifulSoup
from fake_useragent import UserAgent


class CustomWebScraper:

    def __init__(self, proxy_list=None):
        self.proxy_list = proxy_list or [
            'http://196.51.132.133:8800',
            'http://196.51.129.230:8800',
            'http://196.51.135.162:8800',
            'http://196.51.129.252:8800'
        ]

        self.ua = UserAgent(
            browsers=['edge', 'chrome', 'firefox'],
            fallback='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:116.0) Gecko/20100101 Firefox/116.0'
        )

    def fetch_response_text(self, url, retries=3):
        for _ in range(retries):
            proxy = random.choice(self.proxy_list)
            proxies = {
                'http': proxy,
                'https': proxy
            }

            try:
                response = requests.get(url, headers={'User-Agent': self.ua.random}, proxies=proxies, timeout=5)
                response.raise_for_status()  # Raise an exception for HTTP error status codes

                return BeautifulSoup(response.text, 'html.parser')

            except requests.RequestException as e:
                print(f"ERROR - {e}")
                time.sleep(3)

        print(f"ERROR - Requests failed, unable to fetch {url}.")
        return None

    def save_soup_as_html(self, soup, filename):
        try:
            if soup:
                with open(filename, 'w', encoding='utf-8') as file:
                    file.write(soup.prettify())
                print(f"INFO  - HTML content saved as '{filename}'")
        except Exception as e:
            print(f"ERROR - Unable to save HTML content: {e}")


def test():
    url_to_scrape = "https://www.scrapethissite.com/pages/simple/"
    scraper = CustomWebScraper()

    try:
        soup = scraper.fetch_response_text(url_to_scrape)
        print(f"INFO  - Success fetching response from '{url_to_scrape}'")
    except Exception as e:
        print(f"ERROR - Unable to fetch response: {e}")
        return

    if soup:
        try:
            scraper.save_soup_as_html(soup, "soup_test.html")
        except Exception as e:
            print(f"ERROR - Unable to save HTML content: {e}")


if __name__ == '__main__':
    test()


"""
#!/venv/bin/python3

import requests
import time

from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from random import randint


def test():

    url_to_scrape = "https://www.scrapethissite.com/pages/simple/"

    scraper = CustomWebScraper()
    soup = scraper.fetch_text_content(url_to_scrape)

    if soup:
        scraper.save_soup_as_html(soup, "soup_test.html")


class CustomWebScraper:

    def __init__(self, proxy_list=None):

        self.proxy_list = proxy_list or ['http://196.51.132.133:8800',
                                         'http://196.51.129.230:8800',
                                         'http://196.51.135.162:8800',
                                         'http://196.51.129.252:8800']

        self.ua = UserAgent(
            browsers=['edge', 'chrome', 'firefox'],
            fallback='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:116.0) Gecko/20100101 Firefox/116.0'
        )

    def fetch_response_text(self, url, retries=3):
        # Request page with user-agent and proxies for response.TEXT

        for _ in range(retries):
            proxy_index = randint(0, len(self.proxy_list) - 1)  # random index to fetch proxy from list
            proxies = {
                'http': self.proxy_list[proxy_index],
                'https': self.proxy_list[proxy_index],
            }

            try:
                # print(f"INFO  - Request using {headers}, {proxies}")
                response = requests.get(url, headers={'User-Agent': self.ua.random}, proxies=proxies, timeout=5)

                if response.status_code == 200:
                    # print(f"INFO  - Request successful for {url}")
                    return BeautifulSoup(response.text, 'html.parser')
                elif response.status_code in [408, 504]:
                    print("INFO  - Timeout, retrying in 3 seconds...")
                else:
                    print(f"ERROR - Request for {url} failed with status code: {response.status_code}")
                    break

            except requests.RequestException as e:
                print(f"ERROR - {e}")
                time.sleep(3)

        print(f"ERROR - Requests failed, unable to fetch {url}.")
        return None

    def save_soup_as_html(self, soup, filename):
        if soup:
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(soup.prettify())
            print(f"INFO  - HTML content saved as '{filename}'")


if __name__ == '__main__':
    test()
 """
