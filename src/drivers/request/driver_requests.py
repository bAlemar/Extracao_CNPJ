import requests

class Requests:
    def __init__(self) -> None:
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        pass

    def get(self,url):
        response = requests.get(url,
                                headers=self.headers
                                )
        return response
    
    def get_with_cookies(self,url):
        response = requests.get(url)
        cookies = response.cookies
        response_final = requests.get(url,
                                      cookies=cookies,
                                      )
        return response_final



    