import scrapy
import json
import requests
from bs4 import BeautifulSoup
from sqlite3 import dbapi2 as sqlite
        
class DiemThiSpider(scrapy.Spider):  
    name = "diemthi"
    pattern_1 = "https://tienphong.vn/api/diemthi/get/result?keyword={:02d}0*"
    pattern_2 = "https://tienphong.vn/api/diemthi/get/result?keyword={:02d}{:04d}*"
    
    def __init__(self):
        self.connection = sqlite.connect('././diem.db')
        self.cursor = self.connection.cursor()
                            
    def start_requests(self):
        urls = []
        for province in range (0, 99):
            # Get last id each province
            contents = requests.get(self.pattern_1.format(province)).text
            js = json.loads(contents)
            if js['error_code'] == 0:
                results = js['data']['results']
                soup = BeautifulSoup(results, 'html.parser')
                end_id = soup.find('td')
                if end_id != None:
                    last_group = int(end_id.string)//100%10000
                    for group in range(last_group, last_group+1):
                        result = self.cursor.execute("select * from diem where sbd like '{:02d}{:04d}%'".format(province,group)).fetchall()
                        if (group != last_group and len(result) != 100) or (group == last_group):
                            url = self.pattern_2.format(province,group)
                            yield scrapy.Request(url=url, callback=self.parse, meta={'length': len(result)})

    def parse(self, response):
        js = json.loads(response.body)
        length = response.meta.get('length')
        if js['error_code'] == 0:
            results = js['data']['results']
            soup = BeautifulSoup(results, 'html.parser')
            trs = soup.find_all('tr')
            if length == len(trs):
                return
            for tr in trs:
                data = {}
                index = 0
                for td in tr.find_all('td'):
                    if td.string == None:
                        data[index] = None
                    elif len(td.string) == 8:
                        data[index] = td.string
                    else:
                        data[index] = float(td.string)
                    index += 1
                yield data