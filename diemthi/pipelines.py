# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from sqlite3 import dbapi2 as sqlite


class DiemthiPipeline:
    def __init__(self):
        self.connection = sqlite.connect('./diem.db')
        self.cursor = self.connection.cursor()
        self.cursor.execute('CREATE TABLE IF NOT EXISTS diem '
                            '(sbd TEXT PRIMARY KEY, toan REAL, ngu_van REAL, ngoai_ngu REAL, '
                            'vat_li REAL, hoa_hoc REAL, sinh_hoc REAL, lich_su REAL, dia_li REAL, gdcd REAL)')
                            
    def process_item(self, item, spider):
        self.cursor.execute("select * from diem where sbd=(?)", [item[0]])
        result = self.cursor.fetchone()
        if not result:
            self.cursor.execute(
                "insert into diem (sbd, toan, ngu_van, ngoai_ngu, vat_li, hoa_hoc, sinh_hoc, lich_su, dia_li, gdcd) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [item[0], item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9]])
            self.connection.commit()
        return item
