# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import sqlalchemy as sa

TABLE = '''
    CREATE TABLE IF NOT EXISTS books (
        name  TEXT,
        price DOUBLE PRECISION,
        url   TEXT
    )
'''

INSERT = '''
    INSERT INTO books (
        name,
        price,
        url
    )
    VALUES (
        :name,
        :price,
        :url
    )
'''

class TestPipeline:
    def open_spider(self, spider):
        host = spider.settings.get('POSTGRES_HOST')
        self.engine = sa.create_engine(host)
        self.conn = self.engine.connect()
        self.conn.execute(sa.text(TABLE))

    def close_spider(self, spider):
        self.conn.commit()
        self.conn.close()

    def process_item(self, item, spider):
        val = spider.settings.get('MY_CUSTOM_VALUE')
        print('nice:', val)
        self.conn.execute(sa.text(INSERT), item)
        return item
