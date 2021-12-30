# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re
import sqlite3
import time
import datetime
import sys


class EventspiderPipeline:
    def __init__(self):
        self.con = sqlite3.connect('olympic.db')
        self.cur = self.con.cursor()
        self.sql_create_event_table = """
        CREATE TABLE IF NOT EXISTS events (
            event_title TEXT PRIMARY KEY,
            event_place TEXT,
            opening_ceremony TEXT,
            closing_ceremony TEXT,
            n_participants int,
            n_countries int,
            n_medals int,
            n_disciplines int
        );
        """
        
        self.sql_create_medal_table = """ 
        CREATE TABLE IF NOT EXISTS medals (
            event_title TEXT,
            country TEXT,
            gold int,
            silver int,
            bronze int,
            total int
        );
        """
        
        self.sql_create_discipline_table = """
        CREATE TABLE IF NOT EXISTS discipline (
            event_title TEXT,
            discipline TEXT,
            category TEXT,
            date TEXT,
            n_participants int,
            n_country_participants int,
            gold_medalist TEXT,
            gold_country TEXT,
            silver_medalist TEXT,
            silver_country TEXT,
            bronze_medalist TEXT,
            bronze_country TEXT
        );
        """
        
        
        self.create_table(self.sql_create_event_table)
        self.create_table(self.sql_create_medal_table)
        self.create_table(self.sql_create_discipline_table)

    


    def create_table(self, create_table):
        
        self.cur.execute(create_table)
        self.con.commit()
       
        
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        items = ['opening_ceremony', 'closing_ceremony', 'n_medals', 'event_place']
        for element in items:
            adapter[element] = adapter[element].strip()
        
        
        adapter['event_place'] = adapter['event_place'].strip("\n(")
        
        f = re.findall("\d+", adapter['n_medals'], re.I)
        adapter['n_medals'] = f[0]
        
        f = re.findall("\d+", adapter['n_disciplines'], re.I)
        adapter['n_disciplines'] = f[1]
        
        
        
        
        self.cur.execute("""
                         INSERT INTO events (event_title, event_place, opening_ceremony, closing_ceremony, n_participants, n_countries, n_medals, n_disciplines)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                         """, (item['event_title'], item['event_place'], item['opening_ceremony'], item['closing_ceremony'], item['n_participants'], item['n_countries'], item['n_medals'], item['n_disciplines']))
        self.con.commit()
        
            
        for key in item['medals_per_country'].keys():
           
            self.cur.execute("""
                             INSERT INTO medals(event_title, country, gold, silver, bronze, total) 
                             VALUES (?, ?, ?, ?, ?, ?)
                             """, (item['event_title'], key, item['medals_per_country'][key]['gold_medal'], item['medals_per_country'][key]['silver_medal'], item['medals_per_country'][key]['bronze_medal'], item['medals_per_country'][key]['total_medal']))
        self.con.commit()
        
        
        for dis in item['disciplines_details'].keys():
            for category in item['disciplines_details'][dis].keys():
           
                self.cur.execute("""
                                INSERT INTO discipline(event_title, discipline, category, date, n_participants, n_country_participants, gold_medalist, gold_country, silver_medalist, silver_country, bronze_medalist, bronze_country) 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (item['event_title'], dis, category, item['disciplines_details'][dis][category]['date'], item['disciplines_details'][dis][category]['participants'], item['disciplines_details'][dis][category]['n_country_participate'], item['disciplines_details'][dis][category]['gold_medalist'], item['disciplines_details'][dis][category]['gold_country'], item['disciplines_details'][dis][category]['silver_medalist'], 
                                      item['disciplines_details'][dis][category]['silver_country'], item['disciplines_details'][dis][category]['bronze_medalist'], item['disciplines_details'][dis][category]['bronze_country']))
        self.con.commit()
        
        
            
        return item
