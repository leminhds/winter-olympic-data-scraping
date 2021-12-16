# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import re

class EventspiderPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        items = ['opening_ceremony', 'closing_ceremony', 'n_disciplines', 'n_medals', 'event_place']
        for element in items:
            adapter[element] = adapter[element].strip()
        
        
        adapter['event_place'] = adapter['event_place'].strip("\n(")
        
        f = re.findall("\d+", adapter['n_medals'], re.I)
        adapter['n_medals'] = f[0]
        
        f = re.findall("\d+", adapter['n_disciplines'], re.I)
        adapter['n_disciplines'] = f[1]
        
        print(f)
        
        
        return item
