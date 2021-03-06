# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class EventspiderItem(Item):
    # we can put one variable to an item
    event_title = Field()
    event_place = Field()
    opening_ceremony = Field()
    closing_ceremony = Field()
    n_participants = Field()
    n_countries = Field()
    n_medals = Field()
    n_disciplines = Field()
    
    # or a list, dict
    medals_per_country = Field()

    disciplines_details = Field()
   
