import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import Selector
from eventSpider.items import EventspiderItem

class EventsSpider(scrapy.Spider):
    name = 'eventSpider'
    
    # base url to link to the end url we receive
    baseUrl = "http://www.olympedia.org"
    
    def start_requests(self):
        start_urls = [
            'http://www.olympedia.org/editions'
        ]
        
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse_urls)
            
    def parse_urls(self, response):
        """
        Go through the table of owinter olympics
        Get all the url to those olympics events
        Send the urls down to parse items to get the items of interest
        """        
        for tr in response.xpath("//table[2]//tr")[1:-2]: # first one is an empty list, and the last 2 events haven't happened
            url = tr.xpath('td[1]//a//@href').extract()
            # check if we get empty list
            if not url:
                pass
            else:
                url = self.baseUrl + url[0]
                yield scrapy.Request(url=url, callback=self.parse_items)
            
    def parse_items(self, response):
        """
        Get the items of interest
        Extract the list of disciplines and their url
        pass the url 
        """
        item = EventspiderItem()
        selector = Selector(response)
        table1_rows = selector.xpath("//table[1]//tr")
        item['event_title'] = table1_rows[1].xpath('td//text()').extract()
        item['event_place'] = table1_rows[2].xpath('td//text()').extract()
        item['opening_ceremony'] = table1_rows[3].xpath('td//text()').extract()
        item['closing_ceremony']= table1_rows[4].xpath('td//text()').extract()
        item['n_participants'] = table1_rows[7].xpath('td//a[1]//text()').extract()
        item['n_countries'] = table1_rows[7].xpath('td//a[2]//text()').extract()
        item['n_medals'] = table1_rows[8].xpath('td//text()').extract()
        item['n_disciplines'] = table1_rows[8].xpath('td//text()').extract()
        # TODO: sparse the data out
        
        table2 = selector.xpath("//table[3]//tr")
        disciplines = []
    
        for tr in table2:
            disciplines.append(tr.xpath('td//a//text()').extract())
            
            url = tr.xpath('td//a//@href').extract()
            # check if we get empty list
            if not url:
                pass
            else:
                url = self.baseUrl + url[0]
                yield scrapy.Request(url=url, callback=self.parse_sports, meta={'event_item': item})
            
        
        #flattening the list
        # TODO: move this process to pipeline
        item['disciplines_names'] = [item for sublist in disciplines for item in sublist]
        
        table3 = selector.xpath("//table[4]//tr")
        medal_dict = dict()
        for tr in table3[1:]:
            #name of country
            NOC = tr.xpath('td[1]//a//text()').extract_first()
            gold_medal = tr.xpath('td[3]//text()').extract_first()
            silver_medal = tr.xpath('td[4]//text()').extract_first()
            bronze_medal = tr.xpath('td[5]//text()').extract_first()
            total_medal = tr.xpath('td[6]//text()').extract_first()
            
            # store in dict so we can more easily copy this to database later
            medal_dict[NOC] = {'gold_medal': gold_medal,
                                'silver_medal': silver_medal,
                                'bronze_medal': bronze_medal,
                                'total_medal': total_medal}
    
        item['medals_per_country'] = medal_dict
        
    def parse_sports(self, response):
        selector = Selector(response)
        item = response.meta.get('event_item')
        
        sport_dict = dict()
        # category name to use as dict key
        category_name = selector.css('h1::text').get()
        print(category_name)
        
        
        
  

        