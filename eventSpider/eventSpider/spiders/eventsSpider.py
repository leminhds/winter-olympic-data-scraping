import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import Selector
from eventSpider.items import EventspiderItem
import urllib.parse

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
        # remove the last 2 as the events haven't happened yet
        for tr in response.xpath("//table[2]//tr")[:-2]:
            url = tr.xpath('td[1]//a//@href').extract_first()
            # check for None. In this case, we elimiate the 2 events that was canelled
            if url is None:
                continue
            else:
                url_to_check = urllib.parse.urljoin(self.baseUrl, url)
                yield scrapy.Request(url=url_to_check, callback=self.parse_items)
            
    def parse_items(self, response):
        """
        Get the items of interest
        Extract the list of disciplines and their url
        pass the url 
        """
        item = EventspiderItem()
        selector = Selector(response)
        print(response.url)
        table1_rows = selector.xpath("//table[1]//tr")
        
        item['event_title'] = table1_rows[1].xpath('td//text()').extract_first()
        item['event_place'] = table1_rows[2].xpath('td//text()').extract_first()
        item['opening_ceremony'] = table1_rows[3].xpath('td//text()').extract_first()
        item['closing_ceremony']= table1_rows[4].xpath('td//text()').extract_first()
        
        #check if there is the column OCOG, if yes, skip 1 additional row
        if table1_rows[6].xpath('th//text()').extract_first() == 'OCOG':
            item['n_participants'] = table1_rows[7].xpath('td//a[1]//text()').extract_first()
            item['n_countries'] = table1_rows[7].xpath('td//a[2]//text()').extract_first()
            item['n_medals'] = table1_rows[8].xpath('td//text()').extract_first()
            item['n_disciplines'] = table1_rows[8].xpath('td//text()').extract_first()
        else:
            item['n_participants'] = table1_rows[6].xpath('td//a[1]//text()').extract_first()
            item['n_countries'] = table1_rows[6].xpath('td//a[2]//text()').extract_first()
            item['n_medals'] = table1_rows[7].xpath('td//text()').extract_first()
            item['n_disciplines'] = table1_rows[7].xpath('td//text()').extract_first()
        
        
        
        table2 = selector.xpath("//table[3]//tr")
        
        disciplines = []
        url_list = []
    
        for tr in table2:            
            url = tr.xpath('td//a//@href').extract()
            # check if we get empty list
            if not url:
                continue
            else:
                url_list.append(url)     
                disciplines.append(tr.xpath('td//a//text()').extract())
                
        #flattening the list
        disciplines = [item for sublist in disciplines for item in sublist]
        item['disciplines_details'] = disciplines
        # flatten the list
        url_list = [item for sublist in url_list for item in sublist]
        
        # some case there are also a table of "Other disciplines. This change the order of the
        # relevant table we want to get. check fortable name first here"
        if selector.xpath("//h2[6]//text()").extract_first() == 'Other Disciplines':
            table3 = selector.xpath("//table[5]//tr")
        else:
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
 
        
        sport_detail_dict = [] # dictionary to add all the sport and its winner of each category of each disciplines
        for i, url in enumerate(url_list):
            final_url = urllib.parse.urljoin(self.baseUrl, url)
            event_name = item['event_title'] + " " + disciplines[i]
            yield scrapy.Request(url=final_url, callback=self.parse_sports, meta={'event_item': item, 'discipline': event_name})
        
        
        
        
    def parse_sports(self, response):
        """
        In order to pass everything as 1 item later, we will create 1 dictionary per discipline
        For each disciplines, we will have different values which represent each category.
        These categories act as the key for further details about it such as category name, date,
        name of winner, country of winner, etc.
        """
        selector = Selector(response)
        item = response.meta.get('event_item')
        discipline = response.meta.get('discipline')
        
        sport_dict = dict()
        
        table = selector.xpath("//table[2]//tr")
     
        
        """
        For some reason, the list returned below always start and end with an empty list
        we will therefore always exclude these. That's why we use [1:-1]
        """
        for tr in table[1:-1]:
            category = tr.xpath("td[1]//a//text()").extract_first()
            date = tr.xpath("td[3]//text()").extract_first()
            participants = tr.xpath("td[4]//text()").extract_first()
            # number of country in the category
            nocs = tr.xpath("td[5]//text()").extract_first()
            
            sport_dict[category] = {'category': category,
                                    'date': date,
                                    'participants': participants,
                                    'n_country_participate': nocs}
            
        # medalist table
        medal_table = selector.xpath("//table[3]//tr")
        
    
        
        # only the first 1 is always none here
        for tr in medal_table[1:]:
            category = tr.xpath("td[1]//a//text()").extract_first()
            # in case of team sport, the structure of the xpath look different
            gold_name = tr.xpath("td[2]//a//text()").extract_first()
            if gold_name is None:
                gold_name = tr.xpath("td[2]//text()").extract_first()
            
            gold_country = tr.xpath("td[3]//a//text()").extract_first()
            
            silver_name = tr.xpath("td[4]//a//text()").extract_first()
            if silver_name is None:
                silver_name = tr.xpath("td[4]//text()").extract_first()
            
            silver_country = tr.xpath("td[5]//a//text()").extract_first()
            
            bronze_name = tr.xpath("td[6]//a//text()").extract_first()
            if bronze_name is None:
                bronze_name = tr.xpath("td[6]//text()").extract_first()
           
            bronze_country = tr.xpath("td[7]//a//text()").extract_first()
            
            # add these into the sport dictionary as well
            sport_dict[category] = {'gold_medalist': gold_name,
                                    'gold_country': gold_country,
                                    'silver_medalist': silver_name,
                                    'silver_country': silver_country,
                                    'bronze_medalist': bronze_name,
                                    'bronze_country': bronze_country}
        item['disciplines_details'] = sport_dict    
        
        print(item)
        return item
        
      
        
        
  

        