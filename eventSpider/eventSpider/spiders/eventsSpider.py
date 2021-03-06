import scrapy
from scrapy import Selector
from eventSpider.items import EventspiderItem
import urllib.parse
import time

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
        
        discipline_list = []
        url_list = []
    
        for tr in table2:            
            urls = tr.xpath('td//a//@href').extract()
            disciplines = tr.xpath('td//a//text()').extract()
            
            for url in urls:
                url_list.append(url)   
            for discipline in disciplines:
                discipline_list.append(discipline)
                  

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
        item['disciplines_details'] = {}

        if url_list:
            final_url = urllib.parse.urljoin(self.baseUrl, url_list[0])
            item['disciplines_details'][discipline_list[0]] = {}
            yield scrapy.Request(url=final_url, callback=self.parse_sports, meta={'event_item': item, 'url_list': url_list[1:], 'discipline_list': discipline_list[1:], 'current_discipline': discipline_list[0]})
 
        
    def parse_sports(self, response):
        """
        In order to pass everything as 1 item later, we will create 1 dictionary per discipline
        For each disciplines, we will have different values which represent each category.
        These categories act as the key for further details about it such as category name, date,
        name of winner, country of winner, etc.
        """
        selector = Selector(response)
        item = response.meta.get('event_item')
        discipline_list = response.meta.get('discipline_list')
        discipline = response.meta.get('current_discipline')
        url_list = response.meta.get('url_list')
        
        table = selector.xpath("//table[2]//tr")
     
        # category list to keep track of category name
        category_list = []
        
        """
        For some reason, the list returned below always start and end with an empty list
        we will therefore always exclude these. That's why we use [1:-1]
        """
        for tr in table[1:-1]:
            category = tr.xpath("td[1]//a//text()").extract_first()
            
            # in this table and the medal_table, sometime to category name is not the same
            # despite referring to the same sport. therefore, we will keep track of all the category
            category_list.append(category)
            
            date = tr.xpath("td[3]//text()").extract_first()
            participants = tr.xpath("td[4]//text()").extract_first()
            # number of country in the category
            nocs = tr.xpath("td[5]//text()").extract_first()
            
            
       
            item['disciplines_details'][discipline][category] = {}

            item['disciplines_details'][discipline][category].update({'date': date})
            item['disciplines_details'][discipline][category].update({'participants': participants})
            item['disciplines_details'][discipline][category].update({'n_country_participate': nocs})
                
  
        
        # in some case, Non medal category appear as well. we will skip those
        if selector.xpath("//h2[2]//text()").extract_first() == 'Non-medal events':
            medal_table = selector.xpath("//table[4]//tr")
        else:
            medal_table = selector.xpath("//table[3]//tr")
    
        
        i = 0
        # only the first 1 is always none here
        for tr in medal_table[1:]:
            category = category_list[i]
            i += 1
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
            
                
            
            item['disciplines_details'][discipline][category].update({'gold_medalist': gold_name})
            item['disciplines_details'][discipline][category].update({'gold_country': gold_country})
            item['disciplines_details'][discipline][category].update({'silver_medalist': silver_name})
            item['disciplines_details'][discipline][category].update({'silver_country': silver_country})
            item['disciplines_details'][discipline][category].update({'bronze_medalist': bronze_name})
            item['disciplines_details'][discipline][category].update({'bronze_country': bronze_country})
        
        # if there is still url in the list, we wait to go through all the url before yielding the item
        if url_list:
            final_url = urllib.parse.urljoin(self.baseUrl, url_list[0])
            item['disciplines_details'][discipline_list[0]] = {}
            yield scrapy.Request(url=final_url, callback=self.parse_sports, meta={'event_item': item, 'url_list': url_list[1:], 'discipline_list': discipline_list[1:], 'current_discipline': discipline_list[0]})
        else:
            yield item
            
    
        
      
        
        
  

        