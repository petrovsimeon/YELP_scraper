# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request

# Input files and variables needed for counting
venue_types = "Sport Bars, pubs, bowling, Billiards, snooker, family entertainment centers".split(', ')
cities = "Bristol, Glasgow, Leicester, Edinburgh, Leeds".split(', ')
#London, Birmingham, Liverpool, Nottingham, Sheffield, Bristol, Glasgow, Leicester, Edinburgh, Leeds
counter = 0
checked_counter = 0


class YelpScraperSpider(scrapy.Spider):
    name = "yelp_scraper"
    allowed_domains = ["yelp.co.uk"]
    start_urls = ['https://www.yelp.co.uk']

    def start_requests(self):
        global checked_counter, venue_types, cities

        for city in cities:
            for venue_type in venue_types:
                search_url = f'https://www.yelp.co.uk/search?find_desc={venue_type}&find_loc={city}&attrs=RestaurantsPriceRange2.2' \
                             '%2CRestaurantsPriceRange2.3%2CRestaurantsPriceRange2.4 '

                yield Request(search_url, self.parse_page, dont_filter=True)
                checked_counter += 1
                self.logger.info('Cities checked: ' + str(checked_counter))

    def parse_page(self, response):
        global counter
        company_profiles = response.xpath("//div[contains(@class,'businessName')]/div/h4/span/a/@href").extract()

        for company_profile in company_profiles:
            company_profile = 'https://www.yelp.co.uk' + company_profile

            yield Request(company_profile, callback=self.parse_item, meta={'URL': company_profile})

        absolute_next_url = response.xpath("//a[contains(@class,'next-link')]/@href").extract_first()

        if absolute_next_url:
            yield Request(absolute_next_url, callback=self.parse_page)

    def parse_item(self, response):
        global counter

        company_name = response.xpath('normalize-space(//h1/text())').extract_first()
        address = ' '.join(response.xpath("//address//text()").extract())
        phone = response.xpath("//p[contains(text(),'Phone number')]/following-sibling::p/text()").extract_first()
        website_link = response.xpath("//p[contains(text(),'Business website')]/following-sibling::p/a/@href").extract_first()

        if website_link:
            website = 'https://www.yelp.co.uk' + str(response.xpath("//p[contains(text(),'Business website')]/following-sibling::p/a/@href").extract_first())

        else:
            website = 'N/a'

        yelp_profile = response.meta.get('URL')
        rating_stars = response.xpath("//div[contains(@aria-label, 'star rating')]/@aria-label").extract_first()
        reviews = response.xpath("//span[contains(text(),'reviews')]/text()").extract_first()
        prices = response.xpath("//span[contains(text(),'£')]/text()").extract_first()
        categories = ', '.join(response.xpath("//a[contains(@href,'/c/')]/text()").extract())

        if company_name == '':
            self.logger.info('CHECKING THIS LINK AGAIN!!!')
            yield Request(yelp_profile, callback=self.parse_item, meta={'URL': yelp_profile})

        else:
            yield {'Company': company_name,
                   'Address': address,
                   'Phone': phone,
                   'Website': website,
                   'YELP Profile': yelp_profile,
                   'Rating - stars': rating_stars,
                   'Number of reviews': reviews,
                   'Prices': prices,
                   'Venue type': categories,
                   }
            counter += 1
            self.logger.info('Companies found: ' + str(counter))
