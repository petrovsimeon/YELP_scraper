import scrapy
from scrapy import Request


class PediatriciansScraperSpider(scrapy.Spider):
    name = 'pediatricians_scraper'
    allowed_domains = ['yelp.com']
    start_urls = ['https://www.yelp.com/']
    target_postcodes = ["Sacramento County",
                        "Sacramento",
                        "Antelope 95843",
                        "Foothill Farms 95842",
                        "Natomas 95833",
                        "Natomas 95834",
                        "North Sac 95838",
                        "Gardenland 95815",
                        "Arden-Arcade 95815",
                        "Arden-Arcade 95821",
                        "Arden-Arcade 95825",
                        "Arden-Arcade 95864",
                        "Arden-Arcade 95841",
                        "Mid/Downtown 95811",
                        "Mid/Downtown 95812",
                        "Mid/Downtown 95814",
                        "Mid/Downtown 95816",
                        "East Sac 95816",
                        "East Sac 95819",
                        "Rosemont 95826",
                        "Rosemont 95827",
                        "Tahoe Park 95817",
                        "Tahoe Park 95820",
                        "Tahoe Park 95826",
                        "Land Park 95818",
                        "Curtis Park 95818",
                        "Meadowview 95822",
                        "Florin 95820",
                        "Florin 95824",
                        "Florin 95828",
                        "Vineyard 95829",
                        "Vineyard 95830",
                        "Laguna 95832",
                        "Laguna 95823",
                        "Pocket 95831",
                        "TBD 95832",
                        "TBD 95835",
                        "TBD 95836",
                        "TBD (95837",
                        "Elk Grove 95624",
                        "Elk Grove 95757",
                        "Elk Grove 95758",
                        "Elverta 95626",
                        "Carmichael 95608",
                        "Citrus Heights 95610",
                        "Citrus Heights 95621",
                        "Folsom 95630",
                        "Folsom 95762",
                        "Rancho Cordova 95655",
                        "Rancho Cordova 95670",
                        "Rancho Cordova 95742",
                        "North Highlands 95660",
                        "Fair Oaks 95628",
                        "Mather 95655",
                        "Mcclellan 95652",
                        "Orangevale 95662",
                        "Placer County",
                        "Roseville 95661",
                        "Roseville 95678",
                        "Roseville  95746",
                        "Roseville 95747",
                        "Rocklin  95677",
                        "Rocklin  95765",
                        "Loomis 95650",
                        "Yolo County",
                        "West Sacramento 95691",
                        ]

    # Searching for each target postcode
    def start_requests(self):
        for location in self.target_postcodes[:]:
            search_url = f"https://www.yelp.com/search?find_desc=Pediatrician+&find_loc={location}&ns=1"
            yield Request(search_url, callback=self.parse)

    # Parsing doctors' profiles links
    def parse(self, response):
        doctor_profiles = response.xpath("//div[contains(@class,'businessName')]/div/h3/span/a/@href").extract()

        for doctor_profile in doctor_profiles:
            profile_link = 'https://www.yelp.com' + doctor_profile

            yield Request(profile_link, callback=self.parse_details, meta={'URL': profile_link})

        absolute_next_url = response.xpath("//a[contains(@class,'next-link')]/@href").extract_first()

        if absolute_next_url:
            yield Request(absolute_next_url, callback=self.parse)

    # Parsing doctors' details
    def parse_details(self, response):
        pediatrician_name = response.xpath('normalize-space(//h1/text())').extract_first()
        yelp_profile = response.meta.get('URL')
        title = ""
        practice_affiliation = ""
        website_link = response.xpath(
            "//p[contains(text(),'Business website')]/following-sibling::p/a/@href").extract_first()

        if website_link:
            website = 'https://www.yelp.co.uk' + str(
                response.xpath("//p[contains(text(),'Business website')]/following-sibling::p/a/@href").extract_first())

        else:
            website = 'N/a'

        email = ""
        phone = response.xpath("//p[contains(text(),'Phone number')]/following-sibling::p/text()").extract_first()
        address = ' '.join(response.xpath("//address//text()").extract())

        if pediatrician_name == '':
            self.logger.info('CHECKING THIS LINK AGAIN!!!')
            yield Request(yelp_profile, callback=self.parse_details, meta={'URL': yelp_profile})

        else:
            yield {'Name': pediatrician_name,
                   "Title": title,
                   "Practice affiliation": practice_affiliation,
                   'Address': address,
                   'Phone': phone,
                   'Website': website,
                   "Email": email,
                   'YELP Profile': yelp_profile,
                   }