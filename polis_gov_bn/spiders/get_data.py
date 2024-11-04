import html
import re
from datetime import datetime
from typing import Iterable, Union
from urllib.parse import urlparse, parse_qs

import pandas as pd
import scrapy
from deep_translator import GoogleTranslator
from scrapy import Request, Spider
from scrapy.cmdline import execute
from twisted.internet.defer import Deferred


class GetDataSpider(scrapy.Spider):
    name = "get_data"

    def __init__(self):
        super().__init__()
        self.data_list = []
        self.headers = None
        self.cookies = None

    def start_requests(self):
        self.cookies = {
            '_gid': 'GA1.3.442269817.1730086766',
            'WSS_FullScreenMode': 'false',
            '_ga_Z49F57J3T2': 'GS1.1.1730086765.2.1.1730087300.0.0.0',
            '_ga': 'GA1.1.641131861.1729660951',
        }

        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            # 'cookie': '_gid=GA1.3.442269817.1730086766; WSS_FullScreenMode=false; _ga_Z49F57J3T2=GS1.1.1730086765.2.1.1730087300.0.0.0; _ga=GA1.1.641131861.1729660951',
            'pragma': 'no-cache',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        }
        # for i in range(65, 91):
        # yield scrapy.Request(
        #     url=f"https://www.polis.gov.bn/wp/{chr(i)}.aspx",
        #     headers=self.headers,
        #     cookies=self.cookies
        # )
        yield scrapy.FormRequest(
            url="https://www.polis.gov.bn/SitePages/Orang%20Dikehendaki.aspx",
            headers=self.headers,
            cookies=self.cookies,
            callback=self.parse,
            cb_kwargs={'count': 0}
        )

    def parse(self, response, **kwargs):
        base_url = "https://www.polis.gov.bn"
        links = response.xpath("//a[contains(@href,'/Lists/Wanted Persons')]/@href").getall()
        print(response.url, len(links))
        if links:
            for link in links:
                yield scrapy.Request(
                    url=base_url + link,
                    headers=self.headers,
                    cookies=self.cookies,
                    callback=self.parse_profile
                )
            last_id = self.extract_id_from_url(links[-1])
            row_count = kwargs['count'] + len(links) + 1
            data = {
                "__EVENTTARGET": "ctl00$PlaceHolderMain$g_b1d68b62_2e47_43e8_b9ff_fa0fb413c861",
                "__EVENTARGUMENT": "dvt_firstrow={%s};dvt_startposition={Paged=TRUE&p_ID=%s}" % (row_count, last_id),
            }
            yield scrapy.FormRequest(
                url="https://www.polis.gov.bn/SitePages/Orang%20Dikehendaki.aspx",
                formdata=data,
                callback=self.parse,
                cb_kwargs={'count': kwargs['count'] * 2}
            )

    def extract_id_from_url(self, url):
        # Parse the URL to get its components
        parsed_url = urlparse(url)
        # Parse the query parameters from the URL
        query_params = parse_qs(parsed_url.query)
        # Get the 'ID' parameter, if present
        wanted_id = query_params.get('ID', [None])[0]
        return wanted_id

    def parse_profile(self, response):
        # Initialize an empty dictionary to store the extracted data
        data = {}
        data['url'] = response.url
        # Extract all rows where the label and value are present
        rows = response.xpath('//tr[td[@class="ms-formlabel"] and td[@class="ms-formbody"]]')

        for row in rows:
            # Extract the label text (key), and normalize it by removing whitespace
            key = row.xpath('.//td[@class="ms-formlabel"]/h3/nobr/text()').get('').strip()

            # Extract the corresponding value text
            value = row.xpath('.//td[@class="ms-formbody"]//text()').get('').strip()

            # Add the key-value pair to the dictionary
            if key:
                data[key] = value

        # Extract additional data like image URL and description
        data['image_url'] = response.xpath('//td[@class="ms-formbody"]/img/@src').get()
        description_paragraphs = response.xpath(
            '//div[contains(@class, "ExternalClass")]/p//text()').getall()
        data['description'] = self.clean_desc(' '.join(description_paragraphs).strip())
        self.data_list.append(data)

    def clean_desc(self, text):
        # Remove leading/trailing whitespace and hyphens
        cleaned_text = text.encode('ascii', 'ignore').decode('ascii')
        cleaned_text = cleaned_text.strip().strip('-')

        # Remove extra spaces
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text)
        return cleaned_text

    def translate_dataframe(self, df, exclude_columns):
        # Create a copy of the DataFrame to avoid modifying the original
        translated_df = df.copy()

        # Iterate through each column in the DataFrame
        for column in translated_df.columns:
            # Check if the column should be excluded from translation
            if column not in exclude_columns:
                # Translate the column to English
                try:
                    translated_df[column] = translated_df[column].apply(
                        lambda x: GoogleTranslator(source='ms', target='en').translate(x) if isinstance(x, str) else x
                    ).str.replace('.', '')
                except Exception as e:
                    print(f"Error translating column '{column}': {e}")

        translated_df.columns = [
            (lambda col: GoogleTranslator(source='ms', target='en').translate(col) if isinstance(col, str) else col)(
                column)
            for column in df.columns
        ]
        return translated_df

    def clean_df(self, df):
        df.columns = (
            df.columns
            .str.replace(' ', '_')
            .str.replace('.', '')
            .str.replace('/', '_')
            .str.replace('-', '_')
            .str.replace('\s+', ' ', regex=True)
            .str.strip()
            .str.lower()
        )
        # df['nama'].str.replace('.','').str.replace('-',' ')
        # df['umur'].str.replace('tahun', '')
        df.fillna('N/A', inplace=True)
        # Replace empty strings or blanks with 'N/A'
        df.replace(to_replace=r'^\s*$', value='N/A', regex=True, inplace=True)
        df.replace(to_replace=r'-', value='N/A', regex=True, inplace=True)
        return df

    def close(self, spider: Spider, reason: str):
        df = pd.DataFrame(self.data_list)
        df = self.clean_df(df)
        df.to_excel(f"../files/polis_gov_bn_native_{datetime.today().strftime('%Y%m%d')}.xlsx", index=False)
        trans_df = self.translate_dataframe(df, ['url', 'nama', 'image_url'])
        trans_df.to_excel(f"../files/polis_gov_bn_english_{datetime.today().strftime('%Y%m%d')}.xlsx", index=False)


if __name__ == '__main__':
    execute(f'scrapy crawl {GetDataSpider.name}'.split())
