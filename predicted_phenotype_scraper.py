'Class for Scraping Phenotypes Data from IMG JGI'

from lxml import etree
import simplejson as json 
import os
import re
import requests
from requests.adapters import HTTPAdapter
import shutil
from urllib3.util import Retry

BASE_URL = "https://img.jgi.doe.gov/cgi-bin/m/main" 
SAMPLE_URL = "https://img.jgi.doe.gov/cgi-bin/m/main.cgi?section=ImgPwayBrowser&page=phenoRules"
MAX_URL_RETRIES = 25

class PredictedPhenotypeScraper(object):
    def __init__(self):
        '''Initializes session for scrapping utils
        '''
        self.session = requests.Session()
        self.retries = Retry(total=MAX_URL_RETRIES, backoff_factor=1, status_forcelist=[500,502,503,504])
        self.session.mount(SAMPLE_URL, HTTPAdapter(max_retries=self.retries))
        self.default_header = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/83.0.4103.61 Chrome/83.0.4103.61 Safari/537.36'} 

    def fetch_page(self, url, payload=None, verify=True):
        '''Fetches URL and return its textual content, in case of error returns empty text
        '''
        page_text = ''
        for retry_count in range(MAX_URL_RETRIES):
            try:
                if payload:
                    response = self.session.post(url, payload, headers=self.default_header, verify=verify, stream=True)
                else:
                    response = self.session.get(url, headers=self.default_header, verify=verify, stream=True)
                page_text = response.text
            except Exception, error_message:
                logger.error("Unable to fetch following URL: %s, error message: %s, retry count: %s" % (url, error_message, format(retry_count)))
                if "Connection aborted" in error_message:
                    continue
            break
        return page_text
   
    def get_page_dom(self, url, verify=True):
        '''Fetches page from URL, creates and returns dom tree
        '''
        dom_tree = None
        page_text = self.fetch_page(url, verify=verify)
        if page_text:
            dom_tree = etree.HTML(page_text)
        return dom_tree 
   
    def get_page_json_dict(self, url, verify=True):
        '''Fetches page from URL, creates and returns JSON dict object
        '''
        json_dict = {}
        page_text = self.fetch_page(url, verify=verify)
        if page_text:
            json_dict = json.loads(page_text)
        return json_dict 
     
    def get_genomes_with_phenotype_list(self, url):
        '''Fetches links for extracting genomes with phenotypes from the URL
        '''
        genomes_with_phenotype_list = []
        page_dom = self.get_page_dom(url)
        sid_script = page_dom.xpath("//script[contains(.,'sid=')]//text()")[0]
        sid_session_slug = sid_script.split('YAHOO.util.DataSource("json_proxy.cgi?')[-1].split('");')[0]
        json_url = "https://img.jgi.doe.gov/cgi-bin/m/json_proxy.cgi?%sresults=100&startIndex=0&sort=RuleID&dir=asc&c=&f=&t=&callid=1622920132871" % sid_session_slug
        json_dict = self.get_page_json_dict(json_url)
        for record in json_dict['records']:
             record_url_slug = record["NoofGenomeswPhenotypeDisp"].split("<a href='")[1].split("'  onclick")[0]
             genomes_with_phenotype_list.append(BASE_URL + record_url_slug)
        return genomes_with_phenotype_list 

if __name__ == '__main__':
    obj = PredictedPhenotypeScraper()
    obj.get_genomes_with_phenotype_list(SAMPLE_URL) 
