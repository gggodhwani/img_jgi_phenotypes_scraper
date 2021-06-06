'Class for Scraping Phenotypes Data from IMG JGI'

import calendar
from collections import OrderedDict
import json 
from lxml import etree
import os
import re
import requests
from requests.adapters import HTTPAdapter
import shutil
import time
from urllib3.util import Retry

BASE_URL = "https://img.jgi.doe.gov/cgi-bin/m/" 
SAMPLE_URL = "https://img.jgi.doe.gov/cgi-bin/m/main.cgi?section=ImgPwayBrowser&page=phenoRules"
GENOME_INFO_XPATH = "//div[@id='nowrap']//a[1]/@href"
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
        print("Fetching URL: %s" % url) #TODO Remove later
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
     
    def save_dict_as_json(self, data_dict, file_path):
        '''Saves data dict as a JSON file document for the given path
        '''
        json_file = open(file_path, "wb")
        json_file.write(json.dumps(data_dict))
        json_file.close()    
    
    def get_genomes_details_for_phenotypes(self, url, phenotype_json_path, organism_list_json_path, organism_info_json_path):
        '''Get all genomes details for Selected Phenotypes 
        '''
        phenotype_list = self.get_phenotype_metadata_list(url)
        phenotype_list_with_organisms_urls = self.populate_organisms_urls_for_phenotype(phenotype_list)
        self.save_dict_as_json(phenotype_list_with_organisms_urls, phenotype_json_path)
        total_organism_dict = {} 
        for record in phenotype_list_with_organisms_urls:
            for org_id,organism_dict in record['organisms_data'].items():
                total_organism_dict[org_id] = organism_dict
        self.save_dict_as_json(total_organism_dict, organism_list_json_path)
        self.fetch_save_organism_genome_info(total_organism_dict, organism_info_json_path)
    
    def get_phenotype_metadata_list(self, url):
        '''Fetches links for extracting genomes with phenotypes from the URL
        '''
        phenotype_metadata_list = []
        page_dom = self.get_page_dom(url)
        sid_script = page_dom.xpath("//script[contains(.,'sid=')]//text()")[0]
        sid_session_slug = sid_script.split('YAHOO.util.DataSource("json_proxy.cgi?')[-1].split('");')[0]
        json_url = "https://img.jgi.doe.gov/cgi-bin/m/json_proxy.cgi?%sresults=100&startIndex=0&sort=RuleID&dir=asc&c=&f=&t=&callid=1622920132871" % sid_session_slug
        json_dict = self.get_page_json_dict(json_url)
        for record in json_dict['records']:
             record_url_slug = record["NoofGenomeswPhenotypeDisp"].split("<a href='")[1].split("'  onclick")[0]
             record['record_url'] = BASE_URL + record_url_slug
             phenotype_metadata_list.append(record)
        return phenotype_metadata_list  

    def populate_organisms_urls_for_phenotype(self, phenotype_list):
        '''Fetches links of organism specific pages for selected phenotypes
        '''
        for record in phenotype_list:
            record_page_dom = self.get_page_dom(record['record_url'])
            record['organisms_data'] = OrderedDict()
            for organism_url_slug in record_page_dom.xpath(GENOME_INFO_XPATH):
                org_id = organism_url_slug.split("_oid=")[-1]
                record['organisms_data'][org_id] = OrderedDict({"organism_url": BASE_URL + organism_url_slug})
        return phenotype_list

    def fetch_save_organism_genome_info(self, total_organism_dict, organism_info_json_path):
        '''Fetches and saves organism genome info for each phenotype list & organism URLs
        '''
        for org_id,organism_dict in total_organism_dict.items():
            organism_info_dict = OrderedDict()
            organism_dom = self.get_page_dom(organism_dict['organism_url'])
            organism_table_rows = organism_dom.xpath("//tr[@class='img' or @class='highlight']")
            for organism_row in organism_table_rows:
                organism_row_key = organism_row.xpath("./*[1]//text()")[0].strip()
                if not organism_row_key:
                    continue
                organism_row_value_node = organism_row.xpath("./*[2]//text()")
                if not organism_row_value_node:
                    continue
                organism_row_value = organism_row_value_node[0].strip()
                organism_info_dict[organism_row_key] = organism_row_value
            self.save_dict_as_json(organism_info_dict, organism_info_json_path.replace("###", org_id))

if __name__ == '__main__':
    obj = PredictedPhenotypeScraper()
    start_time = format(calendar.timegm(time.gmtime()))
    phenotype_json_path = "phenotype_%s.json" % start_time
    organism_list_json_path = "total_organism_%s.json" % start_time
    organism_info_json_path = "organism_%s_###.json" % start_time #Here ### needs to be replaced with each unique organism_id 
    obj.get_genomes_details_for_phenotypes(SAMPLE_URL, phenotype_json_path, organism_list_json_path, organism_info_json_path) 
