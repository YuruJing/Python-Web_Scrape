


import requests
import csv 
import os
import glob
import pandas as pd
import numpy as np


# # Baidu Map Scraping 

# primary: scrape the Baidu Map
class Map_Scrape: 
    def __init__(self, headers,link,region, keywords):
        # store parameters 
        self.headers = headers
        self.link = link
        self.region = region
        self.keywords = keywords

    # orginal scrape the raw map
    def scrape(self, pagenum=0, ak='None'): ## None here need to type your own api key from Baidu Map
        ## parameters setting
        params = {'query': self.keywords,
          'region': self.region,
         'output':'json',
         'scope':'2', ## get POI details info
         'page_size':'20',
         'page_num': pagenum,
         'ak':ak}
        ## scrape and convert it into json version 
        response = requests.get(self.link, params=params, headers=self.headers, verify=False)
        return response.json()
    
    # store and combine all scrape results
    def combine(self):
        
        ## create the first page search and prepare the following checks
        first_search = self.scrape()
        
        ## verify if the search needs to further extend or not 
        if first_search['result_type'] != 'poi_type':
            print("Too Many! {} {}".format(self.region, self.keywords))
            
            ## create the citylist for the extend new search
            city_length = len(first_search['results'])
            city_list = list()
            for num in range(city_length):
                city_list.append(first_search['results'][num]['name'])
                
            print("Get city list and Extend the further search!")
            
            return city_list
        else:
        ## verify the total number for the search page
            total_num = int(first_search['total'])
            total_dict = dict()
            
            if total_num <= 20: 
                total_dict['result1'] = first_search['results']
                print(1)
            else:
                total_dict['result' + str(1)] = first_search['results']
                
                for i in range(1,int(np.ceil(total_num/20))):
                    total_dict['result' + str(i+1)] = self.scrape(pagenum=i)['results']
                    print(i+1)
                    
        return total_dict
    
    
    
    # adjust the raw data from the scrape
    def adjust_info(self, total_dict):
        for key in total_dict.keys():
            for i in range(len(total_dict[key])):
                if 'detail_info' in total_dict[key][i]:
                    try:
                        total_dict[key][i]['type'] = total_dict[key][i]['detail_info']['type']
                    except:
                        total_dict[key][i]['type'] = str()
                    try: 
                        total_dict[key][i]['detail_url'] = total_dict[key][i]['detail_info']['detail_url']
                    except:
                        total_dict[key][i]['detail_url'] = str()
                    try:
                        total_dict[key][i]['shop_hours'] = total_dict[key][i]['detail_info']['shop_hours']
                    except:
                        total_dict[key][i]['shop_hours'] = str()

                    del total_dict[key][i]['detail_info']
                else:
                    total_dict[key][i]['type'] = str()
                    total_dict[key][i]['detail_url'] = str()
                    total_dict[key][i]['shop_hours'] = str() 
                
        return total_dict
    
    # generate and save the final csv files for the scrape
    def get_csv(self, new_total_dict,filenames): 
        
    ## csv file path
        csvfile = open("{}.csv".format(self.region+self.keywords),mode='w',newline='', encoding='utf-8')
        writer = csv.DictWriter(csvfile,fieldnames=filenames)
        writer.writeheader()
    
        for key in new_total_dict.keys():
            writer.writerows(new_total_dict[key])  
        csvfile.close()
         


# # Auxillary Functions

# auxillary functions

## create the subfolder for the datasets in city search(optional)
def get_list(search_cities):
    city_list = list()
    for i in range(len(search_cities)):
        city = search_cities['results'][i]['name']
        city_list.append(city)
        
    return city_list

## create the city folder under the province folder (optional)

## merge and clean the dataset(python way) 

## get all of the file path 
def merge(files_path):
    files = glob.glob(files_path + '*.csv')
### open and read all of the files 
    datas = list()
    for i in files:
        data = pd.read_csv(i, encoding='utf-8')
        datas.append(data)

    print("Done preparation for the concat!")
## merge all files together to generate a comprehensive data file
    new_df = pd.concat(datas, axis=0)
    
    return new_df.to_csv("全国有色金属集.csv", index=False)

## clean the dataset 
def clean_data(data, remove_features):
    ### check duplicates 
    data.drop_duplicates(subset='uid',inplace=True)
    ### remove unnecessary features 
    data.drop(columns=remove_features, inplace=True)
    ### check status (optional) -- if there's operating status
    
    ### save a new clean dataset 
    return data.to_csv("全国有色金属数据_clean.csv", index=False) 



# start the web scraping
header = { 
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.70'
} ## type your User-Agent 

link = "http://api.map.baidu.com/place/v2/search"

# test1: 山东省有色金属
Web_scrape = Map_Scrape(header, link, "山东省", "有色金属")
total_dict = Web_scrape.combine()
adjust_total_dict = Web_scrape.adjust_info(total_dict)
filename = ["name", "location", "address", "province", "city", "area", "detail", 'street_id', "telephone", "uid", "type", "detail_url", "shop_hours"]
Web_scrape.get_csv(adjust_total_dict, filename)

# test2: 内蒙古自治区有色金属
Web_scrape = Map_Scrape(header, link, "内蒙古自治区", "有色金属")
city_list = Web_scrape.combine()
city_list
# extend further search
for i in range(0,len(city_list)):
    Web_scrape = Map_Scrape(header, link, city_list[i], "有色金属")
    total_dict = Web_scrape.combine()
    adjust_total_dict = Web_scrape.adjust_info(total_dict)
    filename = ["name", "location", "address", "province", "city", "area", "detail", 'street_id', "telephone", "uid", "type", "detail_url", "shop_hours"]
    try:
        Web_scrape.get_csv(adjust_total_dict, filename)
    except:
        filename.append("num")
        Web_scrape.get_csv(adjust_total_dict, filename)
print("Done!")


# the combined way for the above test:test 1 and test 2. 
Web_scrape = Map_Scrape(header, link, "广东省", "金属")
check_results = Web_scrape.combine()
if type(check_results) is list: 
    for i in range(0,len(check_results)):
        Web_scrape = Map_Scrape(header, link, check_results[i], "金属")
        total_dict = Web_scrape.combine()
        adjust_total_dict = Web_scrape.adjust_info(total_dict)
        filename = ["name", "location", "address", "province", "city", "area", "detail", 'street_id', "telephone", "uid", "type", "detail_url", "shop_hours"]
        try:
            Web_scrape.get_csv(adjust_total_dict, filename)
        except:
            filename.append("num")
            Web_scrape.get_csv(adjust_total_dict, filename)
    print("Done!")
else:
    adjust_total_dict = Web_scrape.adjust_info(check_results)
    filename = ["name", "location", "address", "province", "city", "area", "detail", 'street_id', "telephone", "uid", "type", "detail_url", "shop_hours"]
    Web_scrape.get_csv(adjust_total_dict, filename)
    print("save province dataset!")


# merge the data
path = "C:/Users/Berti/Desktop/回收数据/"
merge(path)


# clean the data
df = pd.read_csv("{}/全国有色金属集.csv".format(path))
features = ['detail', 'street_id', 'uid']
clean_data(df, features)

