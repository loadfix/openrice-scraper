#!/usr/local/bin/python27
# -*- coding: utf-8 -*-

#
# Feed Me! - OpenRice Mobile Website Scraper
# Copyright (c) 2017 loadfix
#
from bs4 import BeautifulSoup
import urllib3
import re
import sys
import csv
import requests
import signal
import zhon.hanzi
import time
from datetime import datetime

# ---------- Start of User Configurable Variables ------------- #
apikey = ''
api = "http://api.openrice.com/english/english/mobile/sr2.htm?shopid="
searchurl = 'https://maps.googleapis.com/maps/api/geocode/json'
start = 1
end = 1000000
debug = True
# ---------- End of User Configurable Variables --------------- #

# Check arguments
if len(sys.argv) == 1:
   pass
elif len(sys.argv) == 3:
   try:
      start = int(sys.argv[1])
      end = int(sys.argv[2])
   except:
      print("Incorrect arguments")
      sys.exit(1)
else:
   print( "Incorrect number of arguments")
   sys.exit(1)

print("Starting at " + str(start) + " and ending at " + str(end))

# Output CSV File
outfile = 'restaurants-' + str(start) + '-' + str(end) + '.csv'

# Restaurant count
count = 0

hkareas_c = {
   '迪士尼樂園': 'Disneyland',
   '大嶼山大澳': 'Tai O',
   '旺角':       'Mong Kok',
   '紅磡':       'Hung Hom',
   '青衣':       'Tsing Yi',
   '尖沙咀':     'Tsim Sha Tsui',
   '大嶼山長沙': 'Cheung Sha',
   '黃大仙':     'Wong Tai Sin',
   '馬鞍山':     'Ma On Shan',
   '土瓜灣':     'To Kwa Wan',
   '九龍城':     'Kowloon City',
   '馬灣':       'Ma Wan',
   '美孚':       'Mei Foo',
   '半山':       'Mid-Levels',
   '大嶼山梅窩': 'Mui Wo',
   '大澳':       'Tai O',
   '彩虹牛池灣': 'Ngau Chi Wan',
   '牛頭角':     'Ngau Tau Kok',
   '昂坪':       'Ngong Ping',
   '何文田':     'Ho Man Tin',
   '北角':       'North Point',
   '深水埗':     'Sham Shui Po',
   '杏花邨':     'Heng Fa Chuen',
   '坪洲':       'Peng Chau',
   '蒲苔島':     'Po Toi',
   '薄扶林':     'Pok Fu Lam',
   '上環':       'Sheung Wan',
   '太子':       'Prince Edward',
   '大嶼山貝澳': 'Pui O',
   '鰂魚涌':     'Quarry Bay',
   '太古':       'Tai Koo',
   '淺水灣':     'Repluse Bay',
   '西貢':       'Sai Kung',
   '西環':       'Sai Wan',
   '西灣河':     'Sai Wan Ho',
   '西環西營盤': 'Sai Ying Pun',
   '新蒲崗':     'San Po Kong',
   '觀塘':       'Kwun Tong',
   '沙田':       'Sha Tin',
   '沙田大圍':   'Sha Tin',
   '深井':       'Sham Tseng',
   '筲箕灣':     'Shau Kei Wan',
   '石硤尾':     'Shek Kip Mei',
   '元朗':       'Yuen Long',
   '石澳':       'Shek O',
   '粉嶺':       'Fanling',
   '大埔':       'Tai Po',
   '柴灣':       'Chai Wan',
   '銅鑼灣':     'Causeway Bay',
   '赤柱':       'Stanley',
   '中環':       'Central',
   '大坑':       'Tai Hang',
   '大角咀':     'Tai Kok Tsui',
   '荃灣':       'Tsuen Wan',
   '天后':       'Tin Hau',
   '天水圍':     'Tin Shui Wai',
   '香港仔':     'Aberdeen',
   '將軍澳':     'Tseung Kwan O',
   '樂富聯':     'Wang Tau Hom',
   '佐敦':       'Jordan',
   '油塘':       'Yau Tong',
   '九龍塘':     'Kowloon Tong',
   '荔枝角':     'Lai Chi Kok',
   '長沙灣':     'Cheung Sha Wan',
}

# Hong Kong Areas and Districts
hkareas = {
   'Aldrich Bay':        'Eastern',
   'Butterfly Beach':    'Tuen Mun',
   'Caroline Hill':      'Wan Chai',
   'Po Toi':             'Islands',
   'Cha Kwo Ling':       'Kwun Tong',
   'Bowrington':         'Wan Chai',
   'Admiralty':          'Central and Western',
   'Tsim Sha Tsui':      'Yau Tsim Mong',
   'Tai Po':             'Tai Po',
   'Sha Tin':            'Sha Tin',
   'Jordan':             'Yau Tsim Mong',
   'Causeway Bay':       'Wan Chai',
   'Shek O':             'Southern',
   'Prince Edward':      'Yau Tsim Mong',
   'Chek Lap Kok':       'Islands',
   'Mong Kok':           'Yau Tsim Mong',
   'Sai Kung':           'Sai Kung',
   'Yau Ma Tei':         'Yau Tsim Mong',
   'Chai Wan':           'Eastern',
   'Wan Chai':           'Wan Chai',
   'Kowloon City':       'Kowloon City',
   'Hung Hom':           'Kowloon City',
   'Lamma Island':       'Islands',
   'Lei Yue Mun':        'Kwun Tong',
   'Tai Koo':            'Eastern',
   'Kwai Fong':          'Kwai Tsing',
   'Quarry Bay':         'Eastern',
   'Cheung Chau':        'Islands',
   'Yuen Long':          'Yuen Long',
   'Mei Foo':            'Sham Shui Po',
   'To Kwa Wan':         'Kowloon City',
   'Sheung Shui':        'North',
   'Kowloon Tong':       'Kowloon City', # Divided with Sham Shui Po District
   'Lai Chi Kok':        'Sham Shui Po',
   'North Point':        'Eastern',
   'Mui Wo':             'Islands',
   'Tai O':              'Islands',
   'Ap Lei Chau':        'Southern',
   'Tuen Mun':           'Tuen Mun',
   'Tsing Yi':           'Kwai Tsing',
   'Tai Wai':            'Sha Tin',
   'San Po Kong':        'Wong Tai Sin',
   'Tseung Kwan O':      'Sai Kung',
   'Kwai Chung':         'Kwai Tsing',
   'Tsuen Wan':          'Tsuen Wan',
   'Sham Shui Po':       'Sham Shui Po',
   'Aberdeen':           'Southern',
   'Tin Hau':            'Wan Chai',
   'Kowloon Bay':        'Kwun Tong',
   'Tung Chung':         'Islands',
   'Diamond Hill':       'Wong Tai Sin',
   'Fanling':            'North',
   'Sheung Wan':         'Central and Western',
   'Stanley':            'Southern',
   'Ho Man Tin':         'Kowloon City',
   'Happy Valley':       'Wan Chai',
   'Pok Fu Lam':         'Southern',
   'Ma On Shan':         'Sha Tin',
   'Tai Hang':           'Wan Chai',
   'The Peak':           'Central and Western',
   'Kwun Tong':          'Kwun Tong',
   'Wong Chuk Hang':     'Southern',
   'Ngau Tau Kok':       'Kwun Tong',
   'Shek Kip Mei':       'Sham Shui Po',
   'Kennedy Town':       'Central and Western',
   'Tai Kok Tsui':       'Yau Tsim Mong',
   'Disneyland':         'Islands',
   'Lok Fu':             'Wong Tai Sin',
   'Sai Wan Ho':         'Eastern',
   'Shau Kei Wan':       'Eastern',
   'Ngong Ping':         'Islands',
   'Repulse Bay':        'Southern',
   'Cheung Sha Wan':     'Sham Shui Po',
   'Deep Water Bay':     'Southern',
   'Fo Tan':             'Sha Tin',
   'Mid-Levels':         'Central and Western',
   'Tin Shui Wai':       'Yuen Long',
   'Wong Tai Sin':       'Wong Tai Sin',
   'Lam Tin':            'Kwun Tong',
   'Tai Wo':             'Tai Po',
   'Sai Ying Pun':       'Central and Western',
   'Peng Chau':          'Islands',
   'Discovery Bay':      'Islands',
   'Choi Hung':          'Wong Tai Sin',
   'Pui O':              'Islands',
   'Tsz Wan Shan':       'Wong Tai Sin',
   'Cheung Sha Village': 'Sham Shui Po',
   'Sham Tseng':         'Tsuen Wan',
   'Ma Wan':             'Tsuen Wan',
   'Heng Fa Chuen':      'Eastern',
   'Lok Ma Chau':        'North',
   'Yau Tong':           'Kwun Tong',
   'Lo Wu':              'North',
   'Fortress Hill':      'Eastern',
   'Western District':   'Central and Western',
   'Siu Sai Wan':        'Eastern',
   'Shouson Hill':       'Southern',
   'Tai Tam':            'Southern',
   'Wah Fu':             'Southern',
   'So Uk':              'Sham Shui Po',
   'Tai Kak Airport':    'Kowloon City',
   'Ma Tau Wei':         'Kowloon City',
   'Whampoa Garden':     'Kowloon City',
   'Sau Mau Ping':       'Kwun Tong',
   'Wang Tau Hom':       'Wong Tai Sin',
   'Chuk Yuen':          'Wong Tai Sin',
   'Choi Wan':           'Wong Tai Sin',
   'Fung Wong':          'Wong Tai Sin',
   'Siu Lek Yuen':       'Sha Tin',
   'Ma Liu Shui':        'Sha Tin',
   'Ting Kok':           'Tai Po',
   'Plover Cove':        'Tai Po',
   'Central':            'Central and Western',
   'Lau Fau Shan':       'Yuen Long',
   'Lung Kwu Tan':       'Yuen Long'
}

# HK Districts, Region and Populations
# Source: http://www.censtatd.gov.hk/hkstat/sub/sp150.jsp?productCode=B1130301
# Note: not currently used
hkdistricts = {
   'Islands':              ['New Territories',  146900],
   'Tai Po':               ['New Territories',  307100],
   'North':                ['New Territories',  310800],
   'Sai Kung':             ['New Territories',  448600],
   'Yuen Long':            ['New Territories',  607200],
   'Tsuen Wan':            ['New Territories',  303600],
   'Tuen Mun':             ['New Territories',  495900],
   'Southern':             ['Hong Kong Island', 269200],
   'Sha Tin':              ['New Territories',  648200],
   'Wan Chai':             ['Hong Kong Island', 150900],
   'Central and Western':  ['Hong Kong Island', 224600],
   'Kwai Tsing':           ['New Territories',  507100],
   'Eastern':              ['Hong Kong Island', 574500],
   'Kowloon City':         ['Kowloon',          405400],
   'Sham Shui Po':         ['Kowloon',          390600],
   'Yau Tsim Mong':        ['Kowloon',          318100],
   'Wong Tai Sin':         ['Kowloon',          426200],
   'Kwun Tong':            ['Kowloon',          641100]
}

# Update the restaurant dictionary with address info
def parsesearch(restaurant, searchresults):
  
   if restaurant['lat'] == '':
      restaurant['lat'] = searchresults['lat']
   if restaurant['lng'] == '':
      restaurant['lng'] = searchresults['lng']
   if restaurant['area'] == '':
      restaurant['area'] = searchresults['area']
   if restaurant['district'] == '':
      restaurant['district'] = searchresults['district']
   if restaurant['formatted_address'] == '':
      restaurant['formatted_address'] = searchresults['formatted_address']
     
   return restaurant

# Catch Control-C
def signal_handler(signal, frame):
   print("Stoped at restaurant ID: " + str(restaurant['rest_id']))
   end_time = datetime.now()
   print("\n" + str(count) + " active restaurants processed in " + str(end_time - start_time) + " seconds\n")
   sys.exit(0)

# Use the Google Maps API to attempt to retrieve location data
def getgeolocation(address):
   
   # Initialise search dictionary
   search = {
      'area': '',
      'district': '',
      'lat': '',
      'lng': '',
      'formatted_address': ''
    }
   
   # Search based on provided criteria
   params = {'sensor': 'false', 'address': address, 'key': apikey }
   
   try:
      r = requests.get(searchurl, params=params)
   except:
      # Simply try again in 5 seconds
      time.sleep(5)
      r = requests.get(searchurl, params=params)
   
   if debug == True:
      print("Google Maps API Query: " + r.url)
   
   status = r.json()['status']
   results = r.json()['results']
   
   if status == 'OVER_QUERY_LIMIT':
      # Used Google API too much today
      # Limits are 2,500 for anonymous users and 100,000 for registered users (per day)
      print("Exceeded Google API Query Limit for today.")
      
      # Skip geolocation for now
      #pass
      sys.exit(1)
   
   if len(results) > 0:
      for y in range(0,len(results)):
         for x in range(0, len(results[y]['address_components'])-1):
            if results[y]['address_components'][x]['types'][0] == 'neighborhood':
               search['area'] = results[y]['address_components'][x]['long_name']
            if results[y]['address_components'][x]['types'][0] == 'administrative_area_level_1':
               search['district'] = results[y]['address_components'][x]['long_name']
   
      # Typically these values are always returned
      search['lat'] = results[0]['geometry']['location']['lat']
      search['lng'] = results[0]['geometry']['location']['lng']
      search['formatted_address'] = results[0]['formatted_address']
         
   return search

# Main program
start_time = datetime.now()

# Catch Control-C, flush buffer to CSV
signal.signal(signal.SIGINT, signal_handler)

with open(outfile, 'w') as csvfile:
   fieldnames = [
    'name_e', 'name_c', 'address_e', 'address_c', 'phone',
    'category_1', 'category_2', 'category_3', 'category_4',
    'category_5', 'category_6', 'category_7', 'category_8',
    'spending', 'score', 'good', 'ok', 'bad', 'closed', 'rest_id',
    'area', 'district', 'lat', 'lng', 'formatted_address'
   ]
   writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
   writer.writeheader()

   for x in range(start, end):
      restaurant = {
         'name_e': '',  'name_c': '', 'address_e': '', 'address_c': '',
         'closed': '', 'spending': '', 'phone': '', 'score': '',
         'good': '', 'ok': '', 'bad': '', 'formatted_address': '',
         'category_1': '', 'category_2': '', 'category_3': '', 'category_4': '',
         'category_5': '', 'category_6': '', 'category_7': '', 'category_8': '',
         'rest_id': '', 'area': '', 'district': '', 'lat': '', 'lng': ''
      }
      
      url = api + str(x)
      
      failures = 0
      # Exit on five failed http requests
      while True:
         try:
            http = urllib3.PoolManager()
            response = http.request('GET',url)
            break
         except:
            failures = failures + 1
            time.sleep(5)
      
         if failures > 4:
            print("Failed to retrieve URL 5 times, exiting...")
            sys.exit(1)
         
      print("at soup")
      soup = BeautifulSoup(response.read(), 'html.parser')
      arraylen = len(soup.find_all('span', { 'class': 'basic_info_spacing'}))
   
      # Check if there is a restaurant at this index / OpenRice ID
      if arraylen == 0:
         continue

      # OpenRice restaurant ID
      restaurant['rest_id'] = x

      # Skip if this is a Shenzhen restaurant
      try:
         if soup.find_all('a')[4].text == 'OpenRice Shenzhen Home':
            if debug:
               print("Found Shenzhen restaurant at " + str(restaurant['rest_id']) + ", skipping...")
               continue
         if soup.find_all('a')[4].text == 'OpenRice Macau Home':
            if debug:
               print("Found Macau restaurant at " + str(restaurant['rest_id']) + ", skipping...")
               continue
      except:
         pass
         
      spans = soup.find_all('span', attrs={'class':'basic_info_spacing'})

      # First span tag contains the restaurant name
      restaurant['name_e'] = spans[0].text.encode('utf-8').strip()
      if restaurant['name_e'].find('(Closed)') != -1:
         restaurant['closed'] = 1
      elif restaurant['name_e'].find('(Relocated') != -1:
         # Skip relocated restaurants
         continue
      else:
         restaurant['closed'] = 0
         count = count + 1
      
      # Remove "(Closed)" in restaurant name
      restaurant['name_e'] = re.split('\(', restaurant['name_e'])[0].strip()
 
      # Detect Chinese characters in title
      if len(re.findall('[%s]' % zhon.hanzi.characters, restaurant['name_e'].decode('UTF-8'))) > 0:
         if len(re.findall('[%s]' % zhon.hanzi.characters, restaurant['name_e'].decode('UTF-8').split('  ')[0])) > 0:
            # Chinese characters are detected in the first element, assume no english
            if debug:
               print("FOUND CHINESE ONLY NAME")
            try:
               restaurant['name_c'] = restaurant['name_e'].decode('UTF-8').split('  ')[0]
               if debug:
                  print("CHINESE NAME IS NOW: " + restaurant['name_c'])
                  restaurant['name_e'] = ''
            except:
               if debug:
                  print("CAUGHT EXCEPTION #1")
         else:
            # Try to seperate chinese and english
            try:
               restaurant['name_e'], restaurant['name_c'] = restaurant['name_e'].split('  ')
            except:
               if debug:
                  print("CAUGHT EXCEPTION #2")
               restaurant['name_e'] = re.split('  ', restaurant['name_e'])[0].strip()
      
      if debug:
         print("Processing ID:" + str(restaurant['rest_id']) + "\tEnglish: " + restaurant['name_e'] + \
            "\t\t\t  Chinese: " + restaurant['name_c'] + '(' + str(count) + ')')

      # Do not write closed restaurants to CSV file
      if restaurant['closed'] == 1:
         continue

      # Process the remainder of the span tags
      for span in spans:
         var = re.split(':',span.text)

         if var[0] == 'English Address ':
            restaurant['address_e'] = var[1].encode('utf-8').strip()
         if var[0] == 'Chinese Address ':
            restaurant['address_c'] = var[1].encode('utf-8').strip()
         if var[0] == 'Categories ':
            categories = re.split('\|', var[1])
            try:
               restaurant['category_1'] = categories[0].strip()
            except:
               pass

            try:
               restaurant['category_2'] = categories[1].strip()
            except:
               pass

            try:
               restaurant['category_3'] = categories[2].strip()
            except:
               pass

            try:
               restaurant['category_4'] = categories[3].strip()
            except:
               pass

            try:
               restaurant['category_5'] = categories[4].strip()
            except:
               pass

            try:
               restaurant['category_6'] = categories[5].strip()
            except:
               pass

            try:
               restaurant['category_7'] = categories[6].strip()
            except:
               pass

            try:
               restaurant['category_8'] = categories[7].strip()
            except:
               pass

         if var[0] == 'Phone No. ':
            restaurant['phone'] = var[1].encode('utf-8').strip()

         if var[0] == 'Spending ':
            restaurant['spending'] = var[1].encode('utf-8').strip()

         if var[0] == 'Overall Score ':
            restaurant['score'] = var[1].encode('utf-8').strip()

      restaurant['good'] = soup.find('img', {'alt': 'good'}).find_next_siblings('b')[0].text
      restaurant['ok']   = soup.find('img', {'alt': 'OK'}).find_next_siblings('b')[0].text
      restaurant['bad']  = soup.find('img', {'alt': 'bad'}).find_next_siblings('b')[0].text

      # Find the restaurant area from the address
      if restaurant['address_e'] != "":
         for key in hkareas.iterkeys():
            if restaurant['address_e'].find(key) > 0:
               restaurant['area'] = key
               restaurant['district'] = hkareas[key]
               break
      else:
          for key in hkareas_c.iterkeys():
            if restaurant['address_c'].find(key) > 0:
               restaurant['area'] = hkareas_c[key]
               restaurant['district'] = hkareas[hkareas_c[key]]
               break
                 
      # Geo-locate the restaurant
      while restaurant['lat'] == "" or restaurant['lng'] == "" or restaurant['area'] == "" or restaurant['district'] == '':
      
         # Try English address 
         if restaurant['address_e'] != "":
            searchresults = getgeolocation(restaurant['address_e'] + ",Hong Kong")
            restaurant = parsesearch(restaurant, searchresults)
            
         if restaurant['lat'] != "" and restaurant['lng'] != "" and restaurant['area'] != "" and restaurant['district'] != '':
            break
         
         # Try English name address 
         if restaurant['address_e'] != "" and restaurant['name_e'] != "":
            searchresults = getgeolocation(restaurant['name_e'] + "," + restaurant['address_e'] + ",Hong Kong")
            restaurant = parsesearch(restaurant, searchresults)
            
         if restaurant['lat'] != "" and restaurant['lng'] != "" and restaurant['area'] != "" and restaurant['district'] != '':
            break
         
         # Try English name and Chinese address
         if restaurant['name_e'] != "" and restaurant['address_c'] != "":
            searchresults = getgeolocation(restaurant['name_e'] + "," + restaurant['address_c'])
            restaurant = parsesearch(restaurant, searchresults)
            
         if restaurant['lat'] != "" and restaurant['lng'] != "" and restaurant['area'] != "" and restaurant['district'] != '':
            break
         
         # Try Chinese name and address
         if restaurant['name_c'] != "" and restaurant['address_c'] != "":
            searchresults = getgeolocation(restaurant['name_c'] + "," + restaurant['address_c'])
            restaurant = parsesearch(restaurant, searchresults)
            
         if restaurant['lat'] != "" and restaurant['lng'] != "" and restaurant['area'] != "" and restaurant['district'] != '':
            break
         
         # Try by formatted address
         if restaurant['formatted_address'] != "":
            searchresults = getgeolocation(restaurant['formatted_address'])
            restaurant = parsesearch(restaurant, searchresults)
            
         if restaurant['lat'] != "" and restaurant['lng'] != "" and restaurant['area'] != "" and restaurant['district'] != '':
            break
 
        # Try English name and area
        # This should be tried as a last resort as some restaurant chains (e.g. McDonald's) have many stores in the same area
         if restaurant['name_e'] != "" and restaurant['area'] != "":
            searchresults = getgeolocation(restaurant['name_e'] + "," + restaurant['area'] + ",Hong Kong")
            restaurant = parsesearch(restaurant, searchresults)
                     
         if restaurant['lat'] != "" and restaurant['lng'] != "" and restaurant['area'] != "" and restaurant['district'] != '':
            break
 
         # As a really last resort, try by GPS location
         if restaurant['lat'] != "" and restaurant['lng'] != "":
            searchresults = getgeolocation(str(restaurant['lat']) + "," + str(restaurant['lng']))
            restaurant = parsesearch(restaurant, searchresults)
         
         # Give up on geolocation if the above fails to get the required values
         break
         
         if debug:
            # Everything failed!!! Wait for keypress
            raw_input('Press enter to continue to next restaurant... ')
     
      # Write the restaurant data
      try:
         if debug:  
            print("\n===>> Area: " + restaurant['area'] + " District: " + restaurant['district'] + "\n")
         
         writer.writerow(restaurant)
      except:
         continue

# End of main program
end_time = datetime.now()
print("\n" + str(count) + " active restaurants processed in " + str(end_time - start_time) + " seconds\n")
   
