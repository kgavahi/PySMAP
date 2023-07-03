# -*- coding: utf-8 -*-
"""
Created on Tue Sep  3 15:33:16 2019

@author: 16785
"""

from __future__ import print_function
from netrc import netrc
import time
import os
import requests
import copy
import urllib
import datetime
import shutil
import tkinter as tk
from tkinter import ttk
import base64
import itertools
import json
import netrc
import ssl
import sys
from pyhdf.SD import SD, SDC
import numpy as np
from netCDF4 import Dataset
import multiprocessing
import h5py as h5py
from sklearn.ensemble import RandomForestRegressor
import pandas as pd
import pickle
from getpass import getpass
from scipy.ndimage import gaussian_filter
import pandas as pd
import traceback

try:
    from urllib.parse import urlparse
    from urllib.request import urlopen, Request, build_opener, HTTPCookieProcessor
    from urllib.error import HTTPError, URLError
except ImportError:
    from urlparse import urlparse
    from urllib2 import urlopen, Request, HTTPError, URLError, build_opener, HTTPCookieProcessor
if not os.path.exists('./SMAP'):
    os.mkdir('./SMAP')
if not os.path.exists('./MOD13A2'):
    os.mkdir('./MOD13A2')
if not os.path.exists('./MOD11A2'):
    os.mkdir('./MOD11A2')
if not os.path.exists('./IMERG'):
    os.mkdir('./IMERG')
if not os.path.exists('./SMOPS'):
    os.mkdir('./SMOPS')
if not os.path.exists('./IMERG/IMERG_1km'):
    os.mkdir('./IMERG/IMERG_1km')
if not os.path.exists('./SMAP/SMAP_1km'):
    os.mkdir('./SMAP/SMAP_1km')
if not os.path.exists('./SMAP/SPL3SMP_D'):
    os.mkdir('./SMAP/SPL3SMP_D')
#

class UserPassError(Exception): pass 
class DateOutOfRange(Exception): pass 
def myexcepthook(type, value, tb):
    l = ''.join(traceback.format_exception(type, value, tb))
    print(l)
sys.excepthook = myexcepthook
def nearest(items, pivot):
    return min(items, key=lambda x: abs(x - pivot))
def DownloadList(date_start, date_end, earthData_name):

    if earthData_name == 'IMERG':
        
        dateRange = pd.date_range(date_start, date_end, freq='D')
        
        URLs = []
        for date in dateRange:
            date = str(date)[:10].replace('-','')
            year = date[:4]
            mon = date[4:6]
            URLs.append('https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGDL.06/%s/%s/3B-DAY-L.MS.MRG.3IMERG.%s-S000000-E235959.V06.nc4'%(year, mon, date))
        
    else:
        urls = []
        dateList = []
        dateList_to_download = []
        
        #Convert to datetime
        year, month, day = map(int, date_start.split('-'))
        date_start = datetime.date(year, month, day)  # start date

        year, month, day = map(int, date_end.split('-'))
        date_end = datetime.date(year, month, day)  # end date


        #earthData_name    = 'MOD11A2' # MODIS LST 8-days
        #earthData_name    = 'MOD13A2' # MODIS NVDI
        earthData_version = '006'
               
        url = 'https://e4ftl01.cr.usgs.gov/MOLT/'+ earthData_name +'.'+ earthData_version +'/'
        
        # Read the entire page 
        with urllib.request.urlopen(url) as f:
            r = f.read().decode('utf-8')
        
        
        #select the lines with date indication
        folder_url = [s for s in r.split('\n') if "folder.gif" in s]
        
        #add the dates to the dateList
        for line in folder_url:
            line = line[51:61]
            year, month, day = map(int, line.split('.'))
            d = datetime.date(year, month, day)
            dateList.append(d)
        

    
        first_date = nearest(dateList, date_start)
    
        last_date  = nearest(dateList, date_end)
        
        
        # range check for dates
        if date_start < dateList[0]:
            raise DateOutOfRange('date_start (%s) < first MOD date (%s)'%(date_start, dateList[0]))        
        
        if date_end > dateList[-1]:
            raise DateOutOfRange('date_end (%s) > latest MOD date (%s)'%(date_end, dateList[-1]))
        



        # prepare the dateList_to_download
        for date_MOD in dateList:
            
            if date_MOD > last_date or date_MOD < first_date:
                continue
            
            # check if all the 14 tiles covering CONUS have already been downloaded
            if os.path.exists('%s/%s'%(earthData_name, str(date_MOD))):
                if len(os.listdir('%s/%s'%(earthData_name, str(date_MOD)))) >= 14:
                    print('%s/%s already exists'%(earthData_name, str(date_MOD)))
                    continue
            
            print(date_MOD, 'was added to the download list')
            
            dateList_to_download.append(date_MOD)
           

        # prepare the urls of hdf files over the CONUS
        for date in dateList_to_download:
            
            
        
            dir_url = 'https://e4ftl01.cr.usgs.gov/MOLT/'+ earthData_name +'.'+ earthData_version +'/' + str(date).replace('-','.') + '/'
            
            print('working on %s hdf files'%dir_url)
 
            # read the items in the dir_url
            with urllib.request.urlopen(dir_url) as f:
                r = f.read().decode('utf-8')
            

            # extract the hdf files
            hdf_files = []
            tiles = ['h08v04', 'h08v05', 'h08v06', 'h09v04', 'h09v05', 'h09v06', 'h10v04',
                    'h10v05', 'h10v06', 'h11v04', 'h11v05', 'h12v04', 'h12v05', 'h13v04']
            for tile in tiles:
                hdf_files.extend([s for s in [s for s in r.split('\n') if ("hdf" in s) & ("xml" not in s)] if tile in s])
            
            if len(hdf_files) != 14:
                raise DateOutOfRange('not all 14 tiles are available for in %s folder for download'%date)
            
            # add the files to the list
            for hdf_file in hdf_files:
                urls.append(dir_url + hdf_file.split('<a href="')[1].split('">')[0])
  
        URLs = list(set(urls))
        
    return URLs
def download_run(username , password , date_start , date_end , earthData_name):

    saveDir = './' +  earthData_name # Set local directory to download to
    pathNetrc = os.path.join(os.path.expanduser("~"),'.netrc')
    if os.path.exists(pathNetrc):
        os.remove(pathNetrc)
        
    netrcFile = ['machine urs.earthdata.nasa.gov','login ' + username,'password '+password]
    with open('.netrc', 'w') as f:
        for item in netrcFile:
            f.write("%s\n" % item)
        
    shutil.copy('.netrc',os.path.expanduser("~"))
    
    
    est = time.time()
    
    fileList = DownloadList(date_start , date_end, earthData_name)
    fileList = sorted(fileList)

    print('time for DownloadList: ', time.time() - est)
    #for i in fileList:
    #    print(i)


# -----------------------------------------DOWNLOAD FILE(S)-------------------------------------- #
# Loop through and download all files to the directory specified above, and keeping same filenames

    for count in range(len(fileList)):
        f = fileList[count]
        
        date_of_file = f.split('/')[5].replace('.','-')
        print(date_of_file)
        path = os.path.join(saveDir,date_of_file)
        if not os.path.exists(path):
            os.mkdir(path)
        saveName = os.path.join(path, f.split('/')[-1].strip())
        if os.path.exists(saveName):
            try:
                if not earthData_name=='IMERG':
                    SD(saveName, SDC.READ)
                else:
                    Dataset(saveName, 'r')
                print('%s already exists'%saveName)
                continue
            except:
                print('Damgeged file encountered, redownloading...')
    # Create and submit request and download file
        with requests.get(f.strip(), stream=True) as response:
            
            if response.status_code != 200:
                if response.status_code == 401:
                    raise UserPassError("Verify that your username and password are correct")
                if response.status_code == 404:
                    raise URLError('The URL does not exists (%s is not available for download)'%f)
            else:
                response.raw.decode_content = True
                content = response.raw
                with open(saveName, 'wb') as d:
                    while True:
                        chunk = content.read(16 * 1024)
                        if not chunk:
                            break
                        d.write(chunk)
                print('Downloaded file: {}'.format(saveName))
        print(count)
        print('    ',str((count+1)/len(fileList)*100)[:5] + ' % Completed')
        # Sleep for 30 sec after downloading 100 files to avoid "Connection Refused by the Server"
        if count != 0 and count%100==0:
            print('let me sleep for a while')
            time.sleep(30)
#
def get_username():
    username = ''

    # For Python 2/3 compatibility:
    try:
        do_input = raw_input  # noqa
    except NameError:
        do_input = input

    while not username:
        try:
            username = do_input('Earthdata username: ')
        except KeyboardInterrupt:
            quit()
    return username


def get_password():
    password = ''
    while not password:
        try:
            password = getpass('password: ')
        except KeyboardInterrupt:
            quit()
    return password
def get_credentials(url):
    CMR_URL = 'https://cmr.earthdata.nasa.gov'
    URS_URL = 'https://urs.earthdata.nasa.gov'
    CMR_PAGE_SIZE = 2000
    CMR_FILE_URL = ('{0}/search/granules.json?provider=NSIDC_ECS'
                    '&sort_key[]=start_date&sort_key[]=producer_granule_id'
                    '&scroll=true&page_size={1}'.format(CMR_URL, CMR_PAGE_SIZE))
    """Get user credentials from .netrc or prompt for input."""
    credentials = None
    try:
        info = netrc.netrc()
        username, account, password = info.authenticators(urlparse(URS_URL).hostname)
    except Exception:
        try:
            username, account, password = info.authenticators(urlparse(CMR_URL).hostname)
        except Exception:
            username = None
            password = None

    while not credentials:
        if not username:
            #username = get_username()
            #password = get_password()
            username = '********'
            password = '********'
        credentials = '{0}:{1}'.format(username, password)
        credentials = base64.b64encode(credentials.encode('ascii')).decode('ascii')

        if url:
            try:
                req = Request(url)
                req.add_header('Authorization', 'Basic {0}'.format(credentials))
                opener = build_opener(HTTPCookieProcessor())
                opener.open(req)
            except HTTPError:
                print('Incorrect username or password')
                credentials = None
                username = None
                password = None

    return credentials
def build_version_query_params(version):
    desired_pad_length = 3
    if len(version) > desired_pad_length:
        print('Version string too long: "{0}"'.format(version))
        quit()

    version = str(int(version))  # Strip off any leading zeros
    query_params = ''

    while len(version) <= desired_pad_length:
        padded_version = version.zfill(desired_pad_length)
        query_params += '&version={0}'.format(padded_version)
        desired_pad_length -= 1
    
    return query_params
def build_cmr_query_url(short_name, version, time_start, time_end, polygon=None, filename_filter=None):
    params = '&short_name={0}'.format(short_name)
    params += build_version_query_params(version)
    params += '&temporal[]={0},{1}'.format(time_start, time_end)
    if polygon:
        params += '&polygon={0}'.format(polygon)
    if filename_filter:
        params += '&producer_granule_id[]={0}&options[producer_granule_id][pattern]=true'.format(filename_filter)
    CMR_URL = 'https://cmr.earthdata.nasa.gov'
    URS_URL = 'https://urs.earthdata.nasa.gov'
    CMR_PAGE_SIZE = 2000
    CMR_FILE_URL = ('{0}/search/granules.json?provider=NSIDC_ECS'
                    '&sort_key[]=start_date&sort_key[]=producer_granule_id'
                    '&scroll=true&page_size={1}'.format(CMR_URL, CMR_PAGE_SIZE))
    return CMR_FILE_URL + params
def cmr_download(urls):
    """Download files from list of urls."""
    if not urls:
        return
    
    url_count = len(urls)
    print('Downloading {0} files...'.format(url_count))
    credentials = None

    for index, url in enumerate(urls, start=1):
        if not credentials and urlparse(url).scheme == 'https':
            credentials = get_credentials(url)

        filename = './SMAP/' + url.split('/')[-1]
        if os.path.exists(filename):
            print('%s already exists'%filename)
            continue
        #print('{0}/{1}: {2}'.format(str(index).zfill(len(str(url_count))),
        #                            url_count,
        #                            filename))

        try:
            # In Python 3 we could eliminate the opener and just do 2 lines:
            # resp = requests.get(url, auth=(username, password))
            # open(filename, 'wb').write(resp.content)
            req = Request(url)
            if credentials:
                req.add_header('Authorization', 'Basic {0}'.format(credentials))
            opener = build_opener(HTTPCookieProcessor())
            data = opener.open(req).read()
            open(filename, 'wb').write(data)
            print(url, ' was successfully downloaded')
        except HTTPError as e:
            print('HTTP error {0}, {1}'.format(e.code, e.reason))
        except URLError as e:
            print('URL error: {0}'.format(e.reason))
        except IOError:
            raise
        except KeyboardInterrupt:
            quit()
def cmr_filter_urls(search_results):
    """Select only the desired data files from CMR response."""
    if 'feed' not in search_results or 'entry' not in search_results['feed']:
        return []

    entries = [e['links']
               for e in search_results['feed']['entry']
               if 'links' in e]
    # Flatten "entries" to a simple list of links
    links = list(itertools.chain(*entries))

    urls = []
    unique_filenames = set()
    for link in links:
        if 'href' not in link:
            # Exclude links with nothing to download
            continue
        if 'inherited' in link and link['inherited'] is True:
            # Why are we excluding these links?
            continue
        if 'rel' in link and 'data#' not in link['rel']:
            # Exclude links which are not classified by CMR as "data" or "metadata"
            continue

        if 'title' in link and 'opendap' in link['title'].lower():
            # Exclude OPeNDAP links--they are responsible for many duplicates
            # This is a hack; when the metadata is updated to properly identify
            # non-datapool links, we should be able to do this in a non-hack way
            continue

        filename = link['href'].split('/')[-1]
        if filename in unique_filenames:
            # Exclude links with duplicate filenames (they would overwrite)
            continue
        unique_filenames.add(filename)

        urls.append(link['href'])

    return urls
def cmr_search(short_name, version, time_start, time_end,
               polygon='', filename_filter=''):
    CMR_URL = 'https://cmr.earthdata.nasa.gov'
    URS_URL = 'https://urs.earthdata.nasa.gov'
    CMR_PAGE_SIZE = 2000
    CMR_FILE_URL = ('{0}/search/granules.json?provider=NSIDC_ECS'
                    '&sort_key[]=start_date&sort_key[]=producer_granule_id'
                    '&scroll=true&page_size={1}'.format(CMR_URL, CMR_PAGE_SIZE))
    """Perform a scrolling CMR query for files matching input criteria."""
    cmr_query_url = build_cmr_query_url(short_name=short_name, version=version,
                                        time_start=time_start, time_end=time_end,
                                        polygon=polygon, filename_filter=filename_filter)
    #print('Querying for data:\n\t{0}\n'.format(cmr_query_url))

    cmr_scroll_id = None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    try:
        urls = []
        while True:
            req = Request(cmr_query_url)
            if cmr_scroll_id:
                req.add_header('cmr-scroll-id', cmr_scroll_id)
            response = urlopen(req, context=ctx)
            if not cmr_scroll_id:
                # Python 2 and 3 have different case for the http headers
                headers = {k.lower(): v for k, v in dict(response.info()).items()}
                cmr_scroll_id = headers['cmr-scroll-id']
                hits = int(headers['cmr-hits'])
                if hits > 0:
                    print('Found {0} matches.'.format(hits))
                else:
                    print('Found no matches.')
            search_page = response.read()
            search_page = json.loads(search_page.decode('utf-8'))
            url_scroll_results = cmr_filter_urls(search_page)
            if not url_scroll_results:
                break
            if hits > CMR_PAGE_SIZE:
                print('.', end='')
                sys.stdout.flush()
            urls += url_scroll_results

        if hits > CMR_PAGE_SIZE:
            print()
        return urls
    except KeyboardInterrupt:
        quit()
def SMAP_dl(date_start, date_end):
    short_name = 'SPL3SMP'
    version = '007'
    time_start = date_start + 'T00:00:00Z'
    time_end = date_end + 'T00:00:00Z'
    polygon = ''
    filename_filter = '*'


#


    urls = cmr_search(short_name, version, time_start, time_end,
                     polygon=polygon, filename_filter=filename_filter)
    cmr_download(urls)
    
    
    short_name = 'SPL3SMP'
    version = '008'
    time_start = date_start + 'T00:00:00Z'
    time_end = date_end + 'T00:00:00Z'
    polygon = ''
    filename_filter = '*'


#


    urls = cmr_search(short_name, version, time_start, time_end,
                     polygon=polygon, filename_filter=filename_filter)
    cmr_download(urls)
#
def ClipMODoverCONUS (hdfFile):
    FILE_NAME = hdfFile.split('/')[-1]
    product   = FILE_NAME.split('.')[0]
    tile_coor = FILE_NAME.split('.')[2]
    ind_cliped = np.load('./Data_Preprocessing/MODIS/' + tile_coor + '/MOD_ind.npy') - 1
    hdf = SD(hdfFile, SDC.READ)
    if product == 'MOD13A2':
        DATAFIELD_NAME = '1 km 16 days NDVI'
    elif product == 'MOD11A2':
        DATAFIELD_NAME = 'LST_Day_1km'
        #DATAFIELD_NAME = 'LST_Night_1km'
    data3D = hdf.select(DATAFIELD_NAME)
    LST_or_NDVI = data3D.get()
    #tile = './Data_Preprocessing/MODIS/'+ tile_coor + '/' + tile_coor + '.npy'
    #lon = np.load(tile)[0]
    #lat = np.load(tile)[1]
    #lat_in_CONUS = lat.flatten()[ind_cliped]
    #lon_in_CONUS = lon.flatten()[ind_cliped]
    LST_or_NDVI_in_CONUS = LST_or_NDVI.flatten()[ind_cliped]
    #return lat_in_CONUS, lon_in_CONUS, LST_or_NDVI_in_CONUS
    return LST_or_NDVI_in_CONUS    
def GatherAllTiles (product):
    folders = os.listdir('./' + product)
    
    #fileList = DownloadList(date_start , date_end, product)
    #fileList = sorted(fileList)
    #print(fileList)

    count = 0
    #for f in fileList:
    #    folder = f.split('/')[5].replace('.','-')
    for folder in folders:
        path = './' + product + '/' + folder
        if product == 'MOD11A2':
            if os.path.exists(path + '/LST.npy'):
                print(path + '/LST.npy' + ' already exists')
                continue
        if product == 'MOD13A2':
            if os.path.exists(path + '/NDVI14.npy'):
                print(path + '/NDVI14.npy' + ' already exists')
                continue                
        files = os.listdir(path)
        files = [f for f in files if f.endswith('.hdf')]
        lat14 = []
        lon14 = []
        LST_or_NDVI14 = []
        for file in files:
            print(file)
            hdfFile = path + '/' + file
            #lat_in_CONUS, lon_in_CONUS, LST_or_NDVI_in_CONUS = ClipMODoverCONUS(hdfFile)
            LST_or_NDVI_in_CONUS = ClipMODoverCONUS(hdfFile)
            #lat14 = np.append(lat14,lat_in_CONUS)
            #lon14 = np.append(lon14,lon_in_CONUS)
            LST_or_NDVI14 = np.append(LST_or_NDVI14,LST_or_NDVI_in_CONUS)
        #np.save(path + '/lat14.npy',lat14)
        #np.save(path + '/lon14.npy',lon14)
        if product=='MOD13A2':
            np.save(path + '/NDVI14.npy',LST_or_NDVI14)
        if product=='MOD11A2':
            np.save(path + '/LST.npy',LST_or_NDVI14)
        print('    ',str((count+1)/len(folders)/14*100)[:5] + ' % Completed')
        count += 1
def IMEGR_DownScale(file):
    path = './IMERG/GPM_3IMERGDL-06'
    #path = path + os.listdir(path)[0]
    dateOFfile = file.split('.')[4][:8]
    if os.path.exists('./IMERG/IMERG_1km/'+ 'Imerg_' + dateOFfile + '.npy'):
        print('./IMERG/IMERG_1km/'+ 'Imerg_' + dateOFfile + '.npy' + ' already exists')
        return
    f_IMERG = Dataset( path + '/' + file , 'r')
    IMERG_precp = f_IMERG.variables['precipitationCal'][0].T
    IMERG_precp = np.flip(IMERG_precp, axis=0)
    IMERG_precp = IMERG_precp[397:660, 550:1150]
    IMERG_precp = IMERG_precp.flatten()
    
    index = np.load('Data_Preprocessing/index_imerg.npy')
    
    IMERG_1km_14 = np.array(IMERG_precp[index])
    
    
    
    np.save('./IMERG/IMERG_1km/'+ 'Imerg_' + dateOFfile + '.npy',IMERG_1km_14)
def SMOPS_DownScale(file):
    f_SMOPS = Dataset( 'C:/Google Drive/University of Alabama/How To Do/SMOPS_data_retriever/CONUS/' + file , 'r')
    SMOPS_SM = f_SMOPS.variables['Blended_SM'][:]
    lat_SM = f_SMOPS.variables['latitude'][:]
    lon_SM = f_SMOPS.variables['longitudes'][:]


    lat_SM = np.repeat(lat_SM, 241, axis=0)
    lat_SM = np.reshape(lat_SM,(112,241))

    lon_SM = np.repeat(lon_SM, 112, axis=0)
    lon_SM = np.reshape(lon_SM,(241,112))
    lon_SM = lon_SM.T

    SMOPS_SM = SMOPS_SM.T

    lat_SM = lat_SM.flatten()
    lon_SM = lon_SM.flatten()
    SMOPS_SM = SMOPS_SM.flatten()

    TILES = ['h08v04','h08v05','h08v06','h09v04','h09v05','h09v06','h10v04','h10v05','h10v06','h11v04','h11v05','h12v04','h12v05','h13v04']
    SMOPS_1km_14 = []
    for tile in TILES:
        print(tile)
        path_tile = './Data_Preprocessing/SMOPS/' + tile
        ind_cliped_SMOPS = np.load(path_tile + '/SMOPS_ind.npy') - 1

        lat_in_CONUS_SMOPS = lat_SM[ind_cliped_SMOPS]
        lon_in_CONUS_SMOPS = lon_SM[ind_cliped_SMOPS]
        SMOPS_SM_in_CONUS  = SMOPS_SM[ind_cliped_SMOPS]

        SMOPS_range = np.arange(len(SMOPS_SM_in_CONUS))

        SMOPS_1km = np.load(path_tile + '/overlap_ind.npy')


        for i in range(len(SMOPS_SM_in_CONUS)):
            #print(i)
            SMOPS_1km = np.where(SMOPS_1km==SMOPS_range[i],SMOPS_SM_in_CONUS[i],SMOPS_1km)
        
        SMOPS_1km_14 = np.append (SMOPS_1km_14,SMOPS_1km)
    np.save('./SMOPS/'+'SMOPS_'+file[:8] + '.npy',SMOPS_1km_14)
def SMAP_to_1km( file , lat_1km , lon_1km , UP_vec , down_vec ):
    path = './SMAP/SMAP_1km/'
    dateOFfile = file[13:21]
    if os.path.exists(path + 'SMAP_' + dateOFfile + '.npy'):
        print(path + 'SMAP_' + dateOFfile + '.npy' + ' already exists')
        return
        
    #SMAP_1km = np.zeros([lon_1km.shape[0]])
    #SMAP_1km[SMAP_1km==0] = -9999
    
    
    dataset = h5py.File( './SMAP/' + file , 'r')
    name = '/Soil_Moisture_Retrieval_Data_AM/soil_moisture'
    # SM = dataset['Soil_Moisture_Retrieval_Data_AM/soil_moisture'][:]
    # lat = dataset['Soil_Moisture_Retrieval_Data_AM/latitude'][:]
    # lon = dataset['Soil_Moisture_Retrieval_Data_AM/longitude'][:]
    

      
    '''----------------------------AM-------------------------------------'''
    name_am = '/Soil_Moisture_Retrieval_Data_AM/soil_moisture'
    SM_am = dataset['Soil_Moisture_Retrieval_Data_AM/soil_moisture'][:]
    SM_am = np.where(SM_am==-9999.0,np.nan,SM_am)
    lat_am = dataset['Soil_Moisture_Retrieval_Data_AM/latitude'][:]
    lat_am = np.where(lat_am==-9999.0,np.nan,lat_am)
    lon_am = dataset['Soil_Moisture_Retrieval_Data_AM/longitude'][:]
    lon_am = np.where(lon_am==-9999.0,np.nan,lon_am)

    '''----------------------------PM-------------------------------------'''
    name_pm = '/Soil_Moisture_Retrieval_Data_PM/soil_moisture_pm'
    SM_pm = dataset['Soil_Moisture_Retrieval_Data_PM/soil_moisture_pm'][:]
    SM_pm = np.where(SM_pm==-9999.0,np.nan,SM_pm)
    lat_pm = dataset['Soil_Moisture_Retrieval_Data_PM/latitude_pm'][:]
    lat_pm = np.where(lat_pm==-9999.0,np.nan,lat_pm)
    lon_pm = dataset['Soil_Moisture_Retrieval_Data_PM/longitude_pm'][:]
    lon_pm = np.where(lon_pm==-9999.0,np.nan,lon_pm)

    SM = np.nanmean(np.dstack((SM_am,SM_pm)),axis=2)
    lat = np.nanmean(np.dstack((lat_am,lat_pm)),axis=2)
    lon = np.nanmean(np.dstack((lon_am,lon_pm)),axis=2)


    

    
    index = np.load('Data_Preprocessing/index_smap.npy') 
    
    SM = SM[45:132, 145:310].flatten()
    
    SMAP_1km = np.array(SM[index])

    
 
    
    np.save( path + 'SMAP_' + dateOFfile + '.npy', SMAP_1km)
def RF_implementation(date, dateList_MOD11A2, dateList_MOD13A2, DEM, lat_1km, lon_1km):
    stime = time.time()
    year, month, day = map(int, date.split('-'))
    date_datetime = datetime.date(year, month, day)



    nearest_date = nearest(dateList_MOD11A2, date_datetime)
    nearest_date = str(nearest_date)
    #print('Closest date for MOD11A2: ',nearest_date)
    MOD11A2 = np.load('./MOD11A2/' + nearest_date + '/LST.npy')

    print('./MOD11A2/' + nearest_date + '/LST.npy')
    
    nearest_date = nearest(dateList_MOD13A2, date_datetime)
    nearest_date = str(nearest_date)
    #print('Closest date for MOD13A2: ',nearest_date)
    MOD13A2 = np.load('./MOD13A2/' + nearest_date + '/NDVI14.npy')


    SMAP = np.load('./SMAP/SMAP_1km/SMAP_'+date.replace('-','')+'.npy')
    Imerg = np.load('./IMERG/IMERG_1km/Imerg_'+date.replace('-','')+'.npy')
    
    SPL3SMP_D = np.load('./SMAP/SMAP_1km/SMAP_'+date.replace('-','')+'.npy') * np.nan



    soils = ['C','CL','L','LS','O','OM','S','SCL','SIC','SICL','SIL','SL','W','BR']
    for soil_type in soils:
        print('Working on '+soil_type+ ' soil texture')
        soil = './Data_Preprocessing/Soil_textures/' + soil_type + '.csv'
        f = pd.read_csv(soil)
        ind = f['index_1km']-1
        if soil_type == 'W':
            i = ind.astype(int)
            SMAP[i] = 1
            continue
        elif soil_type == 'BR':
            i = ind.astype(int)
            SMAP[i] = 0.01            
            continue
        Mat = np.zeros([ind.shape[0],9])

        Mat[:,0] = ind
        Mat[:,1] = DEM[ind]
        Mat[:,2] = SMAP[ind]
        Mat[:,3] = Imerg[ind]
        Mat[:,4] = MOD11A2[ind]
        Mat[:,5] = MOD13A2[ind]
        Mat[:,6] = lat_1km[ind]
        Mat[:,7] = lon_1km[ind]

        # DEM data valid range control
        Mat = Mat[~np.isnan(Mat).any(axis=1)]

        # SMAP data valid range control
        Mat = Mat[np.invert(Mat[:,2]<=0)]
        Mat = Mat[np.invert(Mat[:,2]>1)]

        # IMERG data valid range control
        Mat = Mat[np.invert(Mat[:,3]<0)]

        # MOD11A2 data valid range control
        Mat = Mat[np.invert(Mat[:,4]< 7500 )]
        Mat = Mat[np.invert(Mat[:,4]> 65535 )]

        # MOD13A2 data valid range control
        Mat = Mat[np.invert(Mat[:,5]< -2000 )]
        Mat = Mat[np.invert(Mat[:,5]> 10000 )]

        X = Mat[:,1:6]
        if X.size == 0:
            continue
        loaded_model = pickle.load(open('./Data_Preprocessing/RFs/finalized_model_'+soil_type+'.sav', 'rb'))

        Mat[:,8] = loaded_model.predict(X)

        i = Mat[:,0].astype(int)
        SPL3SMP_D[i] = Mat[:,8] / 100
        
    np.save('./SMAP/SPL3SMP_D/SPL3SMP_D_' + date + '.npy', SPL3SMP_D)
    e=time.time()
    print('time for RF', e-stime)

def IMERG_par(file):
    IMEGR_DownScale(file)
    print(file,'was finished by',os.getpid())
    print('converting %s to netCDF4'%file)
    ConvertToNC(file[23:31], '/mh1/kgavahi/SMAP_Downscale_Operational/IMERG/IMERG_1km/Imerg_%s.npy'%file[23:31], 
                    '/mh1/kgavahi/SMAP_Downscale_Operational/IMERG/IMERG_1km/Imerg_%s.nc'%file[23:31],
                    'IMERG-L', np.nan, [0, 10000], False)
def SMOPS_par(file):
    SMOPS_DownScale(file)
    print(file,'was finished by',os.getpid())
def SMAP_par(file):
    print('uniform disagregation of %s was started by'%file, os.getpid())
    lat_1km = np.load('./Data_Preprocessing/lat14.npy')
    lon_1km = np.load('./Data_Preprocessing/lon14.npy')
    UP_vec = np.load('./Data_Preprocessing/UP_vec.npy')
    down_vec = np.load('./Data_Preprocessing/down_vec.npy')
    SMAP_to_1km( file , lat_1km , lon_1km , UP_vec , down_vec )
    print('uniform disagregation of %s was finished by'%file,os.getpid())
    
    
    dst = './SMAP/SMAP_1km/SMAP_%s.nc'%file[13:21]
    if os.path.exists(dst):
        print('%s already exists'%dst)
        return
    print('converting %s to netCDF4'%file)
    #ConvertToNC(file[13:21], './SMAP/SMAP_1km/SMAP_%s.npy'%file[13:21], dst, False)
    ConvertToNC(file[13:21], './SMAP/SMAP_1km/SMAP_%s.npy'%file[13:21], 
                    './SMAP/SMAP_1km/SMAP_%s.nc'%file[13:21],
                    'SPL3SMP_D', -9999.0, [0.02, 0.7], False)    
def RFImp_par(date):
    DEM = np.load('./Data_Preprocessing/DEM.npy') + 79
    lat_1km = np.load('./Data_Preprocessing/lat14.npy')
    lon_1km = np.load('./Data_Preprocessing/lon14.npy')
    UP_vec = np.load('./Data_Preprocessing/UP_vec.npy')
    down_vec = np.load('./Data_Preprocessing/down_vec.npy')
    all_dates_MOD11A2 = os.listdir('./MOD11A2')
    dateList_MOD11A2 = []
    for i in all_dates_MOD11A2:
        year, month, day = map(int, i.split('-'))
        d = datetime.date(year, month, day)
        dateList_MOD11A2.append(d)    
    
    
    
    all_dates_MOD13A2 = os.listdir('./MOD13A2')
    dateList_MOD13A2 = []
    for i in all_dates_MOD13A2:
        year, month, day = map(int, i.split('-'))
        d = datetime.date(year, month, day)
        dateList_MOD13A2.append(d)
    print('\nDownscaling SMAP@36 '+date)
    if os.path.exists('SMAP/SPL3SMP_D/SPL3SMP_D_%s.npy'%date):
        print('SPL3SMP_D_%s.npy already exists'%date)
        return
    RF_implementation(date, dateList_MOD11A2, dateList_MOD13A2, DEM, lat_1km, lon_1km)
    print('Converting to netCDF4')
    dst = 'SMAP/SPL3SMP_D/SPL3SMP_D_f06_1km_%s.nc'%date.replace('-','')
    #ConvertToNC(date, 'SMAP/SPL3SMP_D/SPL3SMP_D_%s.npy'%date, dst, True)
    ConvertToNC(date, 'SMAP/SPL3SMP_D/SPL3SMP_D_%s.npy'%date, 
                dst, 'SPL3SMP_D', -9999.0, [0.02, 0.7], True)    
def ConvertToNC(date, file, dst, VarName, NaNValue, ValidRange, Gfilter):
    
    if os.path.exists(dst):
        print(dst, 'already exists')
        return
    
    stime = time.time()
    src = 'Data_Preprocessing/Template_netCDF4.nc'
    #dst = 'SMAP/SPL3SMP_D/SPL3SMP_D_f06_1km_%s.nc'%date.replace('-','')
    shutil.copy(src, dst)

    nc_inds = np.load('Data_Preprocessing/nc_inds_2.npy')
    #SMAP_1km = np.load('SMAP/SPL3SMP_D/SPL3SMP_D_%s.npy'%date)
    SMAP_1km = np.load(file)
    f = Dataset(dst,'a')

    SMAP_1km[0] = NaNValue
    SM_D = np.zeros([2988,6946])


    for i in range(2988):
        for j in range(6946):

            SM_D[i,j] = SMAP_1km[nc_inds[i,j]]

    
    
    if Gfilter:
        s = 5 # standard deviations
        w = 9 # window size
        t = (((w - 1)/2)-0.5)/s
        SM_D = gaussian_filter(SM_D, sigma=s, truncate=t)
    
    

    SM_D = np.where(SM_D<ValidRange[0],np.nan,SM_D)
    SM_D = np.where(SM_D>ValidRange[1],np.nan,SM_D)
    f.variables['SPL3SMP_D'][:] = SM_D
    f.renameVariable(u'SPL3SMP_D', u'%s_%s'%(VarName, date))
    f.close()
    del f
    e=time.time()    
    print('time to convert ', e-stime)

def GetUrls(date_start, date_end, version):

    short_name = 'SPL3SMP'
    version = version
    time_start = date_start + 'T00:00:00Z'
    time_end = date_end + 'T00:00:00Z'
    polygon = ''
    filename_filter = '*'
    urls = cmr_search(short_name, version, time_start, time_end,
             polygon=polygon, filename_filter=filename_filter)    
    
    return urls

def CheckIfAvailForDownload(date, urls_7, urls_8):
    IsItThere = False
    
    
    for url in urls_7:
        if date in url:
            print('%s is available for downlad in version 007 and must be downloaded first to conitinue'%date)    
            IsItThere = True
    
    print('check version 008')

    
    for url in urls_8:
        if date in url:
            print('%s is available for downlad in version 008 and must be downloaded first to conitinue'%date)    
            IsItThere = True            
    
    
    
    
    return IsItThere

#
#if __name__ == '__main__':
def main_func(date_start, date_end):
    
    #date_start = '2019-02-28'
    #date_end   = '2019-03-01'
#
    DEM = np.load('./Data_Preprocessing/DEM.npy') + 79
    lat_1km = np.load('./Data_Preprocessing/lat14.npy')
    lon_1km = np.load('./Data_Preprocessing/lon14.npy')
    UP_vec = np.load('./Data_Preprocessing/UP_vec.npy')
    down_vec = np.load('./Data_Preprocessing/down_vec.npy')





    
    for product in ['MOD11A2', 'MOD13A2', 'IMERG']:
        print('\nDownloading ' + product + ' data...\n')
        download_run('********' , '**********' , date_start , date_end , product)
    
    
    print('\nDownloading ' + 'SMAP' + ' data...\n')
    SMAP_dl(date_start, date_end)

    
    for product in ['MOD11A2','MOD13A2']:
        print('\nMosaic and Clip MODIS ' + product + ' satellite data over CONUS\n')
        GatherAllTiles(product)


    
    dateRange = pd.date_range(date_start, date_end, freq='D')
    
    urls_7 = GetUrls(date_start, date_end, '007')
    urls_8 = GetUrls(date_start, date_end, '008')


    files = os.listdir('SMAP')
    files = [file for file in files if file.endswith('.h5')]
    files_SMAP = []
    for date in dateRange:
        date = str(date)[:10].replace('-','')
        file = [file for file in files if date in file]     
        print(file, 'line: 1093')
        if file== []:
            print('\nError: SMAP file for %s has not been downloaded\n'%date)
            print('check if it available for download...')
            

                
            if not CheckIfAvailForDownload(date, urls_7, urls_8):
                print(date, 'date is not in the SMAP repository')
                continue
                
        file = sorted(file)[-1]
            
        print(file, 'line: 1106')
        files_SMAP.append(file)
    
    
    print(files_SMAP)

    #for i in files_SMAP:
    #    SMAP_par(i)
        
        
    pool = multiprocessing.Pool(processes = 45)
    pool.map(SMAP_par, files_SMAP)


    
    dateRange = pd.date_range(date_start, date_end, freq='D')
    
    files = os.listdir('IMERG/GPM_3IMERGDL-06')
    files = [file for file in files if file.endswith('.nc4')]
    files_Imerg = []
    for date in dateRange:
        date = str(date)[:10].replace('-','')
        file = [file for file in files if date in file]     
        if file== []:
            print('\nError: IMERG file for %s has not been downloaded\n'%date)

        file = file[0]
            
            
        files_Imerg.append(file)

    
    print('im here')
    
    #for i in files_Imerg:
    #    IMERG_par(i) 

    #NumCores = 1

    pool = multiprocessing.Pool(processes = 45)
    pool.map(IMERG_par, files_Imerg)

            

    

    
############################################################################################
    all_dates_MOD11A2 = os.listdir('./MOD11A2')
    dateList_MOD11A2 = []
    for i in all_dates_MOD11A2:
        year, month, day = map(int, i.split('-'))
        d = datetime.date(year, month, day)
        dateList_MOD11A2.append(d)    
    
    
    
    all_dates_MOD13A2 = os.listdir('./MOD13A2')
    dateList_MOD13A2 = []
    for i in all_dates_MOD13A2:
        year, month, day = map(int, i.split('-'))
        d = datetime.date(year, month, day)
        dateList_MOD13A2.append(d)
    

    
    year, month, day = map(int, date_start.split('-'))
    start_date = datetime.date(year, month, day)

    year, month, day = map(int, date_end.split('-'))
    end_date = datetime.date(year, month, day)    
    
    
    today = start_date
    date_list = []
    while today <= end_date:
        '''--------------------------------------------------------------------------- '''
        if not os.path.exists('SMAP/SMAP_1km/SMAP_%s.npy'%str(today).replace('-','')):
            today = today + datetime.timedelta(days=1)
            print('Error: The SMAP uniform disagregated file does not exists.')
            continue
        '''--------------------------------------------------------------------------- '''
        date_list.append(str(today)) 
        today = today + datetime.timedelta(days=1)
    print(date_list)
    print('\nDownscaling SMAP@36 using RandomForestRegressor...\n')
    
    '''
    for date in date_list:
        print('\nDownscaling SMAP@36 '+date)
        if os.path.exists('SMAP/SPL3SMP_D/SPL3SMP_D_%s.npy'%date):
            print('SPL3SMP_D_%s.npy already exists'%date)
            continue
        RF_implementation(date, dateList_MOD11A2, dateList_MOD13A2, DEM,lat_1km, lon_1km)
        print('Converting to netCDF4')
        dst = 'SMAP/SPL3SMP_D/SPL3SMP_D_f06_1km_%s.nc'%date.replace('-','')
        #ConvertToNC(date, 'SMAP/SPL3SMP_D/SPL3SMP_D_%s.npy'%date, dst, True)
        ConvertToNC(date, 'SMAP/SPL3SMP_D/SPL3SMP_D_%s.npy'%date, 
                    dst, 'SPL3SMP_D', -9999.0, [0.02, 0.7], True)'''
                    
    pool = multiprocessing.Pool(processes = 45)
    pool.map(RFImp_par, date_list)                    
                    

    #all = np.zeros([lon_1km.shape[0],3])
    #SMAP_1km = np.load('SMAP_final_1km_2015-05-02.npy')    
    #all[:,0] = SMAP_1km
    #all[:,1] = lat_1km
    #all[:,2] = lon_1km
    #np.savetxt('all_F2.csv',all,delimiter=',')
#############################################################################################
    
if __name__ == '__main__':
    date_start = '2000-02-19'
    date_end   = '2000-04-23'   #Do not use overdue dates. Check the most recent dates for MOD11A2, MOD13A2, and IMERG and select one day behind

    #date_start = '2016-01-18'
    #date_end   = '2016-01-18'  

    main_func(date_start, date_end)


