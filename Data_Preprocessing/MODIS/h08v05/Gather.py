import numpy as np
from netCDF4 import Dataset
import os
from pyhdf.SD import SD, SDC
import time
import pandas as pd


tile_coor = 'h08v05'
FILE_NAME = 'C:/Users/16785/Desktop/SMAP_Resample/tk_MODIS_download/version-20190619/MOD13A2.A2017001.h08v05.006.2017020215925.hdf'
IMERG_FILE = 'C:/Users/16785/Desktop/SMAP_Resample/pyhdf/3B-DAY.MS.MRG.3IMERG.20170101-S000000-E235959.V05.nc4'

dataFrame = pd.read_csv('MOD_ind.csv')
np.save('MOD_ind.npy',dataFrame.pointid)

ind_cliped = np.load('MOD_ind.npy') - 1 

dataFrame = pd.read_csv('Imerg_ind.csv')
np.save('Imerg_ind.npy',dataFrame.pointid)

ind_cliped_IMERG = np.load('Imerg_ind.npy') - 1 




print('\n\n\n\n')

hdf = SD(FILE_NAME, SDC.READ)
#print(hdf.datasets())
DATAFIELD_NAME = '1 km 16 days NDVI'
data3D = hdf.select(DATAFIELD_NAME)

LST_Day_1km = data3D.get()

tile = 'C:/Users/16785/Desktop/SMAP_Resample/TILES/'+ tile_coor +'.npy'
lon = np.load(tile)[0]
lat = np.load(tile)[1]


lat_in_CONUS = lat.flatten()[ind_cliped]
lon_in_CONUS = lon.flatten()[ind_cliped]
LST_in_CONUS = LST_Day_1km.flatten()[ind_cliped]



f_IMERG = Dataset( IMERG_FILE , 'r')
IMERG_precp = f_IMERG.variables['precipitationCal'][:]

IMERG_precp = np.flip(IMERG_precp,axis=1).T

lat_IMERG = f_IMERG.variables['lat'][:]
lat_IMERG = np.repeat(lat_IMERG, 3600, axis=0)
lat_IMERG = np.reshape(lat_IMERG, (1800,3600))
lat_IMERG = np.transpose(lat_IMERG)

lon_IMERG = f_IMERG.variables['lon'][:]
lon_IMERG = np.repeat(lon_IMERG, 1800, axis=0)
lon_IMERG = np.reshape(lon_IMERG, (3600,1800))

lon_IMERG = lon_IMERG.flatten('F')

lat_IMERG = np.flip(lat_IMERG).flatten('F')


lat_in_CONUS_IMERG = lat_IMERG[ind_cliped_IMERG]
lon_in_CONUS_IMERG = lon_IMERG[ind_cliped_IMERG]
IMERG_precp_in_CONUS = IMERG_precp.flatten()[ind_cliped_IMERG]


s = time.time()
ind = np.zeros([len(lat_in_CONUS)],dtype=int)
for i in range(len(lat_in_CONUS)):
	d = np.sqrt((lat_in_CONUS[i] - lat_in_CONUS_IMERG) ** 2 + (lon_in_CONUS[i] - lon_in_CONUS_IMERG) ** 2)
	ind[i] = np.argmin(d)
	if i%1000==0: print('\t\t\t\t\t\t\t',i)

np.save('overlap_ind.npy',ind)
e = time.time()
print(e-s)

IMERG_range = np.arange(len(IMERG_precp_in_CONUS))

IMERG_1km = np.load('overlap_ind.npy')
s = time.time()
for i in range(len(IMERG_precp_in_CONUS)):
	#print(i)
	IMERG_1km = np.where(IMERG_1km==IMERG_range[i],IMERG_precp_in_CONUS[i],IMERG_1km)
e = time.time()
print(e-s)


print(lat_in_CONUS[4500])
print(lon_in_CONUS[4500])
print(IMERG_1km[4500])
print(LST_in_CONUS[4500])




