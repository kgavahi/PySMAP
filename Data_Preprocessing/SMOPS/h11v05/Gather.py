import numpy as np
from netCDF4 import Dataset
import os
import time
import pandas as pd
from pyhdf.SD import SD, SDC

tile_coor = 'h11v05'
FILE_NAME = 'C:/Users/16785/Desktop/SMAP_Resample/tk_MODIS_download/version-20190619/MOD13A2.A2017001.h11v05.006.2017020215922.hdf'
SMOPS_FILE = 'C:/Google Drive/University of Alabama/How To Do/SMOPS_data_retriever/CONUS/20170315.nc'

#dataFrame = pd.read_csv('MOD_ind.csv')
#np.save('MOD_ind.npy',dataFrame.pointid)

ind_cliped = np.load('MOD_ind.npy') - 1 

dataFrame = pd.read_csv('SMOPS_ind.csv')
np.save('SMOPS_ind.npy',dataFrame.pointid)

ind_cliped_SMOPS = np.load('SMOPS_ind.npy') - 1 




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



f_SMOPS = Dataset( SMOPS_FILE , 'r')
SMOPS_SM = f_SMOPS.variables['Blended_SM'][:]
lat_SM = f_SMOPS.variables['latitude'][:]
lon_SM = f_SMOPS.variables['longitudes'][:]
np.savetxt('smops.csv',SMOPS_SM.T,delimiter=',')

lat_SM = np.repeat(lat_SM, 241, axis=0)
lat_SM = np.reshape(lat_SM,(112,241))

lon_SM = np.repeat(lon_SM, 112, axis=0)
lon_SM = np.reshape(lon_SM,(241,112))
lon_SM = lon_SM.T

SMOPS_SM = SMOPS_SM.T
print(lat_SM)
print(lon_SM)
print(SMOPS_SM)
lat_SM = lat_SM.flatten()
lon_SM = lon_SM.flatten()
SMOPS_SM = SMOPS_SM.flatten()


lat_in_CONUS_IMERG = lat_SM[ind_cliped_SMOPS]
lon_in_CONUS_IMERG = lon_SM[ind_cliped_SMOPS]
IMERG_precp_in_CONUS = SMOPS_SM[ind_cliped_SMOPS]

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


print(lat_in_CONUS[45000])
print(lon_in_CONUS[45000])
print(IMERG_1km[45000])
print(LST_in_CONUS[45000])



