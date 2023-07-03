from mpl_toolkits.basemap import Basemap
from netCDF4 import Dataset
import matplotlib as mpl
from scipy import stats
import numpy as np
import copy, os
import time
import shutil
import h5py as h5py
def ConvertToNC(date, file, dst, VarName, NaNValue, ValidRange, Gfilter):
    stime = time.time()
    src = 'Template_netCDF4.nc'
    #dst = 'SMAP/SPL3SMP_D/SPL3SMP_D_f06_1km_%s.nc'%date.replace('-','')
    shutil.copy(src, dst)

    nc_inds = np.load('nc_inds_2.npy')
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

date = '20220208'
path_file = '/mh1/kgavahi/SMAP_Downscale_Operational/SMAP/SMAP_L3_SM_P_%s_R18240_001.h5'%date

files = os.listdir('/mh1/kgavahi/SMAP_Downscale_Operational/SMAP')
files = [x for x in files if x.endswith('.h5')]
print(files)

LAT = np.zeros([406, 964]) * np.nan
LON = np.zeros([406, 964]) * np.nan


for file in files[:20]:
    dataset = h5py.File('/mh1/kgavahi/SMAP_Downscale_Operational/SMAP/'+file, 'r')
    name = '/Soil_Moisture_Retrieval_Data_AM/soil_moisture'

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
    
    LAT = np.dstack((LAT, lat))
    LON = np.dstack((LON, lon))
    
    
    print(file)
adad
    
lat = np.nanmean(LAT, axis=2)
lon = np.nanmean(LON, axis=2)

    
lat_smap = lat[45:132, 145:310]
lon_smap = lon[45:132, 145:310]

np.savetxt('lat_smap.csv', lat_smap)
np.savetxt('lon_smap.csv', lon_smap)


lat_mdl = np.load('/mh1/kgavahi/SMAP_Downscale_Operational/Data_Preprocessing/lat14.npy')
lon_mdl = np.load('/mh1/kgavahi/SMAP_Downscale_Operational/Data_Preprocessing/lon14.npy')


lat_smap = lat_smap.flatten()
lon_smap = lon_smap.flatten()
SM = SM[45:132, 145:310].flatten()


N = len(lat_mdl)
index = np.zeros([len(lat_mdl)], dtype='int')
s = time.time()
for i in range(N):

    d = np.sqrt((lat_smap - lat_mdl[i])**2 + (lon_smap - lon_mdl[i])**2)
    index[i] = np.where(d==d.min())[0][0]
    
    #print(i, index[i])

    #print('smap: ', lat_smap[index[i]], lon_smap[index[i]], SM[index[i]])
    #print('mdl: ', lat_mdl[i], lon_mdl[i])
print((time.time()-s)*len(lat_mdl)/N/3600)


np.save('index_smap', index)   

index = np.load('index_smap.npy')    
SM_mdl = np.zeros([len(lat_mdl)])*np.nan
SM_mdl = SM[index]

#print(IMERG_mdl[99])

np.save('SM_mdl_test', np.array(SM_mdl))

ConvertToNC(date, 'SM_mdl_test.npy', 'SM_mdl_test.nc', 'SPL3SMP_D', -9999.0, [0.02, 0.7], False)


'''
path_file = '/mh1/kgavahi/SMAP_Downscale_Operational/IMERG/GPM_3IMERGDL-06/3B-DAY-L.MS.MRG.3IMERG.%s-S000000-E235959.V06.nc4'%date
f = Dataset(path_file, 'r')
lat = f.variables['lat'][:]
lon = f.variables['lon'][:]
IMERG = f.variables['precipitationCal'][0].T
IMERG = np.flip(IMERG, axis=0)


lon_imerg, lat_imerg = np.meshgrid(lon, lat)
lat_imerg = np.flip(lat_imerg, axis=0)

#print(lat_imerg.shape)
#print(lat_imerg[397:660])

#print(lon_imerg.shape)
#print(lon_imerg[397:660, 550:1150])

lon_imerg = lon_imerg[397:660, 550:1150]
lat_imerg = lat_imerg[397:660, 550:1150]
IMERG = IMERG[397:660, 550:1150]



lat_imerg = lat_imerg.flatten()
lon_imerg = lon_imerg.flatten()
IMERG = IMERG.flatten()


lat_mdl = np.load('/mh1/kgavahi/SMAP_Downscale_Operational/Data_Preprocessing/lat14.npy')
lon_mdl = np.load('/mh1/kgavahi/SMAP_Downscale_Operational/Data_Preprocessing/lon14.npy')'''

'''
N=len(lat_mdl)
index = np.zeros([len(lat_mdl)], dtype='int')
s = time.time()
for i in range(N):

    d = np.sqrt((lat_imerg - lat_mdl[i])**2 + (lon_imerg - lon_mdl[i])**2)
    index[i] = np.where(d==d.min())[0][0]
    
    print(i, index[i])

    print('imerg: ', lat_imerg[index[i]], lon_imerg[index[i]], IMERG[index[i]])
    print('mdl: ', lat_mdl[i], lon_mdl[i])
print((time.time()-s)*len(lat_mdl)/N/3600/24)

np.save('index_imerg', index)'''    

'''index = np.load('index_imerg.npy')    
IMERG_mdl = np.zeros([len(lat_mdl)])*np.nan
IMERG_mdl = IMERG[index]

#print(IMERG_mdl[99])

np.save('IMERG_mdl_test', np.array(IMERG_mdl))'''

#ConvertToNC(date, '/mh1/kgavahi/SMAP_Downscale_Operational/MOD13A2/2015-04-07/NDVI14.npy', 'NDVI_test.nc', False)



    
    
    
'''
index = np.zeros([239, 399],dtype='int')
for i in range(239):
	for j in range(399):
		d = np.sqrt((lat_nldas - lat_mdl[i, j])**2 + (lon_nldas - lon_mdl[i, j])**2)
		print(np.where(d==d.min()))
		
		index[i, j] = np.where(d==d.min())[0][0]
		
np.save('index_1km.npy',index)'''
'''
index = np.load('index_1km.npy')

files = os.listdir('weeks')




for file in files:
	SM = np.load('weeks/%s'%file)
	SM_mdl = np.zeros([41, 239, 399])*np.nan
	
	print(SM.shape)
	for i in range(41):
		SM_mdl[i] = SM[i].flatten()[index]
	
	np.save('weeks_domain/%s_domain.npy'%file[:-4], SM_mdl)'''
	






		