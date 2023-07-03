from mpl_toolkits.basemap import Basemap
from netCDF4 import Dataset
import matplotlib as mpl
from scipy import stats
import numpy as np
import copy, os
import time
import shutil
def ConvertToNC(date, file, dst, VarName, NaNValue, ValidRange, Gfilter):
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

date = '20220131'

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
lon_mdl = np.load('/mh1/kgavahi/SMAP_Downscale_Operational/Data_Preprocessing/lon14.npy')

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

index = np.load('index_imerg.npy')    
IMERG_mdl = np.zeros([len(lat_mdl)])*np.nan
IMERG_mdl = IMERG[index]


np.save('IMERG_mdl_test', np.array(IMERG_mdl))

#ConvertToNC(date, '/mh1/kgavahi/SMAP_Downscale_Operational/MOD13A2/2015-04-07/NDVI14.npy', 'NDVI_test.nc', False)



    
    

	






		