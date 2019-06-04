###############################################################################
## Funciones de preprocesamiento de los datos para construccion del database ##
###############################################################################
import pandas as pd
from datetime import datetime
import os

def date_process_stations(date):
    '''Convert date format of the data set into date_datetime format'''
    data_aux = date.split("T")
    time_aux = data_aux[1].split(":")
    date_list = [data_aux[0], time_aux[0], time_aux[1]]
    date_string = ("-").join(date_list)
    date_datetime = datetime.strptime(date_string, '%Y-%m-%d-%H-%M')
    return date_datetime

def make_dataset(df, value, verbose=True):
    '''Make a times-series flat dataset with one type of data (value) of the dictionary.
        i.e. date vs activation of the station'''
    
    def no_available(x):
        if x[j]['no_available'] == 0:
            return x[j][value]
        else:
            return 0
    
    df_aux = df.copy()
    
    list_stations = df_aux['stations'][0]
    
    for j, station in enumerate(list_stations):
        
        if verbose:
            if j == 0:
                verbose_fun(j+1, len(list_stations), first=True, last=False)
            elif j+1 == len(list_stations):
                verbose_fun(j+1, len(list_stations), first=False, last=True)
            else:
                verbose_fun(j+1, len(list_stations), first=False, last=False)
        
        
        station_id = str(station['id']) + '-' + station['number']
        
        if station_id in ['23-21a', '24-21b', '37-33', '45-41']:
            pass
        else:
            df_aux[station_id + '_' + value] = df['stations'].map(no_available)

    df_aux.drop('stations', axis=1, inplace=True)
    
    return df_aux

def verbose_fun(part, total, first=True, last=False):
    '''This function is complementary to long calculation functions
        in order to visualise the process'''
    if first: 
        global _10
        global _20
        global _30
        global _40
        global _50
        global _60
        global _70
        global _80
        global _90
        _10 = True
        _20 = True
        _30 = True
        _40 = True
        _50 = True
        _60 = True
        _70 = True
        _80 = True
        _90 = True
    if part/total >= 0.1 and _10:
        print("0%[--10%------------------]100%")
        _10 = False
    if part/total >= 0.2 and _20:
        print("0%[----20%----------------]100%")
        _20 = False
    if part/total >= 0.3 and _30:
        print("0%[------30%--------------]100%")
        _30 = False
    if part/total >= 0.4 and _40:
        print("0%[--------40%------------]100%")
        _40 = False
    if part/total >= 0.5 and _50:
        print("0%[----------50%----------]100%")
        _50 = False
    if part/total >= 0.6 and _60:
        print("0%[------------60%--------]100%")
        _60 = False
    if part/total >= 0.7 and _70:
        print("0%[--------------70%------]100%")
        _70 = False
    if part/total >= 0.8 and _80:
        print("0%[----------------80%----]100%")
        _80 = False
    if part/total >= 0.9 and _90:
        print("0%[------------------90%--]100%")
        _90 = False
    if last:
        print("DATAFRAME COMPLETED")

def load_dict_from_file(path):
    '''For loading the Postal Codes dictionary of the statios'''
    path = os.path.join(path, 'postal_codes.txt' )
    f = open(path,'r')
    data=f.read()
    f.close()
    return eval(data)

def make_dataset_by_postal_code(df, value, postal_codes_dict, verbose=False):
    '''Make a times-series flat dataset with one type of data (value) of the dictionary.
        i.e. date vs activation of the station'''
    
    df_aux = df.copy()
    
    def no_available(x):
        if x[j]['no_available'] == 0:
            return x[j][value]
        else:
            return 0
    
    
    list_stations = df_aux['stations'][0]
    
    for j, station in enumerate(list_stations):
        
        if verbose:
            if j == 0:
                verbose_fun(j+1, len(list_stations), first=True, last=False)
            elif j+1 == len(list_stations):
                verbose_fun(j+1, len(list_stations), first=False, last=True)
            else:
                verbose_fun(j+1, len(list_stations), first=False, last=False)
        
        
        station_id = str(station['id']) + '-' + station['number']
    
        for postal_code in postal_codes_dict:

            if station_id in postal_codes_dict[postal_code]:
                column_name = postal_code + '_' + value
                if column_name in ['23-21a', '24-21b', '37-33', '45-41']:
                    pass
                elif column_name not in df_aux.columns:
                    df_aux[column_name] = df['stations'].map(no_available)
                    break
                else:
                    df_aux[column_name] = df_aux[column_name] + df['stations'].map(no_available)
                    break

    df_aux.drop('stations', axis=1, inplace=True)
    
    return df_aux
		
def make_all_dataset(data_path, value, by_postal_code=False, verbose=True):
    '''For making the whole dataset with the all .json files'''

    df_all = pd.DataFrame()
    
    data_list = os.listdir(data_path)
    data_list = list(filter(lambda x: '.json' in x, data_list))

    for i, file in enumerate(data_list):

        if verbose:
                if i == 0:
                    verbose_fun(i+1, len(data_list), first=True)
                else:
                    verbose_fun(i+1, len(data_list), first=False)

        path_aux = os.path.join(data_path ,file)
        df_aux = pd.read_json(path_aux, lines=True)

        df_aux['Date'] = df_aux['_id'].map(date_process_stations)
        df_aux.drop('_id', inplace=True, axis=1)
        
        if by_postal_code:
            postal_codes_dict = load_dict_from_file(data_path)
            df_aux = make_dataset_by_postal_code(df_aux, value, postal_codes_dict, verbose=False)
        else:
            df_aux = make_dataset(df_aux, value, verbose=False)

        if df_all.empty:
            df_all = df_aux
        else:
            df_all = pd.concat([df_all, df_aux])

        if verbose:
            print("=============================")
            print(file, "added to DataSet")
            print("=============================")
    
    df_all.sort_values('Date', ascending = True, inplace = True)
    df_all.reset_index(inplace=True, drop=True)
    return df_all

def total_bases_dataset(data_path, by_postal_code=True):
    df_dock_bikes_pc = make_all_dataset(data_path, 'dock_bikes', by_postal_code=by_postal_code, verbose=False)
    df_free_bases_pc = make_all_dataset(data_path, 'free_bases', by_postal_code=by_postal_code, verbose=False)
    np_total_bases = df_dock_bikes_pc[df_dock_bikes_pc.columns[1:]].values + df_free_bases_pc[df_free_bases_pc.columns[1:]].values
    total_bases = pd.DataFrame(np_total_bases)
    columns = list(map(lambda x: x.split("_")[0] + "_" + "total_bases", df_dock_bikes_pc.columns[1:]))
    total_bases.columns = columns
    columns = ['Date'] + columns
    total_bases['Date'] = df_free_bases_pc['Date']
    total_bases = total_bases[columns]
    return total_bases

def make_df_occupation_rate(df_dock_bikes, df_total_bases):
    df_dock_bikes.set_index('Date', inplace=True)
    df_total_bases.set_index('Date', inplace=True)
    df_dock_bikes = df_dock_bikes.resample('D').mean()
    df_total_bases = df_total_bases.resample('D').mean()
    df_dock_bikes_np = df_dock_bikes[df_dock_bikes.columns].values
    df_total_bases_np = df_total_bases[df_total_bases.columns].values
    df_occupation_rate_np = df_dock_bikes_np / df_total_bases_np
    df_occupation_rate = pd.DataFrame(df_occupation_rate_np)
    columns = [x.split("_")[0] + "_" + "OccupationRate" for x in df_dock_bikes.columns]
    df_occupation_rate.columns = columns
    df_occupation_rate.index = df_dock_bikes.index
    return df_occupation_rate
