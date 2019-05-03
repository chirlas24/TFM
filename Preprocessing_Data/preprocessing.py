###############################################################################
## Funciones de preprocesamiento de los datos para construccion del database ##
###############################################################################

def get_id(dct):
    ''' Function for getting the id of a bike track'''
    return dct['$oid'] 

class GeoJSON(object):
    '''Class for the tracks locations reports of bikes'''
    
    def __init__(self, track):
        self.track = track
     
    def no_points(self):
        '''times of reports of each track'''
        return len(self.track['features'])
    
    def coordinates(self):
        '''coordinates of the location'''
        coordinates_list = []
        for d in self.track['features']:
            coordinates_list.append(d['geometry']['coordinates'])
        return coordinates_list

    def adresses(self):
        '''Adress of the location'''
        adresses_list = []
        for d in self.track['features']:
            adresses_list.append(d['properties']['var'])
        return adresses_list
 
    def postal_code(self):
        '''Postal code of the location'''
        cp_list = []
        for d in self.track['features']:
            adress = d['properties']['var']
            adress_list = adress.split(",")
            cp_list.append(adress_list[0])
        return cp_list

    def streets(self):
        '''Street of the location'''
        streets_list = []
        for d in self.track['features']:
            adress = d['properties']['var']
            adress_list = adress.split(",")
            streets_list.append(adress_list[4])
        return streets_list

    def speed(self):
        '''Speed at the moment of the report'''
        speed_list = []
        for d in self.track['features']:
            speed_list.append(d['properties']['speed'])
        return speed_list

    def time(self):
        '''Time in seconds of the duration of the track'''
        time_list = []
        for d in self.track['features']:
            time_list.append(d['properties']['secondsfromstart'])
        return time_list

from datetime import datetime

def date_process_tracks(date):
    '''Convert date format of the data set into date_datetime format'''
    data_aux = date['$date'].split("T")
    time_aux = data_aux[1].split(":")
    date_list = [data_aux[0], time_aux[0]]
    date_string = ("-").join(date_list)
    date_datetime = datetime.strptime(date_string, '%Y-%m-%d-%H')
    return date_datetime
		
class station_status(object):
    
    def __init__(self, state):
        self.state = state
     
    def activate(self):
        '''Returns if the station is activated or it is not'''
        return self.state['activate']
    
    def name(self):
        '''Returns the name of the station'''
        return self.state['name']
    
    def reservations_count(self):
        '''Returns the reservetations booked at the time of the report'''
        return self.state['reservations_count']

    def light(self):
        '''Returns the ocuppation state of the station:
            0=LOW
            1=MEDIUM
            3=HIGHT'''
        return self.state['light']
    
    def total_bases(self):
        '''Number of slots for bikes in the station
        at the time of the report'''
        return self.state['total_bases']
    
    def free_bases(self):
        '''Number of free slots for bikes in the station
        at the time of the report''' 
        return self.state['free_bases']
    
    def number(self):
        '''Logic number asigned to the station''' 
        return self.state['number']
    
    def longitude(self):
        '''Longitude of the location'''
        return self.state['longitude']
    
    def no_available(self):
        '''Returns if the station is avaliable or it is not'''
        return self.state['no_available']
    
    def address(self):
        '''Adress of the location of the station'''
        return self.state['address']
    
    def latitude(self):
        '''Latitude of the location'''
        return self.state['latitude']
    
    def dock_bikes(self):
        '''Number of bikes which are connected to the station'''
        return self.state['dock_bikes']
    
    def id_(self):
        '''Code for the station'''
        return self.state['id']
				
def make_dataset(df, value, verbose=False):
    '''Make a times-series flat dataset with one type of data (value) of the dictionary.
        i.e. date vs activation of the station'''
    
    df_aux = df.copy()
    
    for i, report_list_time in enumerate(df_aux['stations']):
        
        if verbose:
            if i == 0:
                verbose_fun(i+1, len(df_aux['stations']), first=True)
            else:
                verbose_fun(i+1, len(df_aux['stations']), first=False)

        
        for j, station in enumerate(df_aux['stations'][i]):
            station_id = str(df['stations'][i][j]['id']) + '-' + df['stations'][i][j]['number']
            df_aux[station_id + '_' + value] = df['stations'].map(lambda x : x[j][value])
    
    df_aux.drop('stations', axis=1, inplace=True)
    
    return df_aux

def verbose_fun(part, total, first=True):
    '''This function is complementary to long calculation functions
        in order to visulize the process'''
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
    if part/total > 0.9 and not(_90):
        print("DATAFRAME COMPLETED")
				
def date_process_stations(date):
    '''Convert date format of the data set into date_datetime format'''
    data_aux = date.split("T")
    time_aux = data_aux[1].split(":")
    date_list = [data_aux[0], time_aux[0], time_aux[1]]
    date_string = ("-").join(date_list)
    date_datetime = datetime.strptime(date_string, '%Y-%m-%d-%H-%M')
    return date_datetime
		
def make_all_dataset(data_list, value, verbose=True):

    df_all = pd.DataFrame()

    for i, file in enumerate(data_list):

        if verbose:
                if i == 0:
                    verbose_fun(i+1, len(data_list), first=True)
                else:
                    verbose_fun(i+1, len(data_list), first=False)

        path_aux = os.path.join('DATA' ,file)
        df_aux = pd.read_json(path_aux, lines=True)

        df_aux['Date'] = df_aux['_id'].map(date_process_stations)
        df_aux.drop('_id', inplace=True, axis=1)

        df_aux = make_dataset(df_aux, value)

        if df_all.empty:
            df_all = df_aux
        else:
            df_all = pd.concat([df_all, df_aux])

        if verbose:
            print("=============================")
            print(file, "added to DataSet")
            print("=============================")
    
    return df_all