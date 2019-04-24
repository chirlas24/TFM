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

def date_process(date):
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