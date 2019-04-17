###############################################################################
## Funciones de preprocesamiento de los datos para construccion del database ##
###############################################################################

#Funcion para extraccion del ID del viaje
def get_id(dct): return dct['$oid'] 

#Dado que el feature 'Track' esta en formato GeoJSON que es un formato
#estandarizado, se decide hacer un objeto manteniendo el formato para esta feature.

class GeoJSON(object):
    
    def __init__(self, track):
        self.track = track
     
		#Extraccion del numero de localizaciones cargadas durante trayecto	
    def no_points(self):
        return len(self.track['features'])
		
		#Extraccion de las coordinadas de cada punto		
    def coordinates(self):
        coordinates_list = []
        for d in self.track['features']:
            coordinates_list.append(d['geometry']['coordinates'])
        return coordinates_list
				
		#Extraccion de las direcciones de cada punto	    
    def adresses(self):
        adresses_list = []
        for d in self.track['features']:
            adresses_list.append(d['properties']['var'])
        return adresses_list
 
		#Extraccion de los codigos postales de cada punto	    			
    def postal_code(self):
        cp_list = []
        for d in self.track['features']:
            adress = d['properties']['var']
            adress_list = adress.split(",")
            cp_list.append(adress_list[0])
        return cp_list

		#Extraccion de las calles de cada punto	    			
    def streets(self):
        streets_list = []
        for d in self.track['features']:
            adress = d['properties']['var']
            adress_list = adress.split(",")
            streets_list.append(adress_list[4])
        return streets_list
				
		#Extraccion de las velocidades en cada punto	    	    
    def speed(self):
        speed_list = []
        for d in self.track['features']:
            speed_list.append(d['properties']['speed'])
        return speed_list
				
		#Extraccion del tiempo de trayecto en cada punto	
    def time(self):
        time_list = []
        for d in self.track['features']:
            time_list.append(d['properties']['secondsfromstart'])
        return time_list

from datetime import datetime

def date_process(date):
    data_aux = date['$date'].split("T")
    time_aux = data_aux[1].split(":")
    date_list = [data_aux[0], time_aux[0]]
    date_string = ("-").join(date_list)
    date_datetime = datetime.strptime(date_string, '%Y-%m-%d-%H')
    return date_datetime				