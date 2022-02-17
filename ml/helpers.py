from pyspark.sql.types import StringType, FloatType
from math import radians, cos, sin, asin, sqrt

def switch_tr_day(day_index):
    my_dict = {
        1: 'Pazar',
        2: 'Pazartesi',
        3: 'Salı',
        4: 'Çarşamba',
        5: 'Perşembe',
        6: 'Cuma',
        7: 'Cumartesi'
    }
    
    return my_dict.get(day_index)

def switch_month_day(month_index):
    my_dict = {
        1: 'Ocak',
        2: 'Subat',
        3: 'Mart',
        4: 'Nisan',
        5: 'Mayis',
        6: 'Haziran',
        7: 'Temmuz',
        8: 'Agustos',
        9: 'Eylul',
        10: 'Ekim',
        11: 'Kasim',
        12: 'Aralik'
    }
    
    return my_dict.get(month_index)

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance in kilometers between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
    return c * r