import httplib, os, sqlite3, sys, urllib, logging, datetime, csv, tarfile, gzip #, geohash

_DBCONN = None
logger = logging.getLogger('gdd')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

def setup_environment():
    temperature_db = 'mostly.db'
    if not os.path.exists(temperature_db):
        logger.debug('creating mostly.db')
        create_temperature_database(temperature_db)
    global _DBCONN
    _DBCONN = sqlite3.connect(temperature_db)

def create_temperature_database (path):
    '''Create an sqlite database for storing temperature station data. Load the station
id and location information from the Global Historical Climate Network's data inventory file'''
    logger.debug('creating database: stations and readings')
    db_conn = sqlite3.connect(path)
    db_cursor = db_conn.cursor()
    db_cursor.execute('CREATE TABLE stations ("ID" VARCHAR(12) NOT NULL, "USAF" VARCHAR(6) NOT NULL, "WBAN" VARCHAR(5) NOT NULL, "NAME", "CTRY", "FIPS", "STATE", "CALL", "LAT" NOT NULL, "LON" NOT NULL, "GEOHASH", "ELEV", "BEGIN" VARCHAR(8), "END" VARCHAR(8), PRIMARY KEY (ID));')
    db_cursor.execute('CREATE TABLE readings ("ID" VARCHAR(12) NOT NULL, "USAF" VARCHAR(6) NOT NULL, "WBAN" VARCHAR(5) NOT NULL, "YEARMODA" INT(8) NOT NULL, "TEMP" REAL(6) NOT NULL, "DEWP" REAL(6) NOT NULL, "SLP" REAL(6) NOT NULL, "STP" REAL(6) NOT NULL, "VISIB" REAL(5) NOT NULL, "WDSP" REAL(5) NOT NULL, "MXSPD" REAL(5) NOT NULL, "GUST" REAL(5) NOT NULL, "MAX" REAL(6) NOT NULL, "MIN" REAL(6) NOT NULL, "PRCP" REAL(5) NOT NULL, "PRCP_FLAG" VARCHAR(1), "SNDP"  REAL(5) NOT NULL, "FRSHTT");')

    db_conn.commit()
    current_year = datetime.datetime.now().year

    '''
        USAF = Air Force Datsav3 station number
        WBAN = NCDC WBAN number
        CTRY = WMO historical country ID, followed by FIPS country ID
        ST = State for US stations
        CALL = ICAO call sign
        LAT = Latitude in thousandths of decimal degrees
        LON = Longitude in thousandths of decimal degrees
        ELEV = Elevation in tenths of meters
        BEGIN = Beginning Period Of Record (YYYYMMDD). There may be reporting gaps within the P.O.R.
        END = Ending Period Of Record (YYYYMMDD). There may be reporting gaps within the P.O.R.

        Notes:
        - Missing station name, etc indicate the metadata are not currently available.
        - The term "bogus" indicates that the station name, etc are not available.
        - For a small % of the station entries in this list, climatic data are not 
          available. These issues will be addressed. To determine data availability 
          for each location, see the 'ish-inventory.txt' or 'ish-inventory.csv' file. 
    '''
        
    stations = []
    logger.debug('loading station data from ncdc.noaa.gov')
    #response = http_get('www1.ncdc.noaa.gov', 80, '/pub/data/gsod/ish-history.csv')
    response = http_get('localhost', 8888, '/ish-history.csv')
    _response = response.split('\n')
    _response = _response[1:] # Remove first line with the CSV headers
    _stations = csv.reader(_response, delimiter=',', quotechar='"')

    for station in _stations:
        #print station

        try:
            USAF = station[0]
        except IndexError:
            continue

        # "USAF","WBAN","STATION NAME","CTRY","FIPS","STATE","CALL","LAT","LON","ELEV(.1M)","BEGIN","END"
        USAF = station[0]
        WBAN = station[1]
        ID   = '%s-%s' %(USAF,WBAN)
        NAME = station[2]
        CTRY = station[3]
        FIPS = station[4]
        STATE = station[5]
        CALL = station[6]
        LAT = station[7]
        LON = station[8]
        ELEV = station[9]
        BEGIN = station[10]
        END = station[11]

        if LAT == '-99999' or LON == '-99999' or BEGIN == 'NO DATA' or CTRY == '':
            continue
        if ELEV == '-99999':
            ELEV = None

        if LAT == '+00000' and LON == '+00000':
            continue

        stations.append((ID,USAF,WBAN,NAME,CTRY,FIPS,STATE,CALL,LAT,LON,ELEV,BEGIN,END))

    logger.debug('loaded %s stations into DB' % len(stations))
    db_cursor.executemany('INSERT INTO stations (ID,USAF,WBAN,NAME,CTRY,FIPS,STATE,CALL,LAT,LON,ELEV,"BEGIN",END) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', stations)
    db_conn.commit()
    db_cursor.close()

def read_file (path):
    '''Create an sqlite database for storing temperature station data. Load the station
id and location information from the Global Historical Climate Network's data inventory file'''
    
    #logger.debug('reading data from file %s' %path)
    db_conn = _DBCONN
    db_cursor = db_conn.cursor()
        
    readings = []
    _response = gzip.open(path, 'rb').readlines()
    _response = _response[1:] # Remove first line with the headers

    for l in _response:
       USAF = l[0:6].strip(' \t\n\r')
       WBAN = l[7:12].strip(' \t\n\r')
       ID = "%s-%s" %(USAF,WBAN)
       YEARMODA = l[14:22].strip(' \t\n\r')
       TEMP = l[24:29].strip(' \t\n\r')
       DEWP = l[35:40].strip(' \t\n\r')
       SLP = l[46:51].strip(' \t\n\r')
       STP = l[57:62].strip(' \t\n\r')
       VISIB = l[68:72].strip(' \t\n\r')
       WDSP = l[78:82].strip(' \t\n\r')
       MXSPD = l[88:92].strip(' \t\n\r')
       GUST = l[95:99].strip(' \t\n\r')
       MAX = l[102:107].strip(' \t\n\r')
       MIN = l[110:115].strip(' \t\n\r')
       PRCP = l[118:122].strip(' \t\n\r')
       PRCP_FLAG = l[123:124].strip(' \t\n\r')
       SNDP =l[125:129].strip(' \t\n\r')
       FRSHTT = l[132:137].strip(' \t\n\r')

       if TEMP == '9999.9' or TEMP == 9999.9:
            continue

       if MAX == '9999.9' or MAX == 9999.9:
            continue

       if MIN == '9999.9' or MIN == 9999.9:
            continue

       readings.append((ID,USAF,WBAN,YEARMODA,TEMP,DEWP,SLP,STP,VISIB,WDSP,MXSPD,GUST,MAX,MIN,PRCP,PRCP_FLAG,SNDP,FRSHTT))

    #logger.debug('loaded %s readings' % len(readings))
    db_cursor.executemany('INSERT INTO readings (ID,USAF,WBAN,YEARMODA,TEMP,DEWP,SLP,STP,VISIB,WDSP,MXSPD,GUST,MAX,MIN,PRCP,PRCP_FLAG,SNDP,FRSHTT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', readings)
    db_conn.commit()
    db_cursor.close()

# 
def read_year(year):
    if not os.path.exists('gsod_%s.tar' % year ):
        logger.debug('gsod_%s.tar not found on local file system, downloading from NOAA' % year)
        http_get_to_file('ftp://ftp.ncdc.noaa.gov/pub/data/gsod/%s/gsod_%s.tar' %(year, year), 'gsod_%s.tar' %year)
    else:
        logger.debug('gsod_%s.tar found on local file system, skipping download' % year)

    logger.debug("unpacking gsod_%s.tar" % year)
    tar = tarfile.open('gsod_%s.tar' % year)
    tar.extractall('gsod_%s' %year)

    logger.debug("reading weather data")
    for filename in os.listdir('gsod_%s' %year):
        #logger.debug("reading %s into DB" %filename )
        read_file('gsod_%s/%s' %(year,filename))

    logger.debug("year %s imported successfully" %year)


def main (argv=None):
    setup_environment()
    #read_file('gsod_2011/024850-99999-2011.op.gz')
    read_year(2011)
    #read_year(2009)
    #read_year(2010)
    #read_year(2011)
    #read_file('024600-99999-2011.op.op')

    #select AVG(MIN), AVG(MAX), AVG(TEMP) from readings where ID = '024840-99999' and YEARMODA > 20110501 and YEARMODA < 20110601

def http_get_to_file(url, file):
    urllib.urlretrieve(url, file)

def http_get (host, port, path):
    conn = httplib.HTTPConnection(host, port)
    conn.request('GET', path)
    response = conn.getresponse()
    if response.status == 200:
        return response.read()
    else:
        raise httplib.HTTPException()

if __name__ == "__main__":
    status = main(sys.argv[1:])
    sys.exit(0)