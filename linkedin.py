import urllib
from xml.dom import minidom
import MySQLdb

WS_URL = 'http://www.linkedin.com/wsSchoolDir?q=&country=%s'
DB_HOST = 'localhost'
DB_NAME = 'bell'
DB_USER = 'root'
DB_PASSWD = '123456'
TBL_COUNTRY = 'countries'
COUNTRY_ID = 'id'
COUNTRY_CODE = 'iso2'
TBL_SCHOOL = 'universities'

def buildSchools():
    conn = MySQLdb.connect(
        host = DB_HOST, 
        user = DB_USER, 
        passwd = DB_PASSWD,
        db = DB_NAME,
        charset = 'utf8'
    )
    print "Creating table %s\n" % TBL_SCHOOL
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS %s" % (TBL_SCHOOL))
    cursor.execute("CREATE TABLE %s(`id` INT(11) NOT NULL AUTO_INCREMENT,`country_id` INT(5) NOT NULL,`name` VARCHAR(150) NOT NULL, PRIMARY KEY (`id`), KEY `country_id` (`country_id`)) ENGINE=MyISAM DEFAULT CHARSET=utf8" % (TBL_SCHOOL))
    
    print "Get countries from table %s\n\n" % TBL_COUNTRY
    cursor.execute("SELECT %s, %s FROM %s WHERE 1" % (COUNTRY_ID, COUNTRY_CODE, TBL_COUNTRY))
    countries = cursor.fetchall()
    
    print "Trying to build schools..\n"
    for country in countries:
        print "Populate schools in %s\n" % country[1]
        schools = getSchools(country[1].lower())
        for school in schools:
            if len(school.getAttribute('v')) > 0 and school.getAttribute('v') != '0':
                univ = school.childNodes[0].data.replace('"', '\\"')
                cursor.execute('''INSERT INTO %s VALUES(NULL, %d, "%s")''' % (TBL_SCHOOL, country[0], univ))
    print "\nEnd building schools."
    cursor.close()
    conn.close()

def getSchools(code):
    dom = minidom.parse(urllib.urlopen(WS_URL % code))
    return dom.getElementsByTagName('s')

if __name__ == "__main__":
    buildSchools()