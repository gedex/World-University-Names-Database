from sgmllib import SGMLParser
from urllib2 import urlopen, Request, BaseHandler
from httplib import BadStatusLine
import time
import MySQLdb

WS_URL = 'http://www.webometrics.info/university_by_country.asp?country=%s'
DB_HOST = 'localhost'
DB_NAME = 'bell'
DB_USER = 'root'
DB_PASSWD = '123456'
TBL_COUNTRY = 'countries'
COUNTRY_ID = 'id'
COUNTRY_CODE = 'iso2'
TBL_SCHOOL = 'webometric_universities'

class UnivParser(SGMLParser):
    domain = 'http://www.webometrics.info/'
    path = '/university_by_country.asp?country=%s'
    univ = []
    errorURL = []
    
    insideRowData = 0
    insideColOfUnivName = 0
    passedColOfUnivName = 0
    insideColOfUnivLink = 0
    insideUnivLink = 0
    currentUniv = {}
    
    sleepCount = 1;
    insideColNav = 0
    insideLinkOfNav = 0
    currentLinkOfNav = ''
    
    nextPage = ''
    pageNumber = 1
    endOfPage = 0
    
    def __init__(self):
        SGMLParser.__init__(self, verbose=0)
    
    def parse(self, url):
        self.univ = []
        self.pageNumber = 1 
        self.goToNextPage(url)
        return self.univ
        
    def goToNextPage(self, url):
        print "crawl page %d" % self.pageNumber
        
        self.insideRowData = 0
        self.insideColOfUnivName = 0
        self.passedColOfUnivName = 0
        self.insideColOfUnivLink = 0
        self.insideUnivLink = 0
        self.currentUniv = {}
        
        self.insideColNav = 0
        self.insideLinkOfNav = 0
        self.currentLinkOfNav = ''
        
        self.nextPage = ''
        self.endOfPage = 0
        
        try:
            req = urlopen(url)
        except IOError, e:
            print "Oops, we got HTTPError."
            reason = ""
            if hasattr(e, 'reason'):
                reason = 'Failed to reach a server. Reason: %d' % e.reason
            elif hasattr(e, 'code'):
                reason = 'The server couldn\'t fulfill the request. Error code: %d' % e.code
            print reason
            self.errorURL.append({'url': url, 'reason': reason})
            return
        except BadStatusLine, e:
            reason = ""
            if hasattr(e, 'reason'):
                reason = 'Failed to reach a server. Reason: %d' % e.reason
            elif hasattr(e, 'code'):
                reason = 'The server couldn\'t fulfill the request. Error code: %d' % e.code
            print reason
            self.errorURL.append({'url': url, 'reason': reason})
            return
        finally:
            self.sleepCount = 1
        
        self.feed(req.read())
        req.close()
        
        if len(self.nextPage):
            self.goToNextPage(self.domain + self.nextPage)
    
    def __sleep(self):
        time.sleep(0.5 * self.sleepCount)
    
    def getErrorURL(self):
        return self.errorURL
        
    def start_tr(self, attrs):
        if self.insideRowData == 0:
            for name, val in attrs:
                if name == 'class' and val == 'nav6a':
                    self.insideRowData = 1
    
    def start_td(self, attrs):
        if self.insideRowData:
            if self.passedColOfUnivName == 0: 
                self.insideColOfUnivName = 1
            else:
                self.insideColOfUnivLink = 1
        else: # inside col of nav
            for name, val in attrs:
                if name == 'class' and val == 'nav6a':
                    self.insideColNav = 1
    
    def start_a(self, attrs):
        if self.insideColOfUnivName:
            self.insideUnivLink = 1
        elif self.insideColNav:
            for name, val in attrs:
                if name == 'class' and val == 'nav6a':
                    self.insideLinkOfNav = 1
                if name == 'href':
                    self.currentLinkOfNav = val
    
    def end_tr(self):
        if self.insideRowData:
            self.insideRowData = 0
            self.passedColOfUnivName = 0
            self.univ.append( self.currentUniv )
            self.currentUniv = {}
            
    def end_td(self):
        if self.insideRowData:
            if self.insideColOfUnivName:
                self.insideColOfUnivName = 0
                self.passedColOfUnivName = 1
            else:
                self.insideColOfUnivLink = 0
        elif self.insideColNav:
            self.insideColNav = 0
            
    def end_a(self):
        if  self.insideUnivLink:
            self.insideUnivLink = 0
        elif self.insideLinkOfNav:
            self.insideLinkOfNav = 0
            self.currentLinkOfNav = ''
   
    def handle_data(self, data):
        if self.insideUnivLink:
            self.currentUniv['name'] = unicode( data.strip().replace('"', '\\"'), 'latin-1')
            print self.currentUniv['name']
        elif self.insideColOfUnivLink:
            self.currentUniv['link'] = u'%s' % data.strip()
        elif self.insideLinkOfNav and data.lower() == 'next' and self.nextPage == '':
            self.nextPage = self.currentLinkOfNav
            self.pageNumber += 1

def buildSchools():
    conn = MySQLdb.connect(
        host = DB_HOST, 
        user = DB_USER, 
        passwd = DB_PASSWD,
        db = DB_NAME,
        charset = 'latin1'
    )
    print "Creating table %s\n" % TBL_SCHOOL
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS %s" % (TBL_SCHOOL))
    cursor.execute("CREATE TABLE %s(`id` INT(11) NOT NULL AUTO_INCREMENT,`country_id` INT(5) NOT NULL,`name` VARCHAR(150) NOT NULL, `url` VARCHAR(150) NOT NULL, PRIMARY KEY (`id`), KEY `country_id` (`country_id`)) ENGINE=MyISAM DEFAULT CHARSET=utf8" % (TBL_SCHOOL))
    
    print "Get countries from table %s\n\n" % TBL_COUNTRY
    cursor.execute("SELECT %s, %s FROM %s WHERE 1" % (COUNTRY_ID, COUNTRY_CODE, TBL_COUNTRY))
    countries = cursor.fetchall()
    
    w = UnivParser()
    
    print "Trying to build schools from webometric..\n"
    for country in countries:
        print "\nPopulate schools in %s" % country[1]
        schools = w.parse(WS_URL % country[1].lower())
        for school in schools:
            cursor.execute('''INSERT INTO %s VALUES(NULL, %d, "%s", "%s")''' % (TBL_SCHOOL, country[0], school['name'], school['link']))
    print "\nEnd building schools."
    
    """ print error """
    for err in w.getErrorURL():
        print "%s\n" % err
    
    w.close()
    cursor.close()
    conn.close()

def insertSchools(url, cc):
    conn = MySQLdb.connect(
        host = DB_HOST, 
        user = DB_USER, 
        passwd = DB_PASSWD,
        db = DB_NAME,
        charset = 'latin1'
    )
    cursor = conn.cursor()
    cursor.execute("SELECT %s FROM %s WHERE iso2 = '%s'" % (COUNTRY_ID, TBL_COUNTRY, cc.upper()))
    country = cursor.fetchall()
    
    w = UnivParser()
    schools = w.parse(url)
    for school in schools:
        cursor.execute('''INSERT INTO %s VALUES(NULL, %d, "%s", "%s")''' % (TBL_SCHOOL, country[0][0], school['name'], school['link']))
    print "\nEnd building schools."
    
    """ print error """
    for err in w.getErrorURL():
        print "%s\n" % err
    
    w.close()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    buildSchools()