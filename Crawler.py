import os
import re
import sys
import gc
import ConfigParser
import optparse
import requests
import urllib
import pickle
import getpass
import time
import glob
import cPickle as pickle
import traceback
from datetime import date,timedelta
from lxml import html
from Profile import Profile
from multiprocessing import Process, Lock
from threading import Thread

class StringMap(object):

    __instance  =   None
    class   __impl:

        def __init__(self):
            self.__mutex        =   Lock()
            with self.__mutex:
                self.__dataPath     =   "Data"
                self.__stringsFile  =   os.path.join(self.__dataPath,"strings.dat")
                self.__stringsTemp  =   os.path.join(self.__dataPath,"strings.tmp")

                if os.path.exists(self.__stringsFile):       
                    self.__sections =   pickle.load( open( self.__stringsFile, "rb" ) )
                else:
                    self.__sections =   {}

        def __save(self):
            pickle.dump( self.__sections, open(self.__stringsTemp, "wb" ) )
            os.rename(self.__stringsTemp,self.__stringsFile)

        def save(self):
            with self.__mutex:
                self.__save()

        def addString(self,section,key,value):
            with self.__mutex:
                if section not in self.__sections:
                    self.__sections[section]    =   {}
                self.__sections[section][key]  =   value

        def hasString(self,section,key):
            with self.__mutex:
                if section in self.__sections and key in self.__sections[section]:
                    return True
                else:
                    return False


    def __init__(self):
           if StringMap.__instance is None:
                StringMap.__instance = StringMap.__impl()

    def __getattr__(self, attr):
        return getattr(self.__instance, attr)

    def __setattr__(self, attr, value):
        return setattr(self.__instance, attr, value)

class FastParser(ConfigParser.ConfigParser):

    def options(self, section):
        """Return a list of option names for the given section name."""
        return self._sections[section].keys()

    def len(self,section):
        return len(self._sections[section])

    def write(self, fp):
        """Write an .ini-format representation of the configuration state."""
        if self._defaults:
            fp.write("[%s]\n" % DEFAULTSECT)
            for (key, value) in self._defaults.items():
                fp.write("%s = %s\n" % (key, str(value).replace('\n', '\n\t')))
            fp.write("\n")
        for section in self._sections:
            fp.write("[%s]\n" % section)
            for (key, value) in self._sections[section].items():
                if key == "__name__":
                    continue
                """
                if (value is not None) or (self._optcre == self.OPTCRE):
                    #print "[%s][%s]" % (key,value)
                    key = " = ".join((key, str(value).replace('\n', '\n\t')))
                """
                fp.write("%s\n" % (key))
            fp.write("\n")


class CrawlerProfile(Profile):

   def setLastActive(self,text):
        now     =   date.today()
        try:
            number  =   int(re.sub(r'[^0-9]','', text))
        except ValueError:
            self.LastActivity = Profile.NEVER_ACTIVE
            return

        if "hour" in text or "minute" in text:
            then    =   now
        elif "day" in text:
            then    =   now - timedelta(number)
        elif "month" in text:
            day         =   1
            month       =   now.month
            year        =   now.year

            for i in range(number):
                month -= 1
                if month == 0:
                    year -= 1
                    month = 12
            then = date(year,month,day)
        elif "year" in text:
            day         =   1
            month       =   1
            year        =   now.year - number
            then = date(year,month,day)
        else:
            raise RuntimeError,"We can not process [%s] yet" % text


        self.LastActivity = "%s/%s/%s" % (then.day,then.month,then.year)

   def fill(self,session):
        sys.stderr.write("Loading Profile [%s]\n" % self.Id)
        assert isinstance(self.Id,int)
        #profile =   Profile(profile_id)
        link    =   "https://fetlife.com/users/%s" % self.Id
        page    =   session.get(link)
        if page.url != link:
            sys.stderr.write("Missing Profile [%s]\n" % self.Id)
            return False

        tree    =   html.fromstring(page.text)
         
        self.Name    =   tree.xpath('//h2[@class="bottom"]/text()')[0].strip()
        rawPair         =   tree.xpath('//span[@class="small quiet"]/text()')[0].strip()
        splitList       =   re.split(" ",rawPair)
        if len(splitList) > 1:
            self.Type    =   splitList[1]
        self.Age         =   int(re.sub(r'[^0-9]','', splitList[0]))
        if self.Age != splitList[0]:
            self.Gender     =   re.sub(r'[0-9 ]','', splitList[0])
        Location            =   tree.xpath('//div[@class="span-13 append-1"]/p/em/a/text()')
        self.Location       =   [unicode(x) for x in Location]
        table               =   tree.xpath('//div[@class="span-13 append-1"]/table/tr')

        #sys.stderr.write("Table\n")
        for item in table:
            #sys.stderr.write("\t[%s]\n" % item)
            children = [x for x in item]
            header  =   children[0]
            if header.text == "relationship status:" or header.text == "D/s relationship status:":
                assert len(children[1:]) == 1
                td = children[1]
                assert len(td.getchildren()) == 1
                ul = td.getchildren()[0]
                for li in ul:
                    if len(li.getchildren()) == 1:
                        a =  li.getchildren()[0]
                        url =   a.get("href")
                        rel =   li.text.strip()
                        pid =   int(re.sub(r'[^0-9 ]','', url))

                        self.Relationships.append(tuple([pid,rel]))
            elif header.text == "orientation:":
                assert len(children[1:]) == 1
                td = children[1]
                self.Orientation =   td.text
            elif header.text == "active:":
                assert len(children[1:]) == 1
                td = children[1]
                self.Active =   td.text
            elif header.text == "is looking for:":
                assert len(children[1:]) == 1
                td = children[1]
                for text in td.itertext():
                    self.LookingFor.append(text)
            else:
                raise RuntimeError,"Unknown table [%s]" % header.text

        lastActive  =   tree.xpath('//ul[@id="mini_feed"]/li/span[@class="quiet small"]/text()')
        if len(lastActive) != 0:
            self.setLastActive(lastActive[0])
        #---------------------------------------------------
        # Fetishes
        #---------------------------------------------------
        stringMap   =   StringMap()
        stuff       =   tree.xpath('//em[text()="Into:"]/ancestor::p')
        if len(stuff) != 0:
            #sys.stderr.write("Into [%s]\n" % stuff)
            intoList    =   []
            for item in stuff[0]:
                if item.text is None:
                    continue
                #sys.stderr.write("\t[%s][%s]\n" % (item,item.text))
                if "href" in item.keys():
                    fetishName          =   item.text
                    fetishId            =   int(re.sub(r'[^0-9 ]','', item.get("href")))
                    intoList.append( [fetishId,None] )
                    if not stringMap.hasString("Fetish",fetishId):
                        stringMap.addString("Fetish",fetishId,fetishName)
                elif len(intoList) > 0:
                    intoList[-1][1]  =   item.text[1:-1]

            #sys.stderr.write("\n%s\n" % intoList)
            for (k,v) in intoList:
                if v not in self.Into:
                    self.Into[v] =   set()
                self.Into[v].add(k)

        stuff      =   tree.xpath('//em[text()="Curious about:"]/ancestor::p')
        if len(stuff) != 0:
        #sys.stderr.write("Curious About [%s]\n" % stuff)
            curiousList =   []
            for item in stuff[0]:
                if item.text is None:
                    continue
                #sys.stderr.write("\t[%s][%s] - [%s]\n" % (item,item.text,item.keys()))
                if "href" in item.keys():
                    fetishName          =   item.text
                    fetishId            =   int(re.sub(r'[^0-9 ]','', item.get("href")))
                    curiousList.append( [fetishId,None] )
                    if not stringMap.hasString("Fetish",fetishId):
                        stringMap.addString("Fetish",fetishId,fetishName)
                elif len(curiousList) > 0:
                    curiousList[-1][1]  =   item.text[1:-1]

            #sys.stderr.write("\n%s\n" % curiousList)
            for (k,v) in curiousList:
                if v not in self.Curious:
                    self.Curious[v] =   set()
                self.Curious[v].add(k)

        #---------------------------------------------------
        # Now, friends
        #---------------------------------------------------
        pageNum =   1
        while True:
            page    =   session.get("https://fetlife.com/users/%s/friends?page=%d" % (self.Id,pageNum))
            tree    =   html.fromstring(page.text)
        
            urls =   tree.xpath('//div[@class="clearfix user_in_list"]/div/a/@href')
            for url in urls:
                friend =  int(re.sub(r'[^0-9 ]','', url))
                self.Friends.append(friend)

            next    =   tree.xpath('//a[@class="next_page"]')
            if len(next) == 1:
                pageNum += 1
            else:
                break



        #sys.stderr.write(str(profile))
        #profile.save()
        #del profile
        self.setCrawlDate()
        sys.stderr.write("Done Loading Profile [%s]\n" % self.Id)

        return True
#TODO - make sure uids and gids are int, not string!
class FauxParser(object):

    def __init__(self):
        self.__sections =   {}

    def clear(self):
        for v in self.__sections.values():
            v.clear()

    def add_section(self,section):
        self.__sections[section]    =   set()

    def set(self,section,key):
        self.__sections[section].add(key)

    def remove_option(self,section,key):
        #sys.stderr.write("removing [%s] from [%s]\n" % (key,self.__sections[section]))
        self.__sections[section].remove(key)

    def has_option(self,section,key):
        return key in self.__sections[section]

    def options(self,section):
        return self.__sections[section]

    def len(self,section):
        return len(self.__sections[section])

    def pop(self,section):
        return self.__sections[section].pop()

    def sections(self):
        return self.__sections.keys()

    def set_section(self,section,values):
        self.__sections[section] = set(values)

class Progress(object):

    def __init__(self,rebuild=False,raw_data=None):
        self.__mutex        =   Lock()
        with self.__mutex:            
            self.__dataPath     =   "Data"
            self.__progressFile =   os.path.join(self.__dataPath,"progress.dat")
            self.__progressTemp =   os.path.join(self.__dataPath,"progress.tmp")
            if raw_data is not None:
                self.__progress = raw_data
            elif os.path.exists(self.__progressFile) and not rebuild:       
                self.__progress = pickle.load( open( self.__progressFile, "rb" ) )
                sys.stderr.write("Moving [%s] Active back to Pending\n" % self.__progress.len("ActiveProfiles"))
                while self.__progress.len("ActiveProfiles") != 0:
                    oldId = self.__progress.pop("ActiveProfiles")
                    self.__progress.set("PendingProfiles",oldId)
            else:
                self.__progress =   FauxParser()
                self.__progress.add_section("PendingProfiles")
                self.__progress.add_section("ErrorProfiles")
                self.__progress.add_section("MissingProfiles")
                self.__progress.add_section("CompletedProfiles")
                self.__progress.add_section("ActiveProfiles")
                self.__saveProgress()

            self.__exit     =   False
            self.__count    =   0
            self.__rate     =   0
            self.__nextTime = time.time()+60

        if rebuild:
            self.__rebuild()
            self.printProgress()
            self.__saveProgress()

    def getRawData(self):
        return self.__progress

    def resetErrorProfiles(self):
        while self.__progress.len("ErrorProfiles") != 0:
            oldId = self.__progress.pop("ErrorProfiles")
            self.__progress.set("PendingProfiles",oldId)

    def validate(self):
        for sectionName in self.__progress.sections():
            for value in self.__progress.options(sectionName):
                if not isinstance(value,int):
                    return False
        return True

    def fix(self):
        for sectionName in self.__progress.sections():
            self.__progress.set_section(sectionName,[ int(x) for x in self.__progress.options(sectionName)])
    def initPending(self,pid):
        self.__progress.clear()
        self.__progress.set("PendingProfiles",pid)
        self.__saveProgress()
    """
    def __rebuild(self):
        sys.stderr.write("Starting Rebuild\n")
        self.__progress.clear()
        fileNames = glob.glob(os.path.join("Profiles","*.ini"))
        if len(fileNames) == 0:
            sys.stderr.write("No Data, defaulting.\n")
            self.__progress.set("PendingProfiles",1)
        else:
            for idx in range(len(fileNames)):
                fileName    =   fileNames[idx]
                pid         =   re.sub(r'[^0-9]','', fileName)
                self.__progress.set("CompletedProfiles",pid)

            for idx in range(len(fileNames)):
                fileName    =   fileNames[idx]
                pid         =   re.sub(r'[^0-9]','', fileName)
                config      =   ConfigParser.ConfigParser()
                config.optionxform=str
                config.read(fileName)
                for k in config.options("Friends") + config.options("Relationships"):
                    if not self.__progress.has_option("CompletedProfiles",k) \
                        and not self.__progress.has_option("PendingProfiles",k):
                        self.__progress.set("PendingProfiles",k)
                if idx%256 == 0:
                    sys.stderr.write("[%s]\n" % idx)

        sys.stderr.write("Done Rebuild\n")
        """
    def setExit(self):
        sys.stderr.write("Shutting Down\n")
        self.__exit =   True

    def getExit(self):
        return self.__exit

    def nextProfile(self):
        with self.__mutex:
            if self.__progress.len("PendingProfiles") == 0:
                return None
            self.__count += 1
            rv = self.__progress.pop("PendingProfiles")
            self.__progress.set("ActiveProfiles",rv)
            return rv 

    def errorProfile(self,pid):
        with self.__mutex:
            if pid in self.__progress.options("ActiveProfiles"):
                self.__progress.remove_option("ActiveProfiles",pid)
            self.__progress.set("ErrorProfiles",pid)
            #self.__saveProgress()

    def missingProfile(self,pid):
        with self.__mutex:
            if pid in self.__progress.options("ActiveProfiles"):
                self.__progress.remove_option("ActiveProfiles",pid)
            self.__progress.set("MissingProfiles",pid)
 

    def completeProfile(self,pid,op):
        sys.stderr.write("Completing [%s]\n" % pid)
        with self.__mutex:
            for opid in op:
                if not self.__progress.has_option("PendingProfiles",opid)            \
                    and not self.__progress.has_option("CompletedProfiles",opid)     \
                    and not self.__progress.has_option("ErrorProfiles",opid)         \
                    and not self.__progress.has_option("MissingProfiles",opid)       \
                    and not self.__progress.has_option("ActiveProfiles",opid):
                    self.__progress.set("PendingProfiles",opid)

            if self.__progress.has_option("ActiveProfiles",pid):
                self.__progress.remove_option("ActiveProfiles",pid)
            else:
                raise RuntimeError,"pid [%s] not in ActiveProfiles\n" % pid
            self.__progress.set("CompletedProfiles",pid)
            #self.__saveProgress()

    def getCompletedProfiles(self):
        with self.__mutex:
            return self.__progress.len("CompletedProfiles")
        
    def __saveProgress(self):
        pickle.dump( self.__progress, open(self.__progressTemp, "wb" ) )
        os.rename(self.__progressTemp,self.__progressFile)

    def saveProgress(self):
        with self.__mutex:
            self.__saveProgress()

    def printProgress(self):
        with self.__mutex:
            #if self.__nextTime < time.time():
            #self.__rate     =   self.__count
            #self.__nextTime = time.time()+60

            sys.stderr.write("Progress: Rate [%d] Active [%d] Completed [%8d] Pending [%8d] Error [%8d] Missing [%8d]\n" %  (        self.__count,
                self.__progress.len("ActiveProfiles"),
                self.__progress.len("CompletedProfiles"),
                self.__progress.len("PendingProfiles"), 
                self.__progress.len("ErrorProfiles"),
                self.__progress.len("MissingProfiles")
                ))
            self.__count    =   0

    def getIds(self,section):
        with self.__mutex:
            return self.__progress.options(section)

class Crawler(object):

    def __init__(self,session,progress,raise_on_failure=False):
        self.__session          =   session
        self.__progress         =   progress
        self.__raiseOnFailure   =   raise_on_failure

    def getSession(self):
        return self.__session
 
    def doTick(self):
        try:
            #sys.stderr.write("doTick()\n")
            nextId  =   self.__progress.nextProfile()
            if nextId is not None:
                #sys.stderr.write("Pending Profiles [%s].\n" % (len(self.__progress.options("PendingProfiles") )))
                profile     =   CrawlerProfile(nextId)
                if profile.fill(self.getSession()):
                    op  = profile.getOtherProfiles()
                    profile.save()
                    del profile
                    self.__progress.completeProfile(nextId,op)
                else:
                    self.__progress.missingProfile(nextId)
            else:
                sys.stderr.write("No pending work.\n")
                self.__progress.setExit()
                return False

            return True
        except KeyboardInterrupt:
            sys.stderr.write("Interrupting work.\n")
            self.__progress.setExit()
            return False
        except Exception,e:
            sys.stderr.write("Failed to load profile [%s].\n" % (nextId))
            traceback.print_exc(sys.stderr)
            self.__progress.errorProfile(nextId)
            if self.__raiseOnFailure:
                self.__progress.setExit()
                raise
            return True


class Session(object):

    def __init__(self):
        self.__dataPath     =   "Data"
        self.__sessionFile  =   os.path.join(self.__dataPath,"session.dat")

        if not os.path.exists(self.__dataPath):
            os.mkdir(self.__dataPath)

        self.__session     =   requests.Session()
        if os.path.exists(self.__sessionFile):
            self.__session.cookies  =    self.__loadCookies()

    def get(self,*args,**kwargs):
        return self.__session.get(*args,**kwargs)


    def getDataPath(self):
        return self.__dataPath

    def isLoggedIn(self):
        page    =   self.__session.get("http://fetlife.com")
        tree    =   html.fromstring(page.text)
        login   =   tree.xpath('//a[@href="/login"]')
        #sys.stderr.write("[%s]\n" % login)
        if len(login) == 0:
            return True
        else:
            return False


    def __saveCookies(self,cookies):
        with open(self.__sessionFile,'w') as fp:
            pickle.dump(cookies,fp)

    def __loadCookies(self):
        with open(self.__sessionFile,'rb') as fp:
            return pickle.load(fp)

    def doLogin(self):
        if self.isLoggedIn():
            sys.stderr.write("Previous login still valid\n")
            return True

        sys.stderr.write("Not logged in yet\n")

        userName = raw_input("Enter username: ")
        passWord = getpass.getpass("Enter password for [%s]:" % userName)


        page    =   self.__session.get("http://fetlife.com/login")
        tree    =   html.fromstring(page.text)
        token   =   tree.xpath('//div[@id="authenticity_token"]/text()')[0]

        payload =   {
                        "authenticity_token":   token,
                        "nickname_or_email" :   userName,
                        "password"          :   passWord,
                        "remember_me"       :   "1",
                        "commit"            :   "Login to FetLife"
                    }
        payloadStr  =   urllib.urlencode(payload)
        payloadStr  +=  "&utf8=%E2%9C%93"
        sys.stderr.write("%s\n" % payloadStr)


        page    =   self.__session.post("https://fetlife.com/session",data=payloadStr)
        if self.isLoggedIn():
            self.__saveCookies(self.__session.cookies)
            sys.stderr.write("Now logged in.\n")
            return True
        else:
            sys.stderr.write("Still not logged in.\n")

if __name__ == "__main__":
    usage       =   "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-p', '--profile', help="profile number to test scan", type='int', default=None)
    parser.add_option('-t', '--threads', help="number of threads to spawn", type='int', default=1)
    parser.add_option('-r', '--rebuild', help="rebuild the progress file",action="store_true",default=False)
    parser.add_option('-e', '--error', help="re-examine the error pages",action="store_true",default=False)

    options, args = parser.parse_args()

    session =   Session()
    if not session.doLogin():
        sys.stderr.write("Something went wrong, still not connected\n")

    if options.rebuild:
        sys.stderr.write("Rebuilding Progress File\n")
        if options.profile is not None:
            progress = Progress()
            progress.initPending(options.profile)
            progress.printProgress()
        else:
            Progress(True)
    elif options.profile is not None:
        profile = CrawlerProfile(options.profile)
        if profile.fill(session):
            sys.stderr.write("Loaded.  Other Profiles [%d]\n" % (len(profile.getOtherProfiles())))
            sys.stderr.write(unicode(profile))
            sys.stderr.write("Valid : [%s]\n" % profile.validate())
            profile.save()
            profile.load()
            sys.stderr.write("Valid : [%s]\n" % profile.validate())
        else:
            sys.stderr.write("Failed\n")
    elif options.error is True:
        sys.stderr.write("Examining Error List\n")
        progress    =   Progress()
        progress.printProgress()
        progress.resetErrorProfiles()
        progress.saveProgress()
        progress.printProgress()
        crawler     =   Crawler(session,progress,True)
        sys.stderr.write("Starting Crawler\n")
        while not progress.getExit():
            crawler.doTick()
        sys.stderr.write("Ending Crawler\n")
        progress.saveProgress()
        progress.setExit()



    else:
        def RunCrawler(num):
            crawler     =   Crawler(session,progress)
            sys.stderr.write("Starting Crawler [%d]\n" % num)
            while not progress.getExit():
                sys.stderr.write("Tick\n")
                crawler.doTick()
                sys.stderr.write("/Tick\n")
            sys.stderr.write("Ending Crawler [%d]\n" % num)
            progress.setExit()

        progress    =   Progress()
        stringMap   =   StringMap()
        progress.printProgress()
        threads     =   []
        for i in range(options.threads):
            threads.append(Thread(None,target=RunCrawler,args=(i,)))
            threads[-1].start()
        try:
            while not progress.getExit():
                time.sleep(60)
                progress.printProgress()
                progress.saveProgress()
                stringMap.save()
        except:
            sys.stderr.write("Shutting down from main thread\n")
            progress.setExit()
            
            for thread in threads:
                thread.join()
