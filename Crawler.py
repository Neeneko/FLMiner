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
from Profile import Profile,Group
from multiprocessing import Process, Lock
from threading import Thread
from Progress import Progress,StringMap

class CrawlerGroup(Group):

    prefix = "groups"

    def __init__(self,gid):
        super(CrawlerGroup,self).__init__(gid)
        self._page  =   None
        self._link  =   None

    def setLastActive(self,text):
        year,month,day       =   re.split("/",text[0:text.find(" ")])
        self.LastActivity = "%s/%s/%s" % (day,month,year)

    def fill(self,session):
        sys.stderr.write("Loading Group [%s]\n" % self.Id)
        assert isinstance(self.Id,int)
        self._link  =   "https://fetlife.com/groups/%s" % self.Id
        self._page  =   session.get(self._link)
        if self._page.url != self._link:
            sys.stderr.write("Missing Profile [%s]\n" % self.Id)
            return False

        tree            =   html.fromstring(self._page.text)
        self.Name       =   str(tree.xpath('//h2[@class="group_name bottom"]/a/text()')[0])
        text            =   tree.xpath('//h2[@class="group_name bottom"]/following-sibling::p/text()')[0]
        self.Membership =   int(re.sub(r'[^0-9]','', text))
        text            =    tree.xpath('//span[@class="last_comment"]/span/text()')[0]
        self.setLastActive(text)

        for text in tree.xpath('//ul[@class="group_mods"]/li/a[@class="small"]/@href'):
            self.Mods.add(int(re.sub(r'[^0-9]','', text)))

        self.Owner      =   int(re.sub(r'[^0-9]','',tree.xpath('//div[contains(text(),"(owner)")]/preceding::a/@href')[-1]))
        #-----------------------------------------------------------------------------------------------
        self._link      =   "https://fetlife.com/groups/%s/group_memberships" % self.Id
        self._page      =   session.get(self._link)
        tree            =   html.fromstring(self._page.text)
        tell            =   tree.xpath('//div[contains(@class,"group_members")]/div[@class="clearfix"]')
        if len(tell)    ==  0:
            self.TooBig =   True
        else:
            self.TooBig =   False
            pageNum =   1
            while True:
                self._link  =   "https://fetlife.com/groups/%s/group_memberships?page=%d" % (self.Id,pageNum)
                self._page  =   session.get(self._link)
                tree    =   html.fromstring(self._page.text)
        
                urls =   tree.xpath('//div[contains(@class,"user_in_list")]/div/a/@href')
                for url in urls:
                    pid =  int(re.sub(r'[^0-9 ]','', url))
                    self.Profiles.add(pid)

                next    =   tree.xpath('//a[@class="next_page"]')
                if len(next) == 1:
                    pageNum += 1
                else:
                    break


        self.setCrawlDate()
        sys.stderr.write("Done Loading Group [%s]\n" % self.Id)

        return True

class CrawlerProfile(Profile):

    prefix  =   "users"

    def __init__(self,pid):
        super(CrawlerProfile,self).__init__(pid)
        self._page  =   None
        self._link  =   None

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
        self._link    =   "https://fetlife.com/users/%s" % self.Id
        self._page    =   session.get(self._link)
        if self._page.url != self._link:
            sys.stderr.write("Missing Profile [%s]\n" % self.Id)
            return False

        tree    =   html.fromstring(self._page.text)
         
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

        for item in table:
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

        for groupURL in tree.xpath('//li/a[contains(@href,"/groups/")]/@href'):
            try:
                self.Groups.add(int(re.sub(r'[^0-9]','', groupURL)))
            except ValueError:
                pass
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
            self._link  =   "https://fetlife.com/users/%s/friends?page=%d" % (self.Id,pageNum)
            self._page  =   session.get(self._link)
            tree    =   html.fromstring(self._page.text)
        
            urls =   tree.xpath('//div[@class="clearfix user_in_list"]/div/a/@href')
            for url in urls:
                friend =  int(re.sub(r'[^0-9 ]','', url))
                self.Friends.append(friend)

            next    =   tree.xpath('//a[@class="next_page"]')
            if len(next) == 1:
                pageNum += 1
            else:
                break

        self.setCrawlDate()
        sys.stderr.write("Done Loading Profile [%s]\n" % self.Id)

        return True

class Crawler(object):

    def __init__(self,session,progress,raise_on_failure=False):
        self.__session          =   session
        self.__progress         =   progress
        self.__raiseOnFailure   =   raise_on_failure
        self.__errorPath        =   "Error"
        if not os.path.exists(self.__errorPath):
            os.mkdir(self.__errorPath)

    def getSession(self):
        return self.__session
 
    def saveError(self,eid,entity):
        errorFile   =   os.path.join(self.__errorPath,"%s_%s.txt" % (entity.prefix,entity.Id))
        with open(errorFile,'w') as fp:
            fp.write("=========================\n")
            traceback.print_exc(file=fp)
            fp.write("=========================\n")

            if entity is None:
                fp.write("No Entity\n")
            else:
                fp.write("Requ URL    - %s\n" % entity._link)
                if entity._page is None:
                    fp.write("No Page\n")
                else:
                    fp.write("Resp URL    - %s\n" % entity._page.url)
                    fp.write("Status Code - %s\n" % entity._page.status_code) 
                    fp.write("Reason      - %s\n" % entity._page.reason) 
                    htmlFile   =   os.path.join(self.__errorPath,"%s_%s.html" % (entity.prefix,entity.Id))
                    with open(htmlFile,'w') as hfp:
                        hfp.write(entity._page.text.encode('utf8'))
            fp.write("=========================\n")

    def doTick(self):
        try:
            nextId  =   self.__progress.nextProfile()
            if nextId is not None:
                entity     =   CrawlerProfile(nextId)
                if entity.fill(self.getSession()):
                    pids  = entity.getOtherProfiles()
                    gids  = entity.getGroups()
                    entity.save()
                    self.__progress.completeProfile(nextId,pids,gids)
                else:
                    self.__progress.missingProfile(nextId)
                del entity
                return True            
        except KeyboardInterrupt:
            sys.stderr.write("Interrupting work.\n")
            self.__progress.setExit()
            return False
        except Exception,e:
            sys.stderr.write("Failed to load profile [%s].\n" % (nextId))
            self.saveError(nextId,entity)
            #traceback.print_exc(sys.stderr)
            self.__progress.errorProfile(nextId)
            if self.__raiseOnFailure:
                self.__progress.setExit()
                raise
            return True

        try:
            nextId  =   self.__progress.nextGroup()
            if nextId is not None:
                entity     =   CrawlerGroup(nextId)
                if entity.fill(self.getSession()):
                    pids  = entity.getProfiles()
                    entity.save()
                    self.__progress.completeGroup(nextId,pids)
                else:
                    self.__progress.missingGroup(nextId)
                del entity
                return True
        except KeyboardInterrupt:
            sys.stderr.write("Interrupting work.\n")
            self.__progress.setExit()
            return False
        except Exception,e:
            sys.stderr.write("Failed to load group [%s].\n" % (nextId))
            traceback.print_exc(sys.stderr)
            self.saveError(nextId,entity)
            self.__progress.errorGroup(nextId)
            if self.__raiseOnFailure:
                self.__progress.setExit()
                raise
            return True

        sys.stderr.write("No pending work.\n")
        self.__progress.setExit()
        return False



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
    parser.add_option('-g', '--group', help="group number to test scan", type='int', default=None)
    parser.add_option('-t', '--threads', help="number of threads to spawn", type='int', default=1)
    parser.add_option('-r', '--rebuild', help="rebuild the progress file",action="store_true",default=False)
    parser.add_option('-i', '--init', help="init the progress file",action="store_true",default=False)
    parser.add_option('-e', '--error', help="re-examine the error pages",action="store_true",default=False)

    options, args = parser.parse_args()

    if options.init:
        sys.stderr.write("Init Progress File\n")
        if options.profile == None and options.group == None:
            sys.stderr.write("\tMust select profile or group to init from\n")
            sys.exit(0)
        progress = Progress()
        progress.initPending(options.profile,options.group)
        progress.printProgress()
        sys.exit(0)
    elif options.rebuild:
        sys.stderr.write("Rebuilding Progress File\n")
        Progress(True)
        progress.printProgress()
        sys.exit(0)

    #everything after this requires a session
    session =   Session()
    if not session.doLogin():
        sys.stderr.write("Something went wrong, still not connected\n")

    if options.profile is not None:
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
    elif options.group is not None:
        group = CrawlerGroup(options.group)
        if group.fill(session):
            sys.stderr.write("Loaded.  Profiles [%d]\n" % (len(group.getProfiles())))
            sys.stderr.write(unicode(group))
            sys.stderr.write("Valid : [%s]\n" % group.validate())
            group.save()
            group.load()
            sys.stderr.write("Valid : [%s]\n" % group.validate())
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
                crawler.doTick()
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
