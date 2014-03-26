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
from datetime import date,timedelta
from lxml import html
from Profile import Profile

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

   def load(self,session):
        sys.stderr.write("Loading Profile [%s]\n" % self.Id)
        #profile =   Profile(profile_id)
        link    =   "https://fetlife.com/users/%s" % self.Id
        page    =   session.get(link)
        if page.url != link:
            sys.stderr.write("Bad Profile [%s]\n" % self.Id)
            #self.__progress.set("BadProfiles",profile.Id,None)
            return False

        tree    =   html.fromstring(page.text)
         
        self.Name    =   tree.xpath('//h2[@class="bottom"]/text()')[0].strip()
        rawPair         =   tree.xpath('//span[@class="small quiet"]/text()')[0].strip()
        splitList       =   re.split(" ",rawPair)
        if len(splitList) > 1:
            self.Type    =   splitList[1]
        self.Age         =   re.sub(r'[^0-9]','', splitList[0])
        if self.Age != splitList[0]:
            self.Gender      =   re.sub(r'[0-9 ]','', splitList[0])
        self.Location    =   tree.xpath('//div[@class="span-13 append-1"]/p/em/a/text()')
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
                        pid =   re.sub(r'[^0-9 ]','', url)

                        self.Relationships[pid]  =   rel
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

        #sys.stderr.write("Last Active [%s]\n" % lastActive)
        urls      =   tree.xpath('//div[@class="span-5 append-1 small"]/ul/li/a/@href')

        for url in urls:
            if "group" in url:
                group = re.sub(r'[^0-9 ]','',  url)
                self.Groups.append(group)

        #---------------------------------------------------
        # Now, friends
        #---------------------------------------------------


        pageNum =   1
        while True:
            page    =   session.get("https://fetlife.com/users/%s/friends?page=%d" % (self.Id,pageNum))
            tree    =   html.fromstring(page.text)
        
            urls =   tree.xpath('//div[@class="clearfix user_in_list"]/div/a/@href')
            for url in urls:
                friend =  re.sub(r'[^0-9 ]','', url)
                self.Friends.append(friend)

            next    =   tree.xpath('//a[@class="next_page"]')
            if len(next) == 1:
                pageNum += 1
            else:
                break

        #sys.stderr.write(str(profile))
        #profile.save()
        #del profile
        sys.stderr.write("Done Loading Profile [%s]\n" % self.Id)

        return True




class Crawler(object):

    def __init__(self,session):
        self.__session      =   session
        self.__progressFile =   os.path.join(session.getDataPath(),"progress.ini")

        self.__progress     =   ConfigParser.ConfigParser()
        self.__progress.optionxform=str

        if os.path.exists(self.__progressFile):        
            self.__progress.read(self.__progressFile)
        else:
            self.__progress.add_section("PendingGroups")
            self.__progress.add_section("PendingProfiles")
            self.__progress.add_section("BadProfiles")
            self.__progress.add_section("GroupsProfiles")
            self.__progress.add_section("CompletedGroups")
            self.__progress.add_section("CompletedProfiles")
            self.__saveProgress()

    def getSession(self):
        return self.__session

    def getCompletedProfiles(self):
        return self.__progress.options("CompletedProfiles")
        
    def __saveProgress(self):
        with open(self.__progressFile,'wb') as fp:
            self.__progress.write(fp)

    def examGroup(self,group_id):
        raise NotImplementedError

 
    def printProgress(self):
        sys.stderr.write("Progress: Profiles [%8d][%8d][%8d]  Groups [%8d][%8d][%8d]\n" %  (
            len(self.__progress.options("CompletedProfiles")),
            len(self.__progress.options("PendingProfiles")), 
            len(self.__progress.options("BadProfiles")), 
            len(self.__progress.options("CompletedGroups")), 
            len(self.__progress.options("PendingGroups")),
            len(self.__progress.options("BadGroups"))
            ))

    def doTick(self):
        try:
            #sys.stderr.write("doTick()\n")
            if len(self.__progress.options("PendingProfiles")) != 0:
                #sys.stderr.write("Pending Profiles [%s].\n" % (len(self.__progress.options("PendingProfiles") )))
                profileId   =   self.__progress.options("PendingProfiles")[0]
                profile     =   CrawlerProfile(profileId)
                if not profile.load(self.getSession()):
                    self.__progress.set("BadProfiles",profileId,None)
                else:
                    op  = profile.getOtherProfiles()
                    og  = profile.getOtherGroups()
                    #(op,og) = self.examProfile(profileId)
                    #sys.stderr.write("Other Profiles - %s\n" % op)
                    #sys.stderr.write("Other Groups   - %s\n" % og)
                    for pid in op:
                        if not self.__progress.has_option("PendingProfiles",pid) and  not self.__progress.has_option("CompletedProfiles",pid):
                            self.__progress.set("PendingProfiles",pid,None)

                    for gid in og:
                        if not self.__progress.has_option("PendingGroups",gid) and  not self.__progress.has_option("CompletedGroups",gid):
                            self.__progress.set("PendingGroups",gid,None)
                    profile.save()
                    del profile

                self.__progress.remove_option("PendingProfiles",profileId)
                self.__progress.set("CompletedProfiles",profileId,None)
                """
            elif len(self.__progress.options("PendingGroups")) != 0:
                pass
                #sys.stderr.write("Pending Groups [%s].\n" % (len(self.__progress.options("PendingGroups") )))
                """
            else:
                sys.stderr.write("No pending work.\n")
                return False

            self.__saveProgress()
            return True
        except KeyboardInterrupt:
            self.__saveProgress()
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
        #sys.stderr.write("Token [%s]\n" % token)


        payload =   {
                        "authenticity_token":   token,
                        "nickname_or_email" :   userName,
                        "password"          :   passWord,
                        "remember_me"       :   "1",
                        "commit"            :   "Login to FetLife"
                    }
        payloadStr  =   urllib.urlencode(payload)
        payloadStr  +=  "&utf8=%E2%9C%93"
        #sys.stderr.write("%s\n" % payloadStr)

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
    parser.add_option('-g', '--group', help="group number to test scan", type='int', default=None)
    parser.add_option('-p', '--profile', help="profile number to test scan", type='int', default=None)
    options, args = parser.parse_args()

    if options.profile is not None and options.group is not None:
        sys.stderr.write("Please select profile or group, but not both\n")
        sys.exit()

    session =   Session()
    if not session.doLogin():
        sys.stderr.write("Something went wrong, still not connected\n")

    crawler =   Crawler(session)
    crawler.printProgress()
    if options.profile is not None:
        profile = CrawlerProfile(options.profile)
        if profile.load(crawler.getSession()):
            sys.stderr.write("Loaded.  Profiles [%d] Groups [%d]\n" % (len(profile.getOtherProfiles()),len(profile.getOtherGroups())))
            sys.stderr.write(str(profile))
        else:
            sys.stderr.write("Failed\n")
    elif options.group is not None:
        crawler.examGroup("%s" % options.group)
    else:
        while crawler.doTick():
            try:
                crawler.printProgress()
                gc.collect()
                #time.sleep(1)
            except KeyboardInterrupt:
                break
