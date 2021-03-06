import os
import re
import sys
import traceback
from datetime import date,datetime
import cPickle as Pickle
import ConfigParser

class Group(object):

    NO_OWNER                =   "0"
    NEVER_ACTIVE            =   "Never"
    MISSING_CRAWL_DATE      =   "Never"

    INT_FIELDS              =   ["Id","Membership","Owner"]
    TEXT_FIELDS             =   ["Name"]
    DATE_FIELDS             =   ["LastActivity","CrawlDate"]
    LIST_ID_FIELDS          =   ["Mods","Members"]
 

    def __init__(self,gid):
        self.Id             =   int(gid)
        self.Name           =   None
        self.Membership     =   0
        self.Members        =   set()
        self.Owner          =   Group.NO_OWNER
        self.Mods           =   set()
        self.TooBig         =   False
        self.CrawlDate      =   Group.MISSING_CRAWL_DATE
        self.LastActivity   =   Group.NEVER_ACTIVE

    def getProfiles(self):
        return self.Members | self.Mods | set([self.Owner])

    def getLastActivity(self):
        if self.LastActivity == Group.NEVER_ACTIVE:
            return date(1970,1,1)
        else:
            splitList = re.split('/',self.LastActivity)
            return date(int(splitList[2]),int(splitList[1]),int(splitList[0]))

    def getCrawlDate(self):
        if self.CrawlDate == Group.MISSING_CRAWL_DATE:
            return date(1970,1,1)
        else:
            splitList = re.split('/',self.CrawlDate)
            return date(int(splitList[2]),int(splitList[1]),int(splitList[0]))

    def setCrawlDate(self,time_stamp=None):
        if time_stamp is None:
            now                 =   date.today()
        else:
            now = datetime.utcfromtimestamp(time_stamp)
        self.CrawlDate      =   "%s/%s/%s" % (now.day,now.month,now.year)

    def validate(self):
        for k,v in vars(self).iteritems():
            try:
                Pickle.dumps(v)
            except TypeError:
                return "No, Picle Failed : Field [%s] Value [%s]\n" % (k,v)
        return "Yes"

    def save(self):
        if not os.path.exists("Groups"):
            os.mkdir("Groups")
        fileName = os.path.join("Groups","%s.dat" % self.Id)
        with open(fileName,"wb") as fp:
            Pickle.dump(vars(self),fp)
            fp.flush()
            os.fsync(fp.fileno())

    def load(self):
        fileName = os.path.join("Groups","%s.dat" % self.Id)
        if os.path.exists(fileName):
            try:
                with open(fileName,"rb") as fp:
                    for k,v in Pickle.load(fp).iteritems():
                        setattr(self,k,v)
                return True
            except Exception:
                sys.stderr.write("Failed to load grouop [%s]\n" % self.Id)
                traceback.print_exc(sys.stderr)
                return False
        else:
            return False

    def __str__(self):
        rv = "\n"
        rv +=   "Profile\n"
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Name",self.Name)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Membership",self.Membership)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Profiles",len(self.Profiles))
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"TooBig",self.TooBig)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Last Activity",self.LastActivity)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Crawl Date",self.CrawlDate)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Owner",self.Owner)
        #-----------------------------------------------
        rv +=   "[%s][%16s]" % (self.Id,"Mods")
        if len(self.Mods) == 0:
            rv += "\n"
        else:
            first   =   True
            for k in self.Mods:
                if not first:
                    rv += "[%s] %16s " % (self.Id,"")
                else:
                    first = False
                rv += "[%32s]\n" % (k)
 
        return rv

class Profile(object):

    DEFAULT_ANSWER          =   "No Answer"
    NEVER_ACTIVE            =   "Never"
    MISSING_CRAWL_DATE      =   "Never"
    GRAPH_NOT_SET           =   "-1"
    GRAPH_ISOLATED          =   "0"
    #-----------------------------------------------
    GENDER_GROUP_MALE       =   ["M","FtM"]
    GENDER_GROUP_FEMALE     =   ["F","FEM","MtF"]
    GENDER_MALE_TITLE       =   "Male"
    GENDER_FEMALE_TITLE     =   "Female"
    GENDER_OTHER_TITLE      =   "Other"
    #-----------------------------------------------
    IDENTITY_GROUPS         =   {
                                    "Top":      ["Top","Sadist","Mistress","Master","Domme","Dom","Daddy"],
                                    "Bottom":   ["sub","slave","pet","brat","babygirl","Masochist"],
                                    "Swtich":   ["Switch","Sadomasochist"],
                                    "Age":      ["babygirl","Daddy"],
                                    "Theme":    ["pet","Vanilla","Primal","Kinkster","Hedonist"],
                                    "Ds":       ["sub","Domme","Dom","brat"],
                                    "Ms":       ["slave","Master","Mistress"],
                                    "SM":       ["Sadist","Masochist","Sadomasochist"] 
                                }

    INT_FIELDS              =   ["Id","Age","Graph"]
    TEXT_FIELDS             =   ["Name","Gender","Type","Orientation","Active"]
    DATE_FIELDS             =   ["LastActivity","CrawlDate"]
    LIST_ID_FIELDS          =   ["Friends"]
    LIST_FIELDS             =   ["Location","LookingFor"]
    FETISH_FIELDS           =   ["Into","Curious"]
    LIST_TUPLE_FIELDS       =   ["Relationships"]

    def __init__(self,pid):
        self.Id             =   int(pid)
        self.Name           =   None
        self.Age            =   None
        self.Gender         =   Profile.DEFAULT_ANSWER
        self.Type           =   Profile.DEFAULT_ANSWER
        self.Location       =   []
        self.Relationships  =   []
        self.LookingFor     =   []
        self.Orientation    =   None
        self.Active         =   Profile.DEFAULT_ANSWER
        self.LastActivity   =   Profile.NEVER_ACTIVE
        self.Friends        =   []
        self.CrawlDate      =   Profile.MISSING_CRAWL_DATE
        self.Degree         =   None
        self.Into           =   {}
        self.Curious        =   {}
        self.Groups         =   set()
        self.Graph          =   Profile.GRAPH_NOT_SET

    def getGroups(self):
        return self.Groups

    def getOtherProfiles(self):
        return set(self.Friends) | set([x for (x,_) in self.Relationships])

    def getLastActivity(self):
        if self.LastActivity == Profile.NEVER_ACTIVE:
            return date(1970,1,1)
        else:
            splitList = re.split('/',self.LastActivity)
            return date(int(splitList[2]),int(splitList[1]),int(splitList[0]))

    def getCrawlDate(self):
        if self.CrawlDate == Profile.MISSING_CRAWL_DATE:
            return date(1970,1,1)
        else:
            splitList = re.split('/',self.CrawlDate)
            return date(int(splitList[2]),int(splitList[1]),int(splitList[0]))

    def setCrawlDate(self,time_stamp=None):
        if time_stamp is None:
            now                 =   date.today()
        else:
            now = datetime.utcfromtimestamp(time_stamp)
        self.CrawlDate      =   "%s/%s/%s" % (now.day,now.month,now.year)

    def save(self):
        if hasattr(self,"_page"):
            delattr(self,"_page") 

        if hasattr(self,"_link"):
            delattr(self,"_link") 

        if not os.path.exists("Profiles"):
            os.mkdir("Profiles")
        fileName = os.path.join("Profiles","%s.dat" % self.Id)
        with open(fileName,"wb") as fp:
            Pickle.dump(vars(self),fp)
            fp.flush()
            os.fsync(fp.fileno())

    def load(self):
        fileName = os.path.join("Profiles","%s.dat" % self.Id)
        if os.path.exists(fileName):
            try:
                with open(fileName,"rb") as fp:
                    for k,v in Pickle.load(fp).iteritems():
                        setattr(self,k,v)
                return True
            except Exception:
                sys.stderr.write("Failed to load profile [%s]\n" % self.Id)
                traceback.print_exc(sys.stderr)
                return False
        else:
            return False

    def validate(self):
        for k,v in vars(self).iteritems():
            try:
                Pickle.dumps(v)
            except TypeError:
                return "No, Picle Failed : Field [%s] Value [%s]\n" % (k,v)

        for k,v in self.Relationships:
            if not isinstance(k,int):
                return "No,  Relationship k [%s]\n" % k
            if not isinstance(v,basestring):
                return "No,  Relationship v [%s]\n" % v
 
        for k in self.Friends:
             if not isinstance(k,int):
                return "No,  Friend k [%s]\n" % k
 
        return "Yes"

    def __str__(self):
        rv = "\n"
        rv +=   "Profile\n"
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Name",self.Name)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Age",self.Age)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Gender",self.Gender)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Type",self.Type)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Orientation",self.Orientation)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Active",self.Active)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Last Activity",self.LastActivity)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Crawl Date",self.CrawlDate)
        rv +=   "[%s][%16s][%32s]\n" % (self.Id,"Groups",len(self.Groups))
        #-----------------------------------------------
        rv +=   "[%s][%16s]" % (self.Id,"Location")
        if len(self.Location) == 0:
            rv += "\n"
        else:
            first   =   True
            for location in self.Location:
                if not first:
                    rv += "[%s] %16s " % (self.Id,"")
                else:
                    first = False
                rv += "[%32s]\n" % (location)
        #-----------------------------------------------
        rv +=   "[%s][%16s]" % (self.Id,"Relationships")
        if len(self.Relationships) == 0:
            rv += "\n"
        else:
            first   =   True
            for k,v in self.Relationships:
                if not first:
                    rv += "[%s] %16s " % (self.Id,"")
                else:
                    first = False
                rv += "[%32s][%16s]\n" % (v,k)
        #-----------------------------------------------
        rv +=   "[%s][%16s]" % (self.Id,"Looking For")
        if len(self.LookingFor) == 0:
            rv += "\n"
        else:
            first   =   True
            for k in self.LookingFor:
                if not first:
                    rv += "[%s] %16s " % (self.Id,"")
                else:
                    first = False
                rv += "[%32s]\n" % (k)
        #-----------------------------------------------
        rv +=   "[%s][%16s]" % (self.Id,"Friends")
        if len(self.Friends) == 0:
            rv += "\n"
        else:
            first   =   True
            for k in self.Friends:
                if not first:
                    rv += "[%s] %16s " % (self.Id,"")
                else:
                    first = False
                rv += "[%32s]\n" % (k)
        #-----------------------------------------------
        rv +=   "[%s][%16s]" % (self.Id,"Into")
        if len(self.Into) == 0:
            rv += "\n"
        else:
            first   =   True
            for k,v in self.Into.iteritems():
                if not first:
                    rv += "[%s] %16s " % (self.Id,"")
                else:
                    first = False
                rv += "[%32s]" % k
                firstTwo    =   True
                for vv in v:
                    if not firstTwo:
                        rv += "[%s] %16s  %32s " % (self.Id,"","")
                    else:
                        firstTwo = False
                    rv += "[%16s]\n" % (vv)
        #-----------------------------------------------
        rv +=   "[%s][%16s]" % (self.Id,"Curious")
        if len(self.Curious) == 0:
            rv += "\n"
        else:
            first   =   True
            for k,v in self.Curious.iteritems():
                if not first:
                    rv += "[%s] %16s " % (self.Id,"")
                else:
                    first = False
                rv += "[%32s]" % k
                firstTwo    =   True
                for vv in v:
                    if not firstTwo:
                        rv += "[%s] %16s  %32s " % (self.Id,"","")
                    else:
                        firstTwo = False
                    rv += "[%16s]\n" % (vv)
        #-----------------------------------------------
        rv += "\n"
        #-----------------------------------------------
        #rv +=   "[%s][%16s]" % (self.Id,"Curious About")
        #if len(self.Curious) == 0:
        #    rv += "\n"
        #else:
 
        return rv


