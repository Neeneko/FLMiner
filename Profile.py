import os
import re
from datetime import date
import ConfigParser


class Profile(object):

    DEFAULT_ANSWER          =   "No Answer"
    NEVER_ACTIVE            =   "Never"
    GENDER_GROUP_MALE       =   ["M","FtM"]
    GENDER_GROUP_FEMALE     =   ["F","FEM","MtF"]

    def __init__(self,pid):
        self.Id             =   pid
        self.Name           =   None
        self.Age            =   None
        self.Gender         =   Profile.DEFAULT_ANSWER
        self.Type           =   Profile.DEFAULT_ANSWER
        self.Location       =   []
        self.Relationships  =   {}
        self.LookingFor     =   []
        self.Orientation    =   None
        self.Active         =   Profile.DEFAULT_ANSWER
        self.LastActivity   =   Profile.NEVER_ACTIVE
        self.Groups         =   []
        self.Friends        =   []
 
    def getOtherProfiles(self):
        return set(self.Friends) | set(self.Relationships.keys())

    def getOtherGroups(self):
        return self.Groups

    def getLastActivity(self):
        if self.LastActivity == Profile.NEVER_ACTIVE:
            return date(1970,1,1)
        else:
            splitList = re.split('/',self.LastActivity)
            return date(int(splitList[2]),int(splitList[1]),int(splitList[0]))

    def save(self):
        if not os.path.exists("Profiles"):
            os.mkdir("Profiles")
        fileName = os.path.join("Profiles","-%s.ini" % self.Id)

        config              =   ConfigParser.ConfigParser()
        config.optionxform  =   str
        config.add_section("Details")
        config.set("Details","Name",        self.Name.encode('ascii','ignore'))
        config.set("Details","Age",         self.Age)
        config.set("Details","Gender",      self.Gender)
        config.set("Details","Type",        self.Type)
        config.set("Details","Orientation", self.Orientation)
        config.set("Details","Active",      self.Active)
        config.set("Details","LastActivity",self.LastActivity)

        def saveList(name,values):
            config.add_section(name)
            for value in values:
                config.set(name,value.encode('ascii','ignore').strip(),"")

        def saveDict(name,values):
            config.add_section(name)
            for k,v in values.iteritems():
                config.set(name,k.encode('ascii','ignore').strip(),v.encode('ascii','ignore').strip() )

        saveList("Location",self.Location)
        saveList("LookingFor",self.LookingFor)
        saveList("Groups",self.Groups)
        saveList("Friends",self.Friends)
        saveDict("Relationships",self.Relationships)
        with open(fileName,'wb') as configFile:
            config.write(configFile)

        os.rename(fileName,os.path.join("Profiles","-%s.ini" % self.Id))

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
            for k,v in self.Relationships.iteritems():
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
        rv +=   "[%s][%16s]" % (self.Id,"Groups")
        if len(self.Groups) == 0:
            rv += "\n"
        else:
            first   =   True
            for k in self.Groups:
                if not first:
                    rv += "[%s] %16s " % (self.Id,"")
                else:
                    first = False
                rv += "[%32s]\n" % (k)

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
 
        rv += "\n"
        return rv

