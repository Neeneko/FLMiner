import os
import sys
import time
import cPickle as pickle
from multiprocessing import Lock

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
                if section in self.__sections and int(key) in self.__sections[section]:
                    return True
                else:
                    return False

        def getString(self,section,key):
            with self.__mutex:
                if section in self.__sections and int(key) in self.__sections[section]:
                    return self.__sections[section][int(key)]
                else:
                    return None

        def getSection(self,section):
            with self.__mutex:
                return self.__sections[section]

    def __init__(self):
           if StringMap.__instance is None:
                StringMap.__instance = StringMap.__impl()

    def __getattr__(self, attr):
        return getattr(self.__instance, attr)

    def __setattr__(self, attr, value):
        return setattr(self.__instance, attr, value)

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

    SECTIONS    =   ["PendingProfiles","ErrorProfiles","MissingProfiles","CompletedProfiles","ActiveProfiles"]

    def __init__(self,rebuild=False,raw_data=None,clear=False):
        self.__mutex        =   Lock()
        with self.__mutex:            
            self.__dataPath     =   "Data"
            self.__progressFile =   os.path.join(self.__dataPath,"progress.dat")
            self.__progressTemp =   os.path.join(self.__dataPath,"progress.tmp")
            self.__exit     =   False
            self.__count    =   0
            self.__rate     =   0
            self.__nextTime = time.time()+60
            if clear is True:
                self.__clear()
            elif raw_data is not None:
                self.__progress = raw_data
            elif rebuild is True:
                self.__clear()
                self.__rebuild()
            elif os.path.exists(self.__progressFile):       
                self.__progress = pickle.load( open( self.__progressFile, "rb" ) )
                sys.stderr.write("Moving [%s] Active back to Pending\n" % self.__progress.len("ActiveProfiles"))
                while self.__progress.len("ActiveProfiles") != 0:
                    oldId = self.__progress.pop("ActiveProfiles")
                    self.__progress.set("PendingProfiles",oldId)
            else:
                self.__clear()
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

    def __clear(self):
        self.__progress =   FauxParser()
        self.__progress.add_section("PendingProfiles")
        self.__progress.add_section("ErrorProfiles")
        self.__progress.add_section("MissingProfiles")
        self.__progress.add_section("CompletedProfiles")
        self.__progress.add_section("ActiveProfiles")

        self.__progress.add_section("PendingGroups")
        self.__progress.add_section("ErrorGroups")
        self.__progress.add_section("MissingGroups")
        self.__progress.add_section("CompletedGroups")
        self.__progress.add_section("ActiveGroups")

    def initPending(self,pid,gid):
        self.__progress.clear()
        if pid is not None:
            self.__progress.set("PendingProfiles",pid)
        if gid is not None:
            self.__progress.set("PendingGroups",gid)
        self.__saveProgress()

    def __rebuild(self):
        sys.stderr.write("Starting Rebuild\n")
        raise NotImplementedError
        self.__progress.clear()
        fileNames = glob.glob(os.path.join("Profiles","*.dat"))
        if len(fileNames) == 0:
            sys.stderr.write("No Data, defaulting.\n")
            self.__progress.set("PendingProfiles",1)
        else:
            profiles        =   set()
            count           =   0
            for fileName in fileNames:
                if count%1024 == 0:
                    sys.stderr.write("[0] Progress - Loaded [%s]\n" % count)
                uid         =   re.sub(r'[^0-9]','', fileName)
                profile =   Profile(uid)
                if(profile.load()):
                    profiles.add(profile)
                    self.__progress.set("CompletedProfiles",uid)
                else:
                    self.errorProfile(uid)
                count += 1
            
            count           =   0
            for profile in profiles:
                if count%1024 == 0:
                    sys.stderr.write("[1] Progress - Processed [%s]\n" % count)
                for opid in profile.getOtherProfiles():
                    if not self.__progress.has_option("PendingProfiles",opid)            \
                        and not self.__progress.has_option("CompletedProfiles",opid)     \
                        and not self.__progress.has_option("ErrorProfiles",opid)         \
                        and not self.__progress.has_option("MissingProfiles",opid)       \
                        and not self.__progress.has_option("ActiveProfiles",opid):
                        self.__progress.set("PendingProfiles",opid)
                count += 1

        sys.stderr.write("Done Rebuild\n")

    def setExit(self):
        sys.stderr.write("Shutting Down\n")
        self.__exit =   True

    def getExit(self):
        return self.__exit
    #------------------------------------------------------------------------------------------
    def __knownProfile(self,pid):
        return (            self.__progress.has_option("PendingProfiles",pid)      \
                    or      self.__progress.has_option("CompletedProfiles",pid)    \
                    or      self.__progress.has_option("ErrorProfiles",pid)        \
                    or      self.__progress.has_option("MissingProfiles",pid)      \
                    or      self.__progress.has_option("ActiveProfiles",pid)        )
 
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

    def missingProfile(self,pid):
        with self.__mutex:
            if pid in self.__progress.options("ActiveProfiles"):
                self.__progress.remove_option("ActiveProfiles",pid)
            self.__progress.set("MissingProfiles",pid)

    def completeProfile(self,pid,op,og):
        sys.stderr.write("Completing [%s]\n" % pid)
        with self.__mutex:
            for opid in op:
                if not self.__knownProfile(opid):
                    self.__progress.set("PendingProfiles",opid)
            for ogid in og:
                if not self.__knownProfile(ogid):
                    self.__progress.set("PendingGroups",ogid)

            if self.__progress.has_option("ActiveProfiles",pid):
                self.__progress.remove_option("ActiveProfiles",pid)
            else:
                raise RuntimeError,"pid [%s] not in ActiveProfiles\n" % pid
            self.__progress.set("CompletedProfiles",pid)
    #------------------------------------------------------------------------------------------
    def __knownGroup(self,gid):
        return (            self.__progress.has_option("PendingGroups",gid)      \
                    or      self.__progress.has_option("CompletedGroups",gid)    \
                    or      self.__progress.has_option("ErrorGroups",gid)        \
                    or      self.__progress.has_option("MissingGroups",gid)      \
                    or      self.__progress.has_option("ActiveGroups",gid)      )
 
    def nextGroup(self):
        with self.__mutex:
            if self.__progress.len("PendingGroups") == 0:
                return None
            self.__count += 1
            rv = self.__progress.pop("PendingGroups")
            self.__progress.set("ActiveGroups",rv)
            return rv 

    def errorGroup(self,gid):
        with self.__mutex:
            if gid in self.__progress.options("ActiveGroups"):
                self.__progress.remove_option("ActiveGroups",gid)
            self.__progress.set("ErrorGroups",gid)

    def missingGroup(self,gid):
        with self.__mutex:
            if gid in self.__progress.options("ActiveGroups"):
                self.__progress.remove_option("ActiveGroups",gid)
            self.__progress.set("MissingGroups",gid)

    def completeGroup(self,gid,op):
        sys.stderr.write("Completing [%s]\n" % gid)
        with self.__mutex:
            for opid in op:
                if not self.__knownProfile(opid):
                    self.__progress.set("PendingProfiles",opid)

            if self.__progress.has_option("ActiveGroups",gid):
                self.__progress.remove_option("ActiveGroups",gid)
            else:
                raise RuntimeError,"gid [%s] not in ActiveGroups\n" % gid
            self.__progress.set("CompletedGroups",gid)
 
    #------------------------------------------------------------------------------------------
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
            self.__printProgress()

    def __printProgress(self):
        sys.stderr.write("Progress: Rate [%d] Active [%d:%d] Completed [%d:%d] Pending [%d:%d] Error [%d:%d] Missing [%d:%d]\n" %  (        
                self.__count,
                self.__progress.len("ActiveProfiles"),      self.__progress.len("ActiveGroups"),
                self.__progress.len("CompletedProfiles"),   self.__progress.len("CompletedGroups"),
                self.__progress.len("PendingProfiles"),     self.__progress.len("PendingGroups"), 
                self.__progress.len("ErrorProfiles"),       self.__progress.len("ErrorGroups"),
                self.__progress.len("MissingProfiles"),     self.__progress.len("MissingGroups")
                ))
        self.__count    =   0


    def getIds(self,section):
        with self.__mutex:
            return self.__progress.options(section)

