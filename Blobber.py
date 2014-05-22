import optparse
import sys
import os
import re
import pickle as Pickle
#import cPickle as Pickle
import sqlite3
from datetime import date,datetime
from Profile import Profile,Group
from Progress import Progress,FauxParser,StringMap

def debug(mesg):
    sys.stderr.write("%s\n" % mesg.encode("utf-8"))
    sys.stderr.flush()

class ProfileDb(object):

    def __init__(self,file_name):
        self.__db           =   sqlite3.connect(file_name,detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
        self.__loadEnums()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.__db.commit()
        self.__db.close()

    def __getEnum(self,text,source):
        if text in self.__enums:
            return self.__enums[text]
        newId               =   len(self.__enums)
        self.__enums[text]  =   newId
        self.__db.execute("INSERT INTO Enums VALUES(?,?,?)",(newId,text,source))
        if source not in self.__enumSrcList:
            self.__enumSrcList.append(source)
        return newId

    def __clearEnums(self):
        self.__enums        =   {}
        self.__enumSrcList  =   []
        cursor              =   self.__db.cursor()
        cursor.execute("DROP TABLE IF EXISTS Enums")
        cursor.execute("CREATE TABLE Enums (Enum INT, Value TEXT, SrcTable TEXT)")

    def __loadEnums(self):
        cursor              =   self.__db.cursor()
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE name=?",["Enums"])
        row = cursor.fetchone()
        if row[0] == 0:
            self.__enums        =   {}
            self.__enumSrcList  =   []
        else:
            cursor.execute("SELECT * FROM Enums")
            self.__enums        =   {}
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                self.__enums[row[1]]    =   row[0]
            cursor.execute("SELECT DISTINCT SrcTable FROM Enums")
            self.__enumSrcList  =   [x[0] for x in cursor.fetchall()]
 
    def __getEnumSrcList(self):
        return self.__enumSrcList

    def Clear(self):
        cursor = self.__db.cursor()
        #-------------------------------------------------------------------------------------
        for section in Progress.SECTIONS:
            cursor.execute("DROP TABLE IF EXISTS %s" % section)
            cursor.execute("CREATE TABLE %s(Id INT)" % section)
        #-------------------------------------------------------------------------------------
        cursor.execute("DROP TABLE IF EXISTS Profiles")
        createString    =   "CREATE TABLE Profiles ("
        createString += "Id INTEGER PRIMARY KEY,"
        for field in Profile.INT_FIELDS:
            if field == "Id":
                continue
            createString += "%s INT," % field

        for field in Profile.TEXT_FIELDS:
            createString += "%s TEXT," % field

        for field in Profile.DATE_FIELDS:
            createString += "%s DATE," % field

        createString += "GenderGroup TEXT)"
        cursor.execute(createString)

        self.__clearEnums()

        for field in Profile.LIST_FIELDS:
            cursor.execute("DROP TABLE IF EXISTS %s" % field)
            cursor.execute("CREATE TABLE %s(Id INT, Enum INT)" % (field))

        for field in Profile.LIST_ID_FIELDS:
            cursor.execute("DROP TABLE IF EXISTS %s" % field)
            cursor.execute("CREATE TABLE %s(Id INT, DstId INT)" % (field))

        for field in Profile.LIST_TUPLE_FIELDS:
            cursor.execute("DROP TABLE IF EXISTS %s" % field)
            cursor.execute("CREATE TABLE %s(Id INT, DstId INT, Enum INT)" % (field))

        cursor.execute("DROP TABLE IF EXISTS Fetishes")
        cursor.execute("CREATE TABLE Fetishes (Id INTEGER PRIMARY KEY, Name TEXT)")
        cursor.execute("DROP TABLE IF EXISTS ProfileToFetish")
        cursor.execute("CREATE TABLE ProfileToFetish(ProfileId INT, FetishId INT, Enum INT)")

        cursor.execute("DROP TABLE IF EXISTS Degrees")
        cursor.execute("CREATE TABLE Degrees (Id INT, DstId INT, Degree INT)")

        cursor.execute("DROP TABLE IF EXISTS IdentGroups")
        cursor.execute("CREATE TABLE IdentGroups(Ident TEXT, IdentGroup TEXT)")
        #-------------------------------------------------------------------------------------
        cursor.execute("DROP TABLE IF EXISTS Groups")
        createString    =   "CREATE TABLE Groups ("
        createString += "Id INTEGER PRIMARY KEY,"
        for field in Group.INT_FIELDS:
            if field == "Id":
                continue
            createString += "%s INT," % field

        for field in Group.TEXT_FIELDS:
            createString += "%s TEXT," % field

        for field in Group.DATE_FIELDS:
            createString += "%s DATE," % field
        createString = createString[:-1]
        createString += ")"
        cursor.execute(createString)

        for field in Group.LIST_ID_FIELDS:
            cursor.execute("DROP TABLE IF EXISTS %s" % field)
            cursor.execute("CREATE TABLE %s(Id INT, DstId INT)" % (field))
        #-------------------------------------------------------------------------------------
        self.__db.commit()

    def InitIdentGroups(self,ident_map):
        cursor = self.__db.cursor()
        for k,v in ident_map.iteritems():
            for vv in v:
                cursor.execute("INSERT INTO IdentGroups VALUES(?,?)",(k,vv))
        self.__db.commit()

    def GetCursor(self):
        return self.__db.cursor()

    def Close(self):
        self.__db.close()
        self.__db   =   None

    def FillSection(self,section,pids):
        cursor = self.__db.cursor()
        for pid in pids:
            cursor.execute("INSERT INTO %s VALUES(?)" % (section),(int(pid),))
        self.__db.commit()

    def FillStrings(self,section,values):
        cursor = self.__db.cursor()
        for k,v in values.iteritems():
            cursor.execute("INSERT INTO %s VALUES(?,?)" % (section),(k,v))
        self.__db.commit()

    def GetProfilesActiveInDays(self,days):
        cursor = self.__db.cursor()
        cursor.execute("SELECT Id FROM Profiles WHERE CrawlDate-LastActivity < %d" % days)
        return cursor.fetchall()

    def GetAllProfileIds(self):
        cursor = self.__db.cursor()
        cursor.execute("SELECT Id FROM Profiles")
        return [x[0] for x in cursor.fetchall()]

    def GetSection(self,section):
        cursor = self.__db.cursor()
        cursor.execute("SELECT Id FROM %s" % section)
        return cursor.fetchall()

    def RunRawQuery(self,query,*args):
        cursor = self.__db.cursor()
        cursor.execute(query,args)
        return cursor.fetchall()

    def Commit(self):
        self.__db.commit()

    def GetProfileName(self,profile_id):
        cursor = self.__db.cursor()
        cursor.execute("SELECT Name FROM Profiles WHERE Id=?",[profile_id])
        rows = cursor.fetchall()
        if len(rows) == 0:
            return None
        assert len(rows) == 1
        return rows[0][0]
 
    def GetProfileCount(self,**kwargs):
        rows        =   self.GetProfiles("COUNT(*)",**kwargs)
        return int(rows[0][0])

    def GetProfiles(self,*args,**kwargs):
        cursor      =   self.__db.cursor()
        sString     =   "Profiles"
        cString     =   ""
        filters     =   []
        for idx in range(len(args)):
            if idx != 0:
                cString += ","
            if '.' in args[idx]:

                splitList = re.split("\.",args[idx])
                table = splitList[0]
                if table in self.__getEnumSrcList() and "Enums" not in sString:
                    sString += ",Enums"

                if table not in sString:
                    sString += ",%s" % table
                    filters.append("Profiles.Id = %s.Id" % table)
                    if table in self.__getEnumSrcList():
                        filters.append("Enums.Enum = %s" % args[idx])

                if table in self.__getEnumSrcList():
                    cString += "Enums.Value"
                else:
                    cString += args[idx]
            else:
                cString += args[idx]
                
        if "activeInDays" in kwargs:
            filters.append("CrawlDate-LastActivity < %d" % kwargs["activeInDays"])

        if "filterEqField" in kwargs:
            for idx in range(len(kwargs["filterEqField"])/2):
                filters.append("%s=\"%s\"" % (kwargs["filterEqField"][idx*2],kwargs["filterEqField"][1+idx*2]))

        if "filterGtField" in kwargs:
            filters.append("%s>\"%s\"" % kwargs["filterGtField"])

        if len(filters) != 0:
            fString =   "WHERE "
        else:
            fString =   ""

        for idx in range(len(filters)):
            if idx != 0:
                fString +=  " AND "
            fString     +=  filters[idx]

        eString     =   "SELECT %s FROM %s %s" % (cString,sString,fString)
        sys.stderr.write("[%s]\n" % eString)
        cursor.execute(eString)
        return cursor.fetchall()

    def GetDegreeOrigins(self):
        cursor      =   self.__db.cursor()
        cursor.execute("SELECT DISTINCT DstId from Degrees")
        return [x[0] for x in cursor.fetchall()]

    def AddProfile(self,profile):
        cursor = self.__db.cursor()

        insertString    =   "INSERT INTO Profiles VALUES("
        args            =   []
        insertString += "?,"
        args.append(profile.Id)

        for field in Profile.INT_FIELDS:
            if field == "Id":
                continue
            insertString += "?,"
            args.append(getattr(profile,field))

        for field in Profile.TEXT_FIELDS:
            insertString += "?,"
            args.append(getattr(profile,field))

        for field in Profile.DATE_FIELDS:
            insertString += "?,"
            dateString  =   getattr(profile,field)
            if dateString == Profile.NEVER_ACTIVE:
                args.append(0)
            else:
                splitList = re.split('/',getattr(profile,field))
                args.append(date(int(splitList[2]),int(splitList[1]),int(splitList[0])).toordinal())

        insertString += "?)"
        if profile.Gender in Profile.GENDER_GROUP_MALE:
            args.append(Profile.GENDER_MALE_TITLE)
        elif profile.Gender in Profile.GENDER_GROUP_FEMALE:
            args.append(Profile.GENDER_FEMALE_TITLE)
        else:
            args.append(Profile.GENDER_OTHER_TITLE) 

        cursor.execute(insertString,tuple(args))

        for field in Profile.LIST_ID_FIELDS:
            for value in getattr(profile,field):
                cursor.execute("INSERT INTO %s VALUES(?,?)" % field,(profile.Id,value))

        for field in Profile.LIST_TUPLE_FIELDS:
            for (dst,text) in getattr(profile,field):
                thisId  =   self.__getEnum(text,field)
                cursor.execute("INSERT INTO %s VALUES(?,?,?)" % field,(profile.Id,dst,thisId))

        for field in Profile.LIST_FIELDS:
            for text in getattr(profile,field):
                thisId  =   self.__getEnum(text,field)
                cursor.execute("INSERT INTO %s VALUES(?,?)" % field,(profile.Id,thisId))

        for field in Profile.FETISH_FIELDS:
            for (text,values) in getattr(profile,field).iteritems():
                thisId  =   self.__getEnum(text,field)
                for value in values:
                    cursor.execute("INSERT INTO ProfileToFetish VALUES(?,?,?)" ,(profile.Id,value,thisId))

        self.__db.commit()

    def AddGroup(self,group):
        cursor = self.__db.cursor()

        insertString    =   "INSERT INTO Groups VALUES("
        args            =   []
        insertString += "?,"
        args.append(group.Id)

        for field in Group.INT_FIELDS:
            if field == "Id":
                continue
            insertString += "?,"
            args.append(getattr(group,field))

        for field in Group.TEXT_FIELDS:
            insertString += "?,"
            args.append(getattr(group,field))

        for field in Group.DATE_FIELDS:
            insertString += "?,"
            dateString  =   getattr(group,field)
            if dateString == Group.NEVER_ACTIVE:
                args.append(0)
            else:
                splitList = re.split('/',getattr(group,field))
                args.append(date(int(splitList[2]),int(splitList[1]),int(splitList[0])).toordinal())
        insertString = insertString[:-1]
        insertString += ")"
 
        cursor.execute(insertString,tuple(args))

        for field in Group.LIST_ID_FIELDS:
            for value in getattr(group,field):
                cursor.execute("INSERT INTO %s VALUES(?,?)" % field,(group.Id,value))

        self.__db.commit()

def LoadSavedBlob(file_name):
    return ProfileDb(file_name)

def CreateLiveBlob(file_name):
    with ProfileDb(file_name) as profileDb:
        profileDb.Clear()

        stringMap   =   StringMap()
        progress    =   Progress()
        for section in Progress.SECTIONS:
            profileDb.FillSection(section,progress.getIds(section))
        profileDb.FillStrings("Fetishes",stringMap.getSection("Fetish"))
        pids        =   set(progress.getIds("CompletedProfiles"))
        sys.stderr.write("Profiles to load: [%s]\n" % len(pids))
        ploaded      =   0
        pfailed      =   0
        ptotal       =   len(pids)
        for pid in pids:
            profile =   Profile(pid)
            if(profile.load()):
                profileDb.AddProfile(profile)
                ploaded   += 1
                sys.stderr.write("Progress - Loaded Profile [%12s], [%12s] of [%12s], [%3s%% Done]\n" % (pid,ploaded,ptotal,100*(ploaded+pfailed)/ptotal))
            else:
                progress.errorProfile(pid)
                pfailed  += 1
                sys.stderr.write("Progress - Failed Profile [%12s], [%12s] of [%12s], [%s%% Done]\n" % (pid,pfailed,ptotal,100*(ploaded+pfailed)/ptotal))
            del profile
        gids        =   set(progress.getIds("CompletedGroups"))
        sys.stderr.write("Groups to load: [%s]\n" % len(gids))
        gloaded      =   0
        gfailed      =   0
        gtotal       =   len(gids)
        for gid in gids:
            group =   Group(gid)
            if(group.load()):
                profileDb.AddGroup(group)
                gloaded   += 1
                sys.stderr.write("Progress - Loaded Group [%12s], [%12s] of [%12s], [%3s%% Done]\n" % (gid,gloaded,gtotal,100*(gloaded+gfailed)/gtotal))
            else:
                progress.errorGroup(gid)
                failed  += 1
                sys.stderr.write("Progress - Failed Group [%12s], [%12s] of [%12s], [%s%% Done]\n" % (gid,gfailed,gtotal,100*(gloaded+gfailed)/gtotal))
            del group
 

        sys.stderr.write("Loaded [%d] Profiles [%d] Groups [%d] Errors.\n" % (ploaded,gloaded,pfailed+gfailed))
        return profileDb

def CreateMemoryOnlyBlob():
    return CreateLiveBlob(":memory:")

if __name__ == "__main__":

    usage       =   "usage: %prog [options] out_file_name"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-c', '--create', help="create blob",action="store_true",default=False)
    parser.add_option('-v', '--validate', help="validate blob",action="store_true",default=False)

    options, args = parser.parse_args()

    if len(args) != 1:
        sys.stderr.write("Please supply blob file name\n")
        sys.exit(0)

    if not (options.create ^ options.validate):
        sys.stderr.write("Please select create or validate\n")
        sys.exit(0)

    fileName    =   args[0]
    if options.create:
        profileDb = CreateLiveBlob(fileName)
        profileDb.Close() 
    elif options.validate:
        raise NotImplementedError
    else:
        raise RuntimeError
