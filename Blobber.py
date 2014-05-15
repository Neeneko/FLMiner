import optparse
import sys
import os
import re
import cPickle as Pickle
import sqlite3
from datetime import date,datetime
from Profile import Profile
from Crawler import Progress,FauxParser,StringMap

def debug(mesg):
    sys.stderr.write("%s\n" % mesg.encode("utf-8"))
    sys.stderr.flush()

class ProfileDb(object):

    def __init__(self,file_name):
        self.__db   =   sqlite3.connect(file_name,detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)

    def Clear(self):
        cursor = self.__db.cursor()
        for section in Progress.SECTIONS:
            cursor.execute("DROP TABLE IF EXISTS %s" % section)
            cursor.execute("CREATE TABLE %s(Id INT)" % section)

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

        cursor.execute("DROP TABLE IF EXISTS Enums")
        cursor.execute("CREATE TABLE Enums (Enum INT, Value TEXT, SrcTable TEXT)")

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
 
    def GetProfiles(self,*args,**kwargs):
        cursor      =   self.__db.cursor()
        sString     =   "Profiles"
        cString     =   ""
        filters     =   []
        cursor.execute("SELECT DISTINCT SrcTable FROM Enums")
        eNumTables  =   [x[0] for x in cursor.fetchall()]
        for idx in range(len(args)):
            if idx != 0:
                cString += ","
            if '.' in args[idx]:

                splitList = re.split("\.",args[idx])
                table = splitList[0]
                if table in eNumTables and "Enums" not in sString:
                    sString += ",Enums"

                if table not in sString:
                    sString += ",%s" % table
                    filters.append("Profiles.Id = %s.Id" % table)
                    if table in eNumTables:
                        filters.append("Enums.Enum = %s" % args[idx])

                if table in eNumTables:
                    cString += "Enums.Value"
                else:
                    cString += args[idx]
            else:
                cString += args[idx]
                
        if "activeInDays" in kwargs:
            filters.append("CrawlDate-LastActivity < %d" % kwargs["activeInDays"])

        if "filterEqField" in kwargs:
            filters.append("%s=\"%s\"" % kwargs["filterEqField"])

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
        debug("Adding Profile [%s]" % profile.Id) 
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
            #args.append(getattr(profile,field))

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

        cursor.execute("SELECT * FROM Enums")
        enumRows =   cursor.fetchall()

        for field in Profile.LIST_TUPLE_FIELDS:
            for (dst,text) in getattr(profile,field):
                if text not in [ x[1] for x in enumRows]:
                    #sys.stderr.write("Enum [%s] not in DB, Adding\n" % text.encode("utf-8"))
                    if len(enumRows) == 0:
                        nextId  =   0   
                    else:
                        nextId      =   max([x[0] for x in enumRows])+1
                    cursor.execute("INSERT INTO Enums VALUES(?,?,?)",(nextId,text,field))
                    cursor.execute("SELECT * FROM Enums")
                    enumRows    =   cursor.fetchall()
                thisId  =   None
                for enumId,enumText,_ in enumRows:
                    if enumText == text:
                        thisId = enumId
                        break
                cursor.execute("INSERT INTO %s VALUES(?,?,?)" % field,(profile.Id,dst,thisId))

        for field in Profile.LIST_FIELDS:
            for text in getattr(profile,field):
                if text not in [ x[1] for x in enumRows]:
                    #sys.stderr.write("Enum [%s] not in DB, Adding\n" % text.encode("utf-8"))
                    if len(enumRows) == 0:
                        nextId  =   0   
                    else:
                        nextId      =   max([x[0] for x in enumRows])+1
                    cursor.execute("INSERT INTO Enums VALUES(?,?,?)",(nextId,text,field))
                    cursor.execute("SELECT * FROM Enums")
                    enumRows    =   cursor.fetchall()
                thisId  =   None
                for enumId,enumText,_ in enumRows:
                    if enumText == text:
                        thisId = enumId
                        break
                cursor.execute("INSERT INTO %s VALUES(?,?)" % field,(profile.Id,thisId))

        stringMap   =   StringMap()
        #cursor.execute("CREATE TABLE Fetishes (Id INTEGER PRIMARY KEY, Name TEXT)")
        #cursor.execute("CREATE TABLE ProfileToFetish(ProfileId INT, FetishId INT, Relation TEXT)")
        for field in Profile.FETISH_FIELDS:
            debug("\t[%s] - [%s]" % (field,len(getattr(profile,field))))
            for (text,values) in getattr(profile,field).iteritems():
                if text not in [ x[1] for x in enumRows]:
                    #sys.stderr.write("Enum [%s] not in DB, Adding\n" % text.encode("utf-8"))
                    if len(enumRows) == 0:
                        nextId  =   0   
                    else:
                        nextId      =   max([x[0] for x in enumRows])+1
                    cursor.execute("INSERT INTO Enums VALUES(?,?,?)",(nextId,text,field))
                    cursor.execute("SELECT * FROM Enums")
                    enumRows    =   cursor.fetchall()
                thisId  =   None
                for enumId,enumText,_ in enumRows:
                    if enumText == text:
                        thisId = enumId
                        break
 
                debug("\t\t[%s][%s] - [%s]" % (text,thisId,len(values)))
                for value in values:
                    fetish    =   stringMap.getString("Fetish",value)
                    debug("\t\t\t[%s] - [%s]" % (value,fetish))
                    cursor.execute("SELECT COUNT(*) FROM Fetishes WHERE Name=?",[fetish])
                    if cursor.fetchall()[0][0] == 0:
                        cursor.execute("INSERT INTO Fetishes VALUES(?,?)", (value,fetish))
                    cursor.execute("INSERT INTO ProfileToFetish VALUES(?,?,?)" ,(profile.Id,value,thisId))
        self.__db.commit()

def LoadSavedBlob(file_name):
    return ProfileDb(file_name)

def CreateLiveBlob(file_name):
    profileDb   =   ProfileDb(file_name)
    profileDb.Clear()
    progress    =   Progress()
    for section in Progress.SECTIONS:
        profileDb.FillSection(section,progress.getIds(section))
    uids        =   set(progress.getIds("CompletedProfiles"))
    sys.stderr.write("Profiles to load: [%s]\n" % len(uids))
    count       =   0
    for uid in uids:
        if count%1024 == 0:
            sys.stderr.write("Progress - Loaded [%s]\n" % count)
        profile =   Profile(uid)
        if(profile.load()):
            profileDb.AddProfile(profile)
        else:
            progress.errorProfile(uid)
        del profile
        count += 1
    sys.stderr.write("Loaded [%d] Profiles. [%d] Errors.\n" % (len(profileDb.GetAllProfileIds()),len(progress.getIds("ErrorProfiles"))))
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
