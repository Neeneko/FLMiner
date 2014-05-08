import optparse
import sys
import os
import re
import cPickle as Pickle
import sqlite3
from datetime import date,datetime
from Profile import Profile
from Crawler import Progress,FauxParser

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
        cursor.execute("CREATE TABLE Enums (Enum INT, Value TEXT, Origin TEXT)")

        for field in Profile.LIST_FIELDS:
            cursor.execute("DROP TABLE IF EXISTS %s" % field)
            cursor.execute("CREATE TABLE %s(Id INT, Enum INT)" % (field))

        for field in Profile.LIST_TUPLE_FIELDS:
            cursor.execute("DROP TABLE IF EXISTS %s" % field)
            cursor.execute("CREATE TABLE %s(Id INT, DstId INT, Enum INT)" % (field))

        for field in Profile.FETISH_FIELDS:
            cursor.execute("DROP TABLE IF EXISTS Fetish%s" % field)
            cursor.execute("CREATE TABLE Fetish%s(Id INT, Fetish INT, Enum INT)" % (field))

        self.__db.commit()

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
        return cursor.fetchall()

    def GetSection(self,section):
        cursor = self.__db.cursor()
        cursor.execute("SELECT Id FROM %s" % section)
        return cursor.fetchall()

    def GetProfiles(self,*args,**kwargs):
        sString =   "Profiles"
        cString =   ""
        filters =   []
        for idx in range(len(args)):
            if idx != 0:
                cString += ","
            if '.' in args[idx]:

                if "Enums" not in sString:
                    sString += ",Enums"

                splitList = re.split("\.",args[idx])
                table = splitList[0]
                if table not in sString:
                    sString += ",%s" % table
                    filters.append("Profiles.Id = %s.Id" % table)
                    filters.append("Enums.Enum = %s" % args[idx])

                cString += "Enums.Value"
            else:
                cString += args[idx]
                
        if "activeInDays" in kwargs:
            filters.append("CrawlDate-LastActivity < %d" % kwargs["activeInDays"])

        if "filterByField" in kwargs:
            splitList = re.split(',',kwargs["filterByField"])
            filters.append("WHERE %s=%s" % (splitList[0],splitList[1]))

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
        cursor      =   self.__db.cursor()
        cursor.execute(eString)
        return cursor.fetchall()

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
            #args.append(getattr(profile,field))

        insertString += "?)"
        if profile.Gender in Profile.GENDER_GROUP_MALE:
            args.append(Profile.GENDER_MALE_TITLE)
        elif profile.Gender in Profile.GENDER_GROUP_FEMALE:
            args.append(Profile.GENDER_FEMALE_TITLE)
        else:
            args.append(Profile.GENDER_OTHER_TITLE) 

        cursor.execute(insertString,tuple(args))

        for field in Profile.LIST_FIELDS:
            for value in getattr(profile,field):
                cursor.execute("INSERT INTO %s VALUES(?,?)" % field,(profile.Id,value))

        cursor.execute("SELECT * FROM Enums")
        enumRows =   cursor.fetchall()

        for field in Profile.LIST_TUPLE_FIELDS:
            for (dst,text) in getattr(profile,field):
                if text not in [ x[1] for x in enumRows]:
                    sys.stderr.write("Enum [%s] not in DB, Adding\n" % text.encode("utf-8"))
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
                    sys.stderr.write("Enum [%s] not in DB, Adding\n" % text.encode("utf-8"))
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

        for field in Profile.FETISH_FIELDS:
            for (text,values) in getattr(profile,field).iteritems():
                if text not in [ x[1] for x in enumRows]:
                    sys.stderr.write("Enum [%s] not in DB, Adding\n" % text.encode("utf-8"))
                    if len(enumRows) == 0:
                        nextId  =   0   
                    else:
                        nextId      =   max([x[0] for x in enumRows])+1
                    cursor.execute("INSERT INTO Enums VALUES(?,?,?)", (nextId,text,field))
                    cursor.execute("SELECT * FROM Enums")
                    enumRows    =   cursor.fetchall()
                thisId  =   None
                for enumId,enumText,_ in enumRows:
                    if enumText == text:
                        thisId = enumId
                        break
                for value in values:
                    cursor.execute("INSERT INTO Fetish%s VALUES(?,?,?)" % field,(profile.Id,value,thisId))

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
    """
    if options.create:

        progress    =   Progress()
        uids        =   set(progress.getIds("CompletedProfiles"))
        profileMap  =   {}
        sys.stderr.write("Profiles to load: [%s]\n" % len(uids))
        count = 0
        for uid in uids:
            if count%1024 == 0:
                sys.stderr.write("Progress - Loaded [%s]\n" % count)
            profile =   Profile(uid)
            if(profile.load()):
                profileMap[uid] =   profile
            else:
                progress.errorProfile(uid)
            count += 1
        sys.stderr.write("Loaded [%d] Profiles\n" % len(profileMap))
    
        sys.stderr.write("Writing Blob [%s]\n" % fileName)
        SaveBlob(DataBlob(profileMap,progress),fileName)
        sys.stderr.write("Done Writing Blob\n")
    elif options.validate:
        dataBlob    =   LoadBlob(fileName)
        progress    =   dataBlob.getProgress()
        progress.printProgress()
        profiles    =   dataBlob.getProfiles()
        sys.stderr.write("[%d] Profiles\n" % len(profiles))
    else:
        raise RuntimeError
    """
