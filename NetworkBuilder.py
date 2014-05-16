import optparse
import sys
import array
from Profile import Profile
from Blobber import CreateMemoryOnlyBlob,LoadSavedBlob

#from Crawler import Progress,FauxParser
#from Blobber import LoadBlob,SaveBlob

#------------------------------------------------------------------------------------------------
def getProfileDegree(profile_db,profile_id):
    rows    =   profile_db.RunRawQuery("SELECT Degree from Profiles WHERE ID=?",profile_id)
    #sys.stderr.write("[%s] rows - %s\n" % (profile_id,rows))
    assert len(rows) == 1
    assert len(rows[0]) == 1
    return rows[0][0]

def setProfileDegree(profile_db,profile_id,degree):
    profile_db.RunRawQuery("UPDATE Profiles Set Degree=? WHERE Id=?",degree,profile_id)

def getOtherProfiles(profile_db,profile_id):
    r =   set([ x[0] for x in profile_db.RunRawQuery("SELECT DstId from Relationships WHERE ID=?",profile_id)])
    f =   set([ x[0] for x in profile_db.RunRawQuery("SELECT DstId from Friends WHERE ID=?",profile_id)])
    return r|f
#------------------------------------------------------------------------------------------------
class NetworkBuilder(object):

    def __init__(self,db):
        self.__db       =   db
        cursor          =   db.GetCursor()
        sys.stderr.write("Init Network Builder\n")
        self.__maxId    =   self.__db.RunRawQuery("SELECT MAX(Id) from Profiles")[0][0]
        sys.stderr.write("Max Id [%s]\n" % self.__maxId)
        self.__allIds   =   self.__db.GetAllProfileIds()
        self.__degrees  =   self.__buildByteArray(self.__maxId+1,-1)
        self.__otherIds =   [None] * (self.__maxId+1)
        sys.stderr.write("Relationships\n")
        cursor.execute("SELECT DISTINCT Id,DstId from Relationships")
        count   =   0
        while True:
            row = cursor.fetchone()
            if row is None:
                sys.stderr.write("\t[%s] Total\n" % count)
                break
            count   +=  1
            if count%(1024*1024) == 0:
                sys.stderr.write("\t[%s]\n" % count)
            if self.__otherIds[row[0]] is None:
                self.__otherIds[row[0]] = self.__buildLongArray(0,0)
            self.__otherIds[row[0]].append(row[1])
            del row
        sys.stderr.write("Friends\n")
        cursor.execute("SELECT Id,DstId from Friends")
        count   =   0
        while True:
            row = cursor.fetchone()
            if row is None:
                sys.stderr.write("\t[%s] Total\n" % count)
                break
            count += 1
            if count%(1024*1024) == 0:
                sys.stderr.write("\t[%s]\n" % count)
            if self.__otherIds[row[0]] is None:
                self.__otherIds[row[0]] = self.__buildLongArray(0,0)
            self.__otherIds[row[0]].append(row[1])
            del row
        sys.stderr.write("Fini Network Builder\n")

    def __buildLongArray(self,size,default):
        if size == 0:
            return array.array('L')
        sys.stderr.write("Building Array of len [%s]\n" % size)
        rv  =   array.array('L')
        for _ in range(size):
            rv.append(default)
        sys.stderr.write("Done\n")
        return rv

    def __buildByteArray(self,size,default):
        if size == 0:
            return array.array('b')
        sys.stderr.write("Building Array of len [%s]\n" % size)
        rv  =   array.array('b')
        for _ in range(size):
            rv.append(default)
        sys.stderr.write("Done\n")
        return rv


    def clearNetwork(self,profile_id=None):
        sys.stderr.write("Clearing Network [%s]\n" % profile_id)
        if profile_id is None:
            self.__db.RunRawQuery("DELETE FROM Degrees")
        else:
            self.__db.RunRawQuery("DELETE FROM Degrees WHERE DstId=?", profile_id)

        self.__degrees  =   self.__buildByteArray(self.__maxId+1,-1)
        sys.stderr.write("Cleared Network\n")

    def buildNetwork(self,origin):
        sys.stderr.write("Building Network from [%s]\n" % origin)
        cIds        =   array.array('L',[origin])
        degree      =   0
        tickSize    =   5
        while True:
            didChange   =   0
            nIds        =   array.array('L')
            total       =   len(cIds)
            sys.stderr.write("Degree [%s] Profiles [%12s][" % (degree,total))
            count       =   0
            nextTick    =   tickSize
            while True:
                try:
                    profileId   =   cIds.pop()
                except IndexError:
                    break
                count   +=  1
                if 100*count/total > nextTick:
                    sys.stderr.write(".")
                    #sys.stderr.write("count [%s] total [%s] nextTick [%s] thisTick [%s]\n" % (count, total,nextTick, 100*count/total))
                    nextTick += tickSize
 

                if profileId > self.__maxId:
                    continue
 
                if self.__degrees[profileId] != -1:
                    continue

                self.__degrees[profileId]   =   degree
                didChange                   +=  1

                if self.__otherIds[profileId] is None:
                    continue
                nIds.extend(self.__otherIds[profileId])
            sys.stderr.write("] - [%12s]\n" % didChange)

            if len(nIds) == 0 or didChange == 0:
                break

            cIds    =   nIds
            degree  +=  1

    def writeNetwork(self,origin):
        sys.stderr.write("Start Batch Insert of Network\n")
        count = 1
        for profileId in self.__allIds:
            self.__db.RunRawQuery("INSERT INTO Degrees VALUES(?,?,?)", profileId,origin,self.__degrees[profileId])
            if count%1024 == 0:
                sys.stderr.write("Inserted [%s]\n" % count)
                self.__db.Commit()
            count += 1
        self.__db.Commit()
        sys.stderr.write("Done Batch Insert of Network\n")


    def checkNetwork(self):
        errorHigh   =   0
        errorLow    =   0
        sys.stderr.write("Checking Network\n")
        for profileId in self.__allIds:
            for otherId in  self.__otherIds[profileId]:
                if self.__degrees[profileId] > (self.__degrees[otherId] + 1):
                    errorHigh   += 1
                elif self.__degrees[profileId] > (self.__degrees[otherId] - 1):
                    errorLow    += 1
        sys.stderr.write("Errors: Too High [%d] Too Low [%d]\n" % (errorHigh,errorLow))



#------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    usage       =   "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-o', '--origin', help="profile number to start from", type='int', default=None)
    parser.add_option('-c', '--clear', help="clear degree map",action="store_true",default=False)
    parser.add_option('-b', '--blob', help="user the blob rather then loading profiles", action="store",default=None)

    options, args = parser.parse_args()
    if options.origin is None and options.clear is False:
        parser.print_help()
        sys.exit()

    if options.blob:
        sys.stderr.write("Loading Blob [%s]\n" % options.blob)
        profileDb   =   LoadSavedBlob(options.blob)
    else:
        sys.stderr.write("Loading Profiles\n")
        profileDb   =   CreateMemoryOnlyBlob()

    sys.stderr.write("Loaded [%d] Profiles\n" % len(profileDb.GetAllProfileIds()))
    if options.origin not in profileDb.GetAllProfileIds() and options.clear is False:
        sys.stderr.write("Profile Id [%s] not in provided data set\n" % options.origin)
        sys.exit(0)
    try:
        networkBuilder  =   NetworkBuilder(profileDb)
        if options.clear:
            networkBuilder.clearNetwork(options.origin)
        else:
            networkBuilder.clearNetwork(options.origin)
            networkBuilder.buildNetwork(options.origin)
            networkBuilder.writeNetwork(options.origin)
    finally:
        profileDb.Close() 

