import optparse
import sys
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
def clearNetwork(profile_db):
    sys.stderr.write("Clearing Network\n")
    profile_db.RunRawQuery("UPDATE Profiles Set Degree=-1")
    sys.stderr.write("Cleared Network\n")

def spiderFix(profile_db,profile_id):
    pending                 =   set( [profile_id] )
    while len(pending) > 0:
        profileId   =   pending.pop()
        nextDegree  =   getProfileDegree(profile_db,profileId) + 1
        for otherId in getOtherProfiles(profile_db,profileId):
            if otherId not in profile_db.GLOBAL_PROFILE_IDS:
                pass
            elif getProfileDegree(profile_db,otherId) == -1:
                pass
            elif getProfileDegree(profile_db,otherId)  > nextDegree:
                setProfileDegree(profile_db,otherId,nextDegree)
                pending.add(otherId)

def buildNetwork(profile_db,origin):
    sys.stderr.write("Building Network from [%s]\n" % origin)

    profile_db.RunRawQuery("UPDATE Profiles Set Degree=0 WHERE Id=?",origin)
    pending                         =   set( [origin] )
    complete                        =   0
    loops                           =   0
    jumps                           =   0
    bad                             =   0
    profile_db.GLOBAL_PROFILE_IDS   =   profile_db.GetAllProfileIds()
    remaining                       =   len(profile_db.GLOBAL_PROFILE_IDS)
    maxDegree                       =   0
    while len(pending) > 0:
        sys.stderr.write("Complete [%8d] Loops [%8d] Jumps [%8d] Bad [%8d] Max Degree [%2d] Remaining [%8d]\n" % (complete,loops,jumps,bad,maxDegree,remaining))
        profileId   =   pending.pop()
        nextDegree  =   getProfileDegree(profile_db,profileId) + 1
        maxDegree   =   max(nextDegree,maxDegree)
        for otherId in getOtherProfiles(profile_db,profileId):
            if otherId not in profile_db.GLOBAL_PROFILE_IDS:
                bad += 1
            elif getProfileDegree(profile_db,otherId) == -1:
                setProfileDegree(profile_db,otherId,nextDegree)
                pending.add(otherId)
            elif getProfileDegree(profile_db,otherId) > nextDegree:
                jumps   +=  1
                setProfileDegree(profile_db,otherId,nextDegree)
                spiderFix(profile_db,otherId)
            else:
                loops   +=  1

        complete    += 1
        remaining   -= 1

    return

def checkNetwork(profile_db):
    return
    errorHigh   =   0
    errorLow    =   0
    sys.stderr.write("Checking Network\n")
    for profile in profiles.itervalues():
        for otherUID in profile.getOtherProfiles():
            other   =   profiles.get(otherUID,None)
            if other is None:
                pass
            elif other.Degree > (profile.Degree + 1):
                errorHigh   += 1
            elif other.Degree < (profile.Degree -1):
                errorLow    += 1

    sys.stderr.write("Errors: Too High [%d] Too Low [%d]\n" % (errorHigh,errorLow))

class NetworkBuilder(object):

    def __init__(self,db):
        self.__db   =   db
        sys.stderr.write("Init Network Builder\n")
        maxId           =   self.__db.RunRawQuery("SELECT MAX(Id) from Profiles")[0][0]
        sys.stderr.write("Max Id [%s]\n" % maxId)
        self.__allIds   =   self.__db.GetAllProfileIds()
        self.__degrees  =   dict.fromkeys(self.__allIds,-1)
        self.__otherIds =   [None] * (maxId+1)
        for row in self.__db.RunRawQuery("SELECT DISTINCT Id,DstId from Relationships"):
            if self.__otherIds[row[0]] is None:
                self.__otherIds[row[0]] = set()
            self.__otherIds[row[0]].add(row[1])
            del row
        for row in self.__db.RunRawQuery("SELECT Id,DstId from Friends"):
            if self.__otherIds[row[0]] is None:
                self.__otherIds[row[0]] = set()
            self.__otherIds[row[0]].add(row[1])
            del row
        sys.stderr.write("Fini Network Builder\n")

    def clearNetwork(self,profile_id=None):
        sys.stderr.write("Clearing Network [%s]\n" % profile_id)
        if profile_id is None:
            self.__db.RunRawQuery("DELETE FROM Degrees")
        else:
            self.__db.RunRawQuery("DELETE FROM Degrees WHERE DstId=?", profile_id)

        self.__degrees  =   dict.fromkeys(self.__allIds,-1)
        sys.stderr.write("Cleared Network\n")

    def spiderFix(self,profile_id):
        pending                 =   set( [profile_id] )
        while len(pending) > 0:
            profileId   =   pending.pop()
            nextDegree  =   self.__degrees[profileId] + 1
            for otherId in self.__otherIds[profileId]:
                if otherId not in self.__allIds:
                    pass
                elif self.__degrees[otherId] == -1:
                    pass
                elif self.__degrees[otherId]  > nextDegree:
                    self.__degrees[otherId] = nextDegree
                    pending.add(otherId)

    def buildNetwork(self,origin):
        sys.stderr.write("Building Network from [%s]\n" % origin)
        pending                         =   set( [origin] )
        self.__degrees[origin]          =   0
        complete                        =   0
        loops                           =   0
        jumps                           =   0
        bad                             =   0
        remaining                       =   len(self.__allIds)
        maxDegree                       =   0
        while len(pending) > 0:
            sys.stderr.write("Complete [%8d] Loops [%8d] Jumps [%8d] Bad [%8d] Max Degree [%2d] Remaining [%8d]\n" % (complete,loops,jumps,bad,maxDegree,remaining))
            profileId   =   pending.pop()
            nextDegree  =   self.__degrees[profileId]+1
            maxDegree   =   max(nextDegree,maxDegree)
            sys.stderr.write("\tProfileId [%s] NextDegree [%s] OtherIds [%s]\n" % (profileId,nextDegree,len(self.__otherIds[profileId])))
            for otherId in self.__otherIds[profileId]:
                if otherId not in self.__allIds:
                    bad += 1
                elif self.__degrees[otherId] == -1:
                    self.__degrees[otherId] = nextDegree
                    pending.add(otherId)
                elif self.__degrees[otherId]  > nextDegree:
                    self.__degrees[otherId] = nextDegree
                    jumps   +=  1
                    self.spiderFix(otherId)
                else:
                    loops   +=  1
            complete    += 1
            remaining   -= 1

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

