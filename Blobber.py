import optparse
import sys
import os
import cPickle as Pickle
from Profile import Profile
from Crawler import Progress,FauxParser

def GetBlob():
    fileName    =   os.path.join("Data","profiles.dat")
    if not os.path.exists(fileName):
        return None
    with open(fileName,"rb") as fp:
        return Pickle.load(fp)

def SaveBlob(blob):
    fileName    =   os.path.join("Data","profiles.dat")
    with open(fileName,"wb") as fp:
        Pickle.dump(blob,fp)
 

if __name__ == "__main__":

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
        count += 1
    sys.stderr.write("Loaded [%d] Profiles\n" % len(profileMap))
    

    fileName    =   os.path.join("Data","profiles.dat")
    sys.stderr.write("Writing Blob [%s]\n" % fileName)
    SaveBlob(profileMap)
    sys.stderr.write("Done Writing Blob\n")
