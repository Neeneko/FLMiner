import optparse
import sys
import os
import cPickle as Pickle
from Profile import Profile
from Crawler import Progress,FauxParser

class DataBlob(object):
    def __init__(self,profiles,progress):
        self.__profiles =   profiles
        self.__progress =   progress.getRawData()

    def getProfiles(self):
        return self.__profiles

    def getProgress(self):
        return Progress(raw_data=self.__progress)

def LoadBlob(file_name):
    if not os.path.exists(file_name):
        return None
    with open(file_name,"rb") as fp:
        return Pickle.load(fp)

def SaveBlob(blob,file_name):
    #fileName    =   os.path.join("Data","profiles.dat")
    with open(file_name,"wb") as fp:
        Pickle.dump(blob,fp)
 
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
