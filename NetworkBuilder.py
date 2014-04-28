import optparse
import sys
from Profile import ReportProfile
from Crawler import Progress,FauxParser
from Blobber import GetBlob,SaveBlob

def clearDegrees(profiles):
    sys.stderr.write("Clearing Degree\n")
    reset   =   0
    added   =   0
    for profile in profiles.itervalues():
        try:
            if profile.Degree != None:
                profile.Degree = None
                reset += 1
        except AttributeError:
            profile.Degree  =   None
            added += 1

    sys.stderr.write("Reset [%d] Added [%d]\n" % (reset,added))

def spiderFix(profiles,profile):
    pending                 =   set( [profile] )
    while len(pending) > 0:
        profile     =   pending.pop()
        nextDegree  =   profile.Degree + 1
        for otherUID in profile.getOtherProfiles():
            other   =   profiles.get(otherUID,None)
            if other is None:
                pass
            elif other.Degree is None:
                pass
            elif other.Degree > nextDegree:
                other.Degree    =   nextDegree
                pending.add(other)
               
def buildNetwork(profiles,origin):
    profiles[origin].Degree =   0
    pending                 =   set( [profiles[origin]] )
    complete                =   0
    loops                   =   0
    jumps                   =   0
    bad                     =   0
    remaining               =   len(profiles)
    maxDegree               =   0
    while len(pending) > 0:
        sys.stderr.write("Complete [%8d] Loops [%8d] Jumps [%8d] Bad [%8d] Max Degree [%2d] Remaining [%8d]\n" % (complete,loops,jumps,bad,maxDegree,remaining))
        profile     =   pending.pop()
        nextDegree  =   profile.Degree + 1
        maxDegree   =   max(nextDegree,maxDegree)
        for otherUID in profile.getOtherProfiles():
            other   =   profiles.get(otherUID,None)
            if other is None:
                bad += 1
            elif other.Degree is None:
                other.Degree    =   nextDegree
                pending.add(other)
            elif other.Degree > nextDegree:
                #TODO we should do something about these
                jumps   +=  1
                other.Degree    =   nextDegree
                spiderFix(profiles,other)
            else:
                loops   +=  1

        complete    += 1
        remaining   -= 1

def checkNetwork(profiles):
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


if __name__ == "__main__":
    usage       =   "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-o', '--origin', help="profile number to start from", type='int', default=None)
    parser.add_option('-b', '--blob', help="user the blob rather then loading profiles", action="store_true",default=False)

    options, args = parser.parse_args()
    if options.origin is None:
        parser.print_help()
        sys.exit()

    if options.blob:
        sys.stderr.write("Loading Blob\n")
        profileMap  =   GetBlob()
    else:
        progress    =   Progress()
        uids        =   set(progress.getIds("CompletedProfiles"))
        profileMap  =   {}
        sys.stderr.write("Profiles to load: [%s]\n" % len(uids))
        count = 0
        for uid in uids:
            if count%1024 == 0:
                sys.stderr.write("Progress - Loaded [%s]\n" % count)
            profile =   ReportProfile(uid)
            if(profile.load()):
                profileMap[uid] =   profile
            count += 1
    sys.stderr.write("Loaded [%d] Profiles\n" % len(profileMap))
    if options.origin not in profileMap:
        sys.stderr.write("Profile Id [%s] not in provided data set\n" % options.origin)
        sys.exit(0)
    clearDegrees(profileMap)
    buildNetwork(profileMap,options.origin)
    checkNetwork(profileMap)
    SaveBlob(profileMap)
