import optparse
import sys
import glob
import os
import re
from Viewer import ReportProfile
from Crawler import Progress,FauxParser


if __name__ == "__main__":
    sys.stderr.write("Begingin Data Validation\n")
    progress    =   Progress()
    if not progress.validate():
        sys.stderr.write("Progress has errors, Correcting\n")
        progress.fix()

        if not progress.validate():
            sys.stderr.write("Still has issues\n")
        else:
            progress.saveProgress()

    oldNames = glob.glob(os.path.join("Profiles","*.ini"))
    sys.stderr.write("[%d] files in old format\n" % len(oldNames))
    for oldName in oldNames:
        number  =   int(re.sub(r'[^0-9]','', oldName))
        profile =   ReportProfile(number)
        profile.load()


    datNames = glob.glob(os.path.join("Profiles","*.dat"))
    sys.stderr.write("[%d] files in new format\n" % len(datNames))
    for datName in datNames:
        number  =   int(re.sub(r'[^0-9]','', datName))
        profile =   ReportProfile(number)
        profile.load()
        if not profile.validate():
            sys.stderr.write("Profile [%s] has errors\n" % profile.Id)
            profile.fix()

            if not profile.validate():
                sys.stderr.write("Profile [%s] still has errors\n" % profile.Id)
            else:
                profile.save()
        del profile



