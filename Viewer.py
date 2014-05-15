import os
import sys
import webbrowser
import lxml.html.builder as E
import lxml.etree
import ConfigParser
import cPickle as Pickle
import optparse
from datetime import date,timedelta,datetime
#from Crawler import Crawler,Session
from Crawler import Progress,FauxParser
from Profile import Profile
from Blobber import CreateMemoryOnlyBlob,LoadSavedBlob
from Report import ReportManager,ReportData,MultiGraph,SimpleGraph,PercentHeatMap
from NetworkBuilder import NetworkBuilder

if __name__ == "__main__":
    usage       =   "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-b', '--blob', help="user the blob rather then loading profiles", action="store",default=None)

    options, args = parser.parse_args()

    if options.blob:
        sys.stderr.write("Loading Blob [%s]\n" % options.blob)
        profileDb   =   LoadSavedBlob(options.blob)
    else:
        sys.stderr.write("Loading Profiles\n")
        profileDb   =   CreateMemoryOnlyBlob()

    sys.stderr.write("Completed [%d] Profiles\n" % len(profileDb.GetSection("CompletedProfiles")))
    sys.stderr.write("Missing   [%d] Profiles\n" % len(profileDb.GetSection("MissingProfiles")))
    reportData      =   ReportData()
    reportData.Graphs.append(SimpleGraph("Profiles",preserve_order=True))
    reportData.Graphs[-1].setValue("Missing",len(profileDb.GetSection("MissingProfiles")))
    reportData.Graphs[-1].setValue("Active in  30 days",len(profileDb.GetProfilesActiveInDays(30)))
    reportData.Graphs[-1].setValue("Active in  60 days",len(profileDb.GetProfilesActiveInDays(60)))
    reportData.Graphs[-1].setValue("Active in  90 days",len(profileDb.GetProfilesActiveInDays(90)))
    reportData.Graphs[-1].setValue("Active in 180 days",len(profileDb.GetProfilesActiveInDays(180)))
    reportData.Graphs[-1].setValue("All",len(profileDb.GetAllProfileIds()))
    genderGraph             =   SimpleGraph("Gender",rows=profileDb.GetProfiles("Gender"))
    genderGraphs            =   {}
    genderGraphs["Age"]     =   MultiGraph("Age By Gender",rows=profileDb.GetProfiles("GenderGroup","Age"))
    genderGraphs["Ori"]     =   MultiGraph("Orientation By Gender",vertical=False,rows=profileDb.GetProfiles("GenderGroup","Orientation"))
    genderGraphs["Ident"]   =   MultiGraph("Identity By Gender",vertical=False,rows=profileDb.GetProfiles("GenderGroup","Type"))
    genderGraphs["Active"]  =   MultiGraph("Actvity Type By Gender",vertical=False,rows=profileDb.GetProfiles("GenderGroup","Active"))  
    genderGraphs["RType"]   =   MultiGraph("Relationship Type By Gender",vertical=False,rows=profileDb.GetProfiles("GenderGroup","Relationships.Enum"))  
    genderGraphs["Look"]    =   MultiGraph("Looking For By Gender",vertical=False,rows=profileDb.GetProfiles("GenderGroup","LookingFor.Enum"))  
    reportData.Graphs.append(genderGraph)
    for v in genderGraphs.values():
        reportData.Graphs.append(v)

    sys.stderr.write("Building HeatMaps\n")
    for heatMapOrigin in profileDb.GetDegreeOrigins():
        profileName = profileDb.GetProfileName(heatMapOrigin)
        sys.stderr.write("\tStarting [%s][%s]\n" % (heatMapOrigin,profileName))
        values  =   profileDb.GetProfiles("Degrees.Degree","Age",filterEqField=("GenderGroup","Female"),filterGtField=("Degrees.Degree",0))
        totals  =   profileDb.GetProfiles("Degrees.Degree","Age",filterGtField=("Degrees.Degree",0))
        heatMap =   PercentHeatMap("Percent Women from %s" % profileName,values_rows=values,totals_rows=totals)
        sys.stderr.write("\tDone [%s][%s]\n" % (heatMapOrigin,profileName))
        reportData.Graphs.append(heatMap)
 
    sys.stderr.write("Done Heatmaps\n")


    sys.stderr.write("Examining Fetishes\n")
    #TODO - we might hit the megadict problem here.
    fetishes    =   {}
    fetishes["Totals"]  =   {}
    cursor      =   profileDb.GetCursor()
    count       =   0
    cursor.execute("SELECT * FROM ProfileToFetish")
    while True:
        row = cursor.fetchone()
        if row is None:
            sys.stderr.write("\t[%s] Total\n" % count)
            break
        count += 1
        profileId   =   row[0]
        fetishId    =   row[1]
        relationId  =   row[2]
        if fetishId not in fetishes["Totals"]:
            fetishes["Totals"][fetishId]    =   0
        fetishes["Totals"][fetishId]        +=  1
    #for fetishId,fetishCount in fetishes["Totals"].iteritems():
    #    sys.stderr.write("[%s] => [%s]\n" % (fetishId,fetishCount))

    sortedTotals = sorted(fetishes["Totals"],key=fetishes["Totals"].get,reverse=True)[:32]

    fetishGraph =   SimpleGraph("Top Fetishes",preserve_order=True)
    for fetishId in sortedTotals:
        cursor.execute("SELECT * FROM Fetishes WHERE Id=?",[fetishId])
        row = cursor.fetchone()
        fetishGraph.setValue(row[1],fetishes["Totals"][row[0]])
        sys.stderr.write("[%s] => [%s]\n" % (row[1],fetishes["Totals"][fetishId]))
    reportData.Graphs.append(fetishGraph)
    sys.stderr.write("Done Fetishes\n")

    reportManager   =   ReportManager()
    reportManager.writeReport(reportData)
    reportManager.displayReport()
