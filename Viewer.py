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
from Progress import Progress,FauxParser
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

    profileDb.InitIdentGroups(Profile.IDENTITY_GROUPS)
    sys.stderr.write("Completed [%d] Profiles\n" % len(profileDb.GetSection("CompletedProfiles")))
    sys.stderr.write("Missing   [%d] Profiles\n" % len(profileDb.GetSection("MissingProfiles")))
    reportData      =   ReportData()
    reportData.Graphs.append(SimpleGraph("Profiles",preserve_order=True))
    reportData.Graphs[-1].setValue("Missing",len(profileDb.GetSection("MissingProfiles")))
    reportData.Graphs[-1].setValue("Never Active",profileDb.GetProfiles("COUNT(*)",filterEqField=("LastActivity",0))[0][0])
    reportData.Graphs[-1].setValue("Active in  30 days",len(profileDb.GetProfilesActiveInDays(30)))
    reportData.Graphs[-1].setValue("Active in  60 days",len(profileDb.GetProfilesActiveInDays(60)))
    reportData.Graphs[-1].setValue("Active in  90 days",len(profileDb.GetProfilesActiveInDays(90)))
    reportData.Graphs[-1].setValue("Active in 180 days",len(profileDb.GetProfilesActiveInDays(180)))
    reportData.Graphs[-1].setValue("All",len(profileDb.GetAllProfileIds()))
    reportData.Graphs.append(SimpleGraph("Role Types",sort_by_value=True))
    for k,v in Profile.IDENTITY_GROUPS.iteritems():
        for vv in v:
            count    =   profileDb.GetProfileCount(filterEqField=("Type",vv))
            reportData.Graphs[-1].incValue(k,count)

    genderGraph             =   SimpleGraph("Gender",rows=profileDb.GetProfiles("Gender"),default_colour="Green",sort_by_value=True)
    genderGraphs            =   {}
    genderGraphs["Age"]     =   MultiGraph("Age By Gender",rows=profileDb.GetProfiles("GenderGroup","Age"),legend="Gender")
    genderGraphs["Ori"]     =   MultiGraph("Orientation By Gender",vertical=False,rows=profileDb.GetProfiles("GenderGroup","Orientation"),sort_by_value=True)
    genderGraphs["Ident"]   =   MultiGraph("Role By Gender",vertical=False,rows=profileDb.GetProfiles("GenderGroup","Type"),sort_by_value=True)
    genderGraphs["Active"]  =   MultiGraph("Actvity Type By Gender",vertical=False,rows=profileDb.GetProfiles("GenderGroup","Active"),sort_by_value=True)  
    genderGraphs["RType"]   =   MultiGraph("Relationship Type By Gender",vertical=False,rows=profileDb.GetProfiles("GenderGroup","Relationships.Enum"),sort_by_value=True)  
    genderGraphs["Look"]    =   MultiGraph("Looking For By Gender",vertical=False,rows=profileDb.GetProfiles("GenderGroup","LookingFor.Enum"),sort_by_value=True)  
    reportData.Graphs.append(genderGraph)
    for v in genderGraphs.values():
        reportData.Graphs.append(v)
    """
    sys.stderr.write("Building HeatMaps\n")
    for heatMapOrigin in profileDb.GetDegreeOrigins():
        profileName = profileDb.GetProfileName(heatMapOrigin)
        sys.stderr.write("\tStarting [%s][%s]\n" % (heatMapOrigin,profileName))
        values  =   profileDb.GetProfiles("Degrees.Degree","Age",filterEqField=("GenderGroup","Female","Degrees.DstId",heatMapOrigin),filterGtField=("Degrees.Degree",0))
        totals  =   profileDb.GetProfiles("Degrees.Degree","Age",filterEqField=("Degrees.DstId",heatMapOrigin),filterGtField=("Degrees.Degree",0))
        heatMap =   PercentHeatMap("Percent Women from %s" % profileName,values_rows=values,totals_rows=totals)
        sys.stderr.write("\tDone [%s][%s]\n" % (heatMapOrigin,profileName))
        reportData.Graphs.append(heatMap)
 
    sys.stderr.write("Done Heatmaps\n")
    sys.stderr.write("Examining Fetishes\n")
    cursor      =   profileDb.GetCursor()
    cursor.execute("SELECT Fetishes.Name,FetishId,COUNT(FetishId) FROM ProfileToFetish,Fetishes WHERE ProfileToFetish.FetishId == Fetishes.Id GROUP BY FetishId ORDER BY COUNT(FetishId) DESC LIMIT 32")
    reportData.Graphs.append(SimpleGraph("Top Fetishes",preserve_order=True,highlight="Yellow"))
    fetishIds   =   {}
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        sys.stderr.write("[%32s:%8s] => [%8s]\n" % row)
        reportData.Graphs[-1].setValue(row[0],row[2])
        fetishIds[row[1]]   =   row[0]

    reportData.Graphs.append(MultiGraph("Top Fetishes By Gender",sort_by_value=True,vertical=False))
    for fetishId,fetishName in fetishIds.iteritems():
        for genderGroup in ["Male","Female","Other"]:
            cursor.execute("SELECT COUNT(ProfileToFetish.FetishId) FROM Profiles,ProfileToFetish WHERE Profiles.Id == ProfileToFetish.ProfileId AND Profiles.GenderGroup=? AND ProfileToFetish.FetishId=?",(genderGroup,fetishId))
            reportData.Graphs[-1].setValue(genderGroup,fetishName,cursor.fetchone()[0])
 
    for key in Profile.IDENTITY_GROUPS.keys():
        sys.stderr.write("Identity Group [%8s]\n" % key)
        cursor.execute("SELECT Fetishes.Name,FetishId,COUNT(FetishId) FROM ProfileToFetish,Fetishes,Profiles,IdentGroups WHERE ProfileToFetish.FetishId == Fetishes.Id AND ProfileToFetish.ProfileId == Profiles.Id AND Profiles.Type == IdentGroups.IdentGroup AND IdentGroups.Ident==? GROUP BY FetishId ORDER BY COUNT(FetishId) DESC LIMIT 32",[key])
        reportData.Graphs.append(SimpleGraph("Top Fetishes for %s" % key,preserve_order=True,highlight=fetishIds.values()))
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            #sys.stderr.write("[%32s:%8s] => [%8s]\n" % row)
            reportData.Graphs[-1].setValue(row[0],row[2])
            fetishIds[row[1]]   =   row[0]

    sys.stderr.write("Done Fetishes\n")
    """
    reportManager   =   ReportManager()
    reportManager.writeReport(reportData)
    reportManager.displayReport()
