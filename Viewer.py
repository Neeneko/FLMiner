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
from Blobber import GetBlob
from Report import ReportManager,ReportData,MultiGraph,SimpleGraph

if __name__ == "__main__":
    usage       =   "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-b', '--blob', help="user the blob rather then loading profiles", action="store_true",default=False)

    options, args = parser.parse_args()

    if options.blob:
        sys.stderr.write("Loading Blob\n")
        profileMap  =   GetBlob()
    else:
        sys.stderr.write("Loading Profiles\n")
        maxProfile      =   sys.maxint
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
            if count == maxProfile:
                break
        sys.stderr.write("Loaded [%d] Profiles\n" % len(profileMap))

    sys.stderr.write("Filling Profile Lists\n")
    """
    reportData                      =   {}
    reportData["AllProfiles"]       =   []
    reportData["ActiveProfiles"]    =   []
    for profile in profileMap.itervalues():
        reportData["AllProfiles"].append(profile)
        crawl   =   profile.getCrawlDate()
        last    =   profile.getLastActivity()
        delta   =   crawl-last
        if delta.days < 30:
            reportData["ActiveProfiles"].append(profile)

    sys.stderr.write("Filling Charts\n")
    reportData["Charts"]                        =   {}
    reportData["Charts"]["Gender"]              =   ReportGraph()
    multiChartNames                             =   [
                                                        "Type",
                                                        "Age",
                                                        "LookingFor",
                                                        "Relationship Types",
                                                        "Count - Relationships",
                                                        #"Count - Friends",
                                                        #"Count - Groups",
                                                        "Orientation",
                                                        "Active"
                                                    ]
    genderNames                                 =   ["Female","Male","Other"]

    tempCharts                                  =   {}
    for name in multiChartNames:
        tempCharts[name]                        =   {}
        for genderName in genderNames:
            tempCharts[name][genderName]        =   ReportGraph()
 
    for profile in reportData["ActiveProfiles"]:
        reportData["Charts"]["Gender"].incValue(profile.Gender,1)
        if profile.Gender in Profile.GENDER_GROUP_MALE:
            gender  =   "Male"
        elif profile.Gender in Profile.GENDER_GROUP_FEMALE:
            gender  =   "Female"
        elif profile.Gender == "No Answer":
            gender  =   None
        else:
            gender  =   "Other"

        if gender != None:

            for value in profile.LookingFor:
                tempCharts["LookingFor"][gender].incValue(value,1)

            for value in profile.Relationships.values():
                tempCharts["Relationship Types"][gender].incValue(value,1)

            tempCharts["Age"][gender].incValue(int(profile.Age),1)   
            tempCharts["Type"][gender].incValue(profile.Type,1)
            tempCharts["Count - Relationships"][gender].incValue(len(profile.Relationships),1)
            #tempCharts["Count - Friends"][gender].incValue(len(profile.Friends),1)
            #tempCharts["Count - Groups"][gender].incValue(len(profile.Groups),1)
            tempCharts["Orientation"][gender].incValue(profile.Orientation,1)
            tempCharts["Active"][gender].incValue(profile.Active,1)

    reportData["MultiCharts"]                   =   {}
    for name in multiChartNames:
        reportData["MultiCharts"][name]         =   ReportMultiGraph()
        for gender in genderNames:
            reportData["MultiCharts"][name].addGraph(gender,tempCharts[name][gender])

    #degree stuff
    reportData["Charts"]["Degrees"]             =   ReportGraph()
    tempCharts                                  =   {"Total" : ReportGraph()}
    tempCharts["Total"].setValue("Total",0)
    tempTotals                                  =   {"Total" : {}}
    tempTotals["Total"]["Total"]                =   0
    for profile in reportData["AllProfiles"]:
        reportData["Charts"]["Degrees"].incValue(profile.Degree,1)
        try:
            age =   int(profile.Age)
        except ValueError:
            sys.stderr.write("Bad Age [%s]\n" % profile.Age)
            continue

        if profile.Degree == None or profile.Degree == 0:
            continue
        if profile.Degree not in tempCharts.keys():
            tempCharts[profile.Degree]          =   ReportGraph()
            tempCharts[profile.Degree].setValue("Total",0)
            tempTotals[profile.Degree]          =   {}
            tempTotals[profile.Degree]["Total"] =   0
        if profile.Gender in Profile.GENDER_GROUP_FEMALE:
            tempCharts[profile.Degree].incValue(age,1)
            tempCharts[profile.Degree].incValue("Total",1)
            tempCharts["Total"].incValue(age,1)
            tempCharts["Total"].incValue("Total",1)
        if age not in tempTotals[profile.Degree]:
            tempTotals[profile.Degree][age] = 0
        if age not in tempTotals["Total"]:
            tempTotals["Total"][age] = 0
 
        tempTotals[profile.Degree][age]     +=  1
        tempTotals[profile.Degree]["Total"] +=  1
        tempTotals["Total"][age]            +=  1
        tempTotals["Total"]["Total"]        +=  1



    reportData["HeatMaps"]                      =   {}
    reportData["HeatMaps"]["Women - Degree/Age"]=   ReportMultiGraph()
    for degree in sorted(tempCharts.keys()):
        for age in tempCharts[degree].getKeys():
            #sys.stderr.write("Degree [%d] Age [%d] OV [%d] A [%d] NV [%s]\n" % (degree,age,tempCharts[degree].getValue(age),tempTotals[degree][age],100*tempCharts[degree].getValue(age)/tempTotals[degree][age]))
            tempCharts[degree].setValue(age,100*tempCharts[degree].getValue(age)/tempTotals[degree][age])

    reportData["HeatMaps"]["Women - Degree/Age"].addGraph("Total",tempCharts["Total"])
    for degree in sorted([key for key in tempCharts.keys() if key != "Total"]):
        reportData["HeatMaps"]["Women - Degree/Age"].addGraph(degree,tempCharts[degree])
    """

    reportData      =   ReportData()
    for profile in profileMap.itervalues():
        reportData.AllProfiles.append(profile)
        crawl   =   profile.getCrawlDate()
        last    =   profile.getLastActivity()
        delta   =   crawl-last
        if delta.days < 30:
            reportData.ActiveProfiles.append(profile)

    reportData.Graphs.append(SimpleGraph("Profiles"))
    reportData.Graphs[-1].setValue("All",len(reportData.AllProfiles))
    reportData.Graphs[-1].setValue("Active",len(reportData.ActiveProfiles))

    multiChartNames                             =   [
                                                        #"Type",
                                                        "Age",
                                                        "LookingFor",
                                                        "Relationship Types",
                                                        "Count - Relationships",
                                                        #"Count - Friends",
                                                        #"Count - Groups",
                                                        "Orientation",
                                                        "Active"
                                                    ]
 

    genderGraph             =   SimpleGraph("Gender")
    genderGraphs            =   {}
    genderGraphs["Age"]     =   MultiGraph("Age By Gender")  
    genderGraphs["Ori"]     =   MultiGraph("Orientation By Gender")  
    genderGraphs["Ori"].setVertical(False)
    genderGraphs["Ident"]   =   MultiGraph("Identity By Gender")  
    genderGraphs["Ident"].setVertical(False)

    for profile in reportData.ActiveProfiles:
        genderGraph.incValue(profile.Gender,1)
        if profile.Gender in Profile.GENDER_GROUP_MALE:
            gender  =   "Male"
        elif profile.Gender in Profile.GENDER_GROUP_FEMALE:
            gender  =   "Female"
        elif profile.Gender == "No Answer":
            gender  =   None
        else:
            gender  =   "Other"

        if gender != None:
            genderGraphs["Age"].incValue(gender,profile.Age,1)
            genderGraphs["Ori"].incValue(gender,profile.Orientation,1)
            genderGraphs["Ident"].incValue(gender,profile.Type,1)


    reportData.Graphs.append(genderGraph)
    for v in genderGraphs.values():
        reportData.Graphs.append(v)
    reportManager   =   ReportManager()
    reportManager.writeReport(reportData)
    reportManager.displayReport()
