import os
import sys
import webbrowser
import lxml.html.builder as E
import lxml.etree
import ConfigParser
import cPickle as Pickle
from datetime import date,timedelta,datetime
#from Crawler import Crawler,Session
from Crawler import Progress,FauxParser
from Crawler import Profile

class ReportProfile(Profile):

    def __iniLoad(self,file_name):
        #sys.stderr.write("Loading INI profile\n")

        config       =   ConfigParser.ConfigParser()
        config.optionxform=str
        config.read(file_name)

        #sys.stderr.write("Sections - [%s]\n" % config.sections())
        self.Name           =   config.get("Details","Name")
        self.Age            =   config.get("Details","Age")
        self.Gender         =   config.get("Details","Gender")
        self.Type           =   config.get("Details","Type")
        self.Orientation    =   config.get("Details","Orientation")
        self.Active         =   config.get("Details","Active")
        self.LastActivity   =   config.get("Details","LastActivity")

        def loadList(name):
            return config.options(name)
        
        def loadDict(name):
            rv = {}
            for k in config.options(name):
                rv[k] = config.get(name,k)
            return rv

        self.Location       =   loadList("Location")
        self.LookingFor     =   loadList("LookingFor")
        self.Groups         =   loadList("Groups")
        self.Friends        =   loadList("Friends")
        self.Relationships  =   loadDict("Relationships")
 
    def __datLoad(self,file_name):
        with open(file_name,"rb") as fp:
            for k,v in Pickle.load(fp).iteritems():
                setattr(self,k,v)

    def __datSave(self,file_name):
        with open(file_name,"wb") as fp:
            Pickle.dump(vars(self),fp)

    def load(self):
        #TODO - if ini, load ini and write dat.  if dat load dat
        #sys.stderr.write("Loading Profile [%s]\n" % self.Id)
        iniName = os.path.join("Profiles","%s.ini" % self.Id)
        datName = os.path.join("Profiles","%s.dat" % self.Id)
        if os.path.exists(iniName):
            self.__iniLoad(iniName)
            #sys.stderr.write("Starting Fix\n")
            #sys.stderr.write("Last Activity [%s]\n" % self.LastActivity)
            with open(iniName,"r") as fp:
                timeStamp = os.fstat(fp.fileno()).st_ctime
            self.setCrawlDate(timeStamp)
            #sys.stderr.write("Fix Last [%s] Crawl [%s]\n" % (self.getLastActivity(),self.getCrawlDate()))
            #sys.stderr.write("Done Fix\n")
            self.__datSave(datName)
            os.unlink(iniName)

            #Quick test
            newProfile = ReportProfile(self.Id)
            newProfile.load()
            #sys.stderr.write("New Last [%s] Crawl [%s]\n" % (newProfile.getLastActivity(),newProfile.getCrawlDate()))

        elif os.path.exists(datName):
            self.__datLoad(datName)
        else:
            return False
        
        return True

class ReportMultiGraph(object):

    def __init__(self):
        self.Graphs             =   []
        self.Labels             =   []

    def addGraph(self,label,graph):
        self.Labels.append(label)
        self.Graphs.append(graph)

class ReportGraph(object):

    def __init__(self,stacked=False):
        self.__Data             =   {}
        self.__Stacked          =   stacked

    def setValue(self,x,value):
        self.__Data[x]      =   value

    def incValue(self,x,value):
        if x not in self.__Data:
            self.__Data[x]  =   0
        self.__Data[x]      +=  value

    def getValue(self,x):
        return self.__Data.get(x,0)

    def getKeys(self):
        return self.__Data.keys()

    def isStacked(self):
        return self.__Stacked

    def printStuff(self):
        sys.stderr.write("%s\n" % str(self.__Data))


class ReportManager(object):

    def __init__(self):
        self.__reportPath      =    os.path.join(os.path.dirname(sys.modules[__name__].__file__), "Report")

    def __nextPowerOfTwo(self,value):
        rv = 16
        while True:
            if value < rv:
                return rv
            if rv >= 256:
                rv += 64
            elif rv >= 128:
                rv += 32
            else:
                rv += 16

    def __buildMultiChart(self,label,charts):
        sys.stderr.write("Building hchart for [%s]\n" % label)
        reportCharts    =   []

        yValues =   []
        xMax    =   2
        #for arg in args[1:]:
        #    reportCharts.append(arg)
        reportCharts    =   charts.Graphs

        sliceSize   =   10
        chunkSize   =   sliceSize*len(reportCharts)

        for z in range(len(reportCharts)):
            yValues = set(yValues) | set(reportCharts[z].getKeys())
            for key in reportCharts[z].getKeys():
                xValue  =   reportCharts[z].getValue(key)
                if reportCharts[z].isStacked():
                    for zz in reportCharts[z:]:
                        xValue += zz.getValue(key)
                xMax    =   max(xMax,xValue)
        xMax    =   self.__nextPowerOfTwo(xMax)

        if len(yValues) == 0:
            return E.DIV(E.H2(args[0]),E.SPAN("No Data"))

        yValues =   sorted(yValues,reverse=True)
        yMin    =   yValues[0]
        yMax    =   yValues[-1]

        chartArgs   =   [E.CLASS("hBarGraph")]
        chartKwargs =   {"style":"height: %spx" % ((chunkSize*len(yValues)))}

        idx = 0
        labelRange  =   yValues

        for y in labelRange:
            chartArgs.append(E.LI("  %s  " % y,E.CLASS("p0"),style="width: 100%%; color: #000; bottom: %spx;" % (chunkSize*idx)))
            for z in range(len(reportCharts)):
                value   =   reportCharts[z].getValue(y)
                if value == 0:
                    x       =   0
                    value   =   ""
                else:
                    x       =   80.0 * float(value)/float(xMax)
                chartArgs.append(E.LI("%s" % value,E.CLASS("p%d" % (z+1)),style="width: %s%%; bottom: %spx; height: %spx; line-height: %spx;" % (x,(chunkSize*idx)+(z*sliceSize),sliceSize,sliceSize)))
            idx += 1
        chart       =   E.UL(*chartArgs,**chartKwargs)

        legendArgs   =   [E.CLASS("hBarGraph")]
        legendKwargs =   {"style":"height: 64px"}
        for i in range(len(charts.Labels)):
            legendArgs.append(E.H3(charts.Labels[i],E.CLASS("p%d" % (i+1))))
        legend      =   E.DIV(*legendArgs,**legendKwargs)

        return E.DIV(E.H2(label),legend,chart)




    def __buildHorizontalChart(self,*args):
        assert isinstance(args[0],basestring)
        sys.stderr.write("Building hchart for [%s]\n" % args[0])
        reportCharts    =   []

        yValues =   []
        xMax    =   2
        for arg in args[1:]:
            reportCharts.append(arg)

        for z in range(len(reportCharts)):
            yValues = set(yValues) | set(reportCharts[z].getKeys())
            for key in reportCharts[z].getKeys():
                xValue  =   reportCharts[z].getValue(key)
                if reportCharts[z].isStacked():
                    for zz in reportCharts[z:]:
                        xValue += zz.getValue(key)
                xMax    =   max(xMax,xValue)
        xMax    =   self.__nextPowerOfTwo(xMax)

        if len(yValues) == 0:
            return E.DIV(E.H2(args[0]),E.SPAN("No Data"))

        yValues =   sorted(yValues,reverse=True)
        yMin    =   yValues[0]
        yMax    =   yValues[-1]

        chartArgs   =   [E.CLASS("hBarGraph")]
        chartKwargs =   {"style":"height: %spx" % ((30*len(yValues)))}

        idx = 0
        #if isinstance(yMin,int) and isinstance(yMax,int):
        #    labelRange  =   range(yMin,yMax+1)
        #else:
        labelRange  =   yValues

        for y in labelRange:
            chartArgs.append(E.LI("  %s  " % y,E.CLASS("p0"),style="width: 100%%; color: #000; bottom: %spx;" % (30*idx)))
            for z in range(len(reportCharts)):
                value   =   reportCharts[z].getValue(y)
                if value == 0:
                    x       =   0
                    value   =   ""

                elif reportCharts[z].isStacked():
                   
                    vSum  =   value
                    for zz in reportCharts[(z+1):]:
                        vSum += zz.getValue(y)
                    #sys.stderr.write("[%s] Index [%s] value [%s] vSum [%s]\n" % (y,z,value,vSum))
                    x       =   80.0 * float(vSum)/float(xMax)
                else:
                    x       =   80.0 * float(value)/float(xMax)
                chartArgs.append(E.LI("%s" % value,E.CLASS("p%d" % (z+1)),style="width: %s%%; bottom: %spx;" % (x,30*idx)))
            idx += 1
        chart       =   E.UL(*chartArgs,**chartKwargs)

        return E.DIV(E.H2(args[0]),chart)



    def __buildIndexPage(self,data):

        cssName =   os.path.join("Report","Chart.css")
        with open(cssName,"r") as fp:
            cssText = fp.read()

        chartArgs   =   []
        for k in sorted(data["Charts"].keys()):
            v = data["Charts"][k]
            chartArgs.append(self.__buildHorizontalChart(k,v))

        for k in sorted(data["MultiCharts"].keys()):
            v = data["MultiCharts"][k]
            chartArgs.append(self.__buildMultiChart(k,v))



        html    =   E.HTML(
                        E.HEAD( 
                            E.TITLE("Foom"),
                            E.STYLE(cssText),
                        ),
                        E.BODY(
                          E.P(
                                "Total Profiles : %s" % len(data["AllProfiles"]),
                                E.BR(),
                                "Active Profiles : %s" % len(data["ActiveProfiles"])
                            ),
                          E.P(*chartArgs)
                        )
                    )


        return lxml.etree.tostring(html,pretty_print=True)

    def writeReport(self,data):
        indexPage     =   self.__buildIndexPage(data)
        output = open(os.path.join(self.__reportPath,"index.html"),"w")
        output.write(indexPage)
        output.close()

    def displayReport(self):
        fileName    =   os.path.join(self.__reportPath,"index.html")
        controller = webbrowser.get()
        controller.open_new("file:" + os.path.abspath(fileName))
  

if __name__ == "__main__":
    progress        =   Progress()
    maxProfile      =   sys.maxint
    #maxProfile      =   4096
    reportData                      =   {}
    reportData["AllProfiles"]       =   []
    reportData["ActiveProfiles"]    =   []
    reportData["pids"]  =   list(progress.getIds("CompletedProfiles"))
    sys.stderr.write("Profiles to load: [%s]\n" % min(maxProfile,len(reportData["pids"])))
    for idx in range(len(reportData["pids"])):
        if idx%1024 == 0:
            sys.stderr.write("Progress - Loaded [%s]\n" % idx)
        profile =   ReportProfile(reportData["pids"][idx])
        if(profile.load()):
            #sys.stderr.write("%s\n" % str(profile))
            reportData["AllProfiles"].append(profile)
            #TODO - read file timestamp, not 'today'
            crawl   =   profile.getCrawlDate()
            last    =   profile.getLastActivity()
            delta   =   crawl-last
            if delta.days < 30:
                reportData["ActiveProfiles"].append(profile)

            #sys.stderr.write("time [%s]\n" % (now-last))
        if len(reportData["AllProfiles"]) == maxProfile:
            break

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
    reportManager   =   ReportManager()
    reportManager.writeReport(reportData)
    reportManager.displayReport()
