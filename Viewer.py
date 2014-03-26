import os
import sys
import webbrowser
import lxml.html.builder as E
import lxml.etree
import ConfigParser
from datetime import date,timedelta
from Crawler import Crawler,Session
from Crawler import Profile

class ReportProfile(Profile):

    def load(self):
        sys.stderr.write("Loading Profile [%s]\n" % self.Id)
        fileName = os.path.join("Profiles","%s.ini" % self.Id)
        if not os.path.exists(fileName):
            return False
        config       =   ConfigParser.ConfigParser()
        config.optionxform=str
        config.read(fileName)

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
        return True

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

        chartArgs   =   []
        for k in sorted(data["Charts"].keys()):
            v = data["Charts"][k]
            chartArgs.append(self.__buildHorizontalChart(k,v))


        html    =   E.HTML(
                        E.HEAD( 
                            E.TITLE("Foom"),
                            E.LINK(rel="stylesheet", type="text/css", href="Chart.css"),
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
    session         =   Session()
    crawler         =   Crawler(session)
    maxProfile      =   sys.maxint
    reportData                      =   {}
    reportData["AllProfiles"]       =   []
    reportData["ActiveProfiles"]    =   []
    reportData["pids"]  =   crawler.getCompletedProfiles() 
    for pid in reportData["pids"]:
        profile =   ReportProfile(pid)
        if(profile.load()):
            #sys.stderr.write("%s\n" % str(profile))
            reportData["AllProfiles"].append(profile)
            now     =   date.today()
            last    =   profile.getLastActivity()
            delta   =   now-last
            if delta.days < 30:
                reportData["ActiveProfiles"].append(profile)

            #sys.stderr.write("time [%s]\n" % (now-last))
        if len(reportData["AllProfiles"]) == maxProfile:
            break

    reportData["Charts"]                        =   {}
    reportData["Charts"]["Age"]                 =   ReportGraph()
    reportData["Charts"]["Gender"]              =   ReportGraph()
    reportData["Charts"]["Type - All"]          =   ReportGraph()
    reportData["Charts"]["Type - Male"]         =   ReportGraph()
    reportData["Charts"]["Type - Female"]       =   ReportGraph()
    reportData["Charts"]["Type - Other"]        =   ReportGraph()
    reportData["Charts"]["LookingFor"]          =   ReportGraph()
    reportData["Charts"]["Relationship Types"]  =   ReportGraph()
    reportData["Charts"]["Relationship Count"]  =   ReportGraph()
    reportData["Charts"]["Orientation"]         =   ReportGraph()
    reportData["Charts"]["Active"]              =   ReportGraph()
    
    for profile in reportData["ActiveProfiles"]:
        reportData["Charts"]["Age"].incValue(int(profile.Age),1)   
        reportData["Charts"]["Gender"].incValue(profile.Gender,1)
        reportData["Charts"]["Type - All"].incValue(profile.Type,1)
        if profile.Gender in Profile.GENDER_GROUP_MALE:
            reportData["Charts"]["Type - Male"].incValue(profile.Type,1)
        elif profile.Gender in Profile.GENDER_GROUP_FEMALE:
            reportData["Charts"]["Type - Female"].incValue(profile.Type,1)
        else:
            reportData["Charts"]["Type - Other"].incValue(profile.Type,1) 
        for value in profile.LookingFor:
            reportData["Charts"]["LookingFor"].incValue(value,1)
        for value in profile.Relationships.values():
            reportData["Charts"]["Relationship Types"].incValue(value,1)
        reportData["Charts"]["Relationship Count"].incValue(len(profile.Relationships),1)
        reportData["Charts"]["Orientation"].incValue(profile.Orientation,1)
        reportData["Charts"]["Active"].incValue(profile.Active,1)
 

   
    reportManager   =   ReportManager()
    reportManager.writeReport(reportData)
    reportManager.displayReport()

