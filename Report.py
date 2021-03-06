import os
import sys
from Profile import Profile
import numpy
import pylab
from matplotlib import pyplot,colors,ticker
from matplotlib.backends.backend_pdf import PdfPages
 

def log(message):
    sys.stderr.write("%s\n" % message)
    sys.stderr.flush()

class MultiGraph(object):

    def __init__(self,title,rows=[],vertical=True,preserve_order=False,sort_by_value=False,legend=""):
        self.__title        =   title
        self.__legend       =   legend
        self.__data         =   {}
        self.__keys         =   []
        self.__vertical     =   vertical
        if preserve_order and sort_by_value:
            raise RuntimeError
        self.__sortByValue  =   sort_by_value
        if preserve_order:
            self.__order        =   []
        else:
            self.__order        =   None
        for row in rows:
            self.incValue(row[0],row[1],1)

    def setVertical(self,vertical):
        self.__vertical     =   vertical

    def getVertical(self):
        return self.__vertical

    def getLegend(self):
        return self.__legend

    def getTitle(self):
        return self.__title

    def __safeKey(self,key):
        return key
        if isinstance(key,int):
            return "%d" % key
        elif key is None:
            return "None"
        else:
            #log("key [%s] type [%s]" % (key,type(key)))
            return key.decode('ascii','replace')

    def incValue(self,cat,key,value):
        key = self.__safeKey(key)
        if cat not in self.__data:
            self.__data[cat]      =   {}
            if self.__order is not None:
                self.__order =  [cat] + self.__order
        if key not in self.__data[cat]:
            self.__data[cat][key] =   0
            if key not in self.__keys:
                self.__keys =   [key] + self.__keys
        self.__data[cat][key]     +=  value

    def setValue(self,cat,key,value):
        key = self.__safeKey(key)
        if cat not in self.__data:
            self.__data[cat]      =   {}
            if self.__order is not None:
                self.__order = [cat] + self.__order
        if key not in self.__data[cat]:
            self.__data[cat][key] =   0
            if key not in self.__keys:
                self.__keys =   [key] + self.__keys
        self.__data[cat][key] =  value

    def getValue(self,cat,key):
        key = self.__safeKey(key)
        return self.__data.get(cat,{}).get(key,0)

    def getCats(self):
        sys.stderr.write("Cats [%s] Order [%s]\n" % (self.__data.keys(),self.__order)) 
        if self.__order is not None:  
            return sorted(self.__order)
        elif self.__sortByValue:
            tmp =   {}
            for cat in self.__data.keys():
                tmp[cat] = 0
            for k,v in self.__data.iteritems():
                for vv in v.values():
                    tmp[k] += vv
            return sorted(tmp,key=tmp.get)
        else:
            return self.__data.keys()

    def getKeys(self):
        sys.stderr.write("Keys [%s] Order [%s]\n" % (self.__keys,self.__order)) 
        if self.__order is not None:
            return self.__keys
        elif self.__sortByValue:
            tmp =   {}
            for key in self.__keys:
                tmp[key] = 0
            for v in self.__data.values():
                for k,vv in v.iteritems():
                    tmp[k] += vv
            return sorted(tmp,key=tmp.get)
        else:
            return sorted(self.__keys)

class PercentHeatMap(object):

    def __init__(self,title,values_rows=None,totals_rows=None):
        self.__title = title
        self.__values   =   MultiGraph("Values",rows=values_rows)
        self.__totals   =   MultiGraph("Totals",rows=totals_rows)
        
    def getTitle(self):
        return self.__title

    def incValue(self,cat,key,value):
        self.__values.incValue(cat,key,value)

    def incTotal(self,cat,key,value):
        self.__totals.incValue(cat,key,value)

    def getCatRange(self):
        return range(min(min(self.__values.getCats()),min(self.__totals.getCats())),max(max(self.__values.getCats()),max(self.__totals.getCats())))

    def getKeyRange(self):
        return range(min(min(self.__values.getKeys()),min(self.__totals.getKeys())),max(max(self.__values.getKeys()),max(self.__totals.getKeys())))

    def getValue(self,cat,key):
        if self.__totals.getValue(cat,key) == 0:
            return 0
        else:
            return int(100.0 * self.__values.getValue(cat,key)/self.__totals.getValue(cat,key))

class SimpleGraph(object):

    def __init__(self,title,preserve_order=False,rows=[],default_colour=None,sort_by_value=False,highlight=None):
        self.__title            =   title
        self.__Data             =   {}
        if preserve_order and sort_by_value:
            raise RuntimeError
        self.__sortByValue      =   sort_by_value
        if preserve_order:
            self.__order        =   []
        else:
            self.__order        =   None
        self.__defaultColour    =   default_colour
        for row in rows:
            self.incValue(row[0],1)
        self.__highlight        =   highlight

    def getTitle(self):
        return self.__title

    def getDefaultColour(self):
        return self.__defaultColour

    def getHighlight(self,key):
        if self.__highlight is None:
            return "Grey"
        if isinstance(self.__highlight,basestring):
            return self.__highlight
        if key in self.__highlight:
            return "Yellow"
        else:
            return "White"

    def setValue(self,x,value):
        if x not in self.__Data and self.__order is not None:
            self.__order = [x] + self.__order
        self.__Data[x]      =   value

    def incValue(self,x,value):
        if x not in self.__Data:
            self.__Data[x]  =   0
            if self.__order is not None:
                self.__order = [x] + self.__order
        self.__Data[x]      +=  value

    def getValue(self,x):
        return self.__Data.get(x,0)

    def hasValue(self,x):
        return x in self.__Data.keys()

    def getKeys(self):
        if self.__order:
            return self.__order
        elif self.__sortByValue:
            return sorted(self.__Data,key=self.__Data.get)
        else:
            return sorted(self.__Data.keys())

class ReportData(object):

    def __init__(self):
        self.Graphs         =   []
        self.AllProfiles    =   []
        self.ActiveProfiles =   []

class ReportManager(object):

    COLOURS =   {
                    Profile.GENDER_MALE_TITLE   :   "Red",
                    Profile.GENDER_FEMALE_TITLE :   "Blue",
                    Profile.GENDER_OTHER_TITLE  :   "Green"
                }

    def __init__(self):
        self.__reportPath      =    os.path.join(os.path.dirname(sys.modules[__name__].__file__), "Report")

    def __getColour(self,label,default=None):
        if label in ReportManager.COLOURS:
            return ReportManager.COLOURS[label]
        elif label in Profile.GENDER_GROUP_MALE:
            return ReportManager.COLOURS["Male"]
        elif label in Profile.GENDER_GROUP_FEMALE:
            return ReportManager.COLOURS["Female"]
        elif default is not None:
            return default
        else:
            return "Black"


    def writeReport(self,data):
        log("Starting Doc Creation")
        fileName    =   os.path.join(self.__reportPath,"fetlife_report.pdf")
        pdf = PdfPages(fileName)
        pyplot.rc('xtick', labelsize=3) 
        pyplot.rc('ytick', labelsize=5)
        for graph in data.Graphs:
            if isinstance(graph,SimpleGraph):
                #keys        =   [x.encode('utf8',"ignore") for x in graph.getKeys()]
                keys        =   graph.getKeys()
                uniKeys     =   self.unicodeKeys(keys)
                ind         =   numpy.arange(len(keys))
                fig, ax     =   pyplot.subplots()
                values  =   [ graph.getValue(key) for key in keys ]
                colours =   [ self.__getColour(key,graph.getDefaultColour()) for key in keys]
                edgeColours =   []
                for key in keys:
                    edgeColours.append(graph.getHighlight(key))

                #rect    =   ax.barh(ind, values, color=colours,edgecolor = "none")
                rect    =   ax.barh(ind, values, color=colours,edgecolor = edgeColours)
                ax.set_title(graph.getTitle())
                ax.set_yticks(ind)
                ax.set_yticklabels(uniKeys)
                ax.set_ylim(0,len(keys))
                ax.tick_params('both', length=0, width=0, which='minor')
                ax.yaxis.set_major_formatter(ticker.NullFormatter())
                ax.yaxis.set_minor_locator(ticker.FixedLocator(0.5 + ind))
                ax.yaxis.set_minor_formatter(ticker.FixedFormatter(uniKeys))
            elif isinstance(graph,PercentHeatMap):
                pyplot.rc('xtick', labelsize=6) 
                pyplot.rc('xtick', labelsize=6) 
                sys.stderr.write("Starting Heat Map [%s]\n" % graph.getTitle())
                catRange    =   graph.getCatRange()
                keyRange    =   graph.getKeyRange()
                sys.stderr.write("catRange [%s]\n" % catRange)
                sys.stderr.write("keyRange [%s]\n" % keyRange)
                fig, ax =   pyplot.subplots()
                data    =   numpy.random.randn(len(catRange),len(keyRange))
                for c in catRange:
                    for k in keyRange:
                        i   =   c-catRange[0]
                        j   =   k-keyRange[0]
                        data[i][j]  =   graph.getValue(c,k)
                        if data[i][j] == 0:
                            data[i][j] = -1
                c   =   pylab.ma.masked_where(c<0,c)
                cdict = {
                        'red':  (       (0.0,   0.0,  1.0),
                                        (0.0,   1.0,  0.0),
                                        (0.5,   0.0,  0.0), 
                                        (1.0,   1.0,  1.0)),
                        'green':(       (0.0,   0.0,  1.0),
                                        (0.0,   1.0,  0.0),
                                        (1.0,   0.0,  0.0)),
                        'blue': (       (0.0,   0.0,  1.0),
                                        (0.0,   1.0,  1.0),
                                        (0.5,   0.0,  0.0), 
                                        (1.0,   0.0,  0.0))
                        }
                """
                cdict = {
                        'red':  (       (0.0,   0.0,  0.0),
                                        (0.5,   0.0,  0.0), 
                                        (1.0,   1.0,  1.0)),
                        'green':(       
                                        (0.0,   0.0,  0.0),
                                        (1.0,   0.0,  0.0)),
                        'blue': (       
                                        (0.0,   1.0,  0.0),
                                        (0.5,   0.0,  0.0), 
                                        (1.0,   0.0,  0.0))
                        }
                """
 

                colorMap    =   colors.LinearSegmentedColormap("custom",cdict)
                p = ax.pcolormesh(data,cmap=colorMap)
                evenKeys    =   []
                for idx in range(len(keyRange)):
                    if keyRange[idx]%2 == 0:
                        evenKeys.append(keyRange[idx])
                    else:
                        evenKeys.append("")
                ax.set_xticks(numpy.arange(len(evenKeys)))
                ax.set_xticklabels( evenKeys )
                ax.set_yticks(numpy.arange(len(catRange)))
                ind         =   numpy.arange(len(catRange))
                ax.tick_params('both', length=0, width=0, which='minor')
                ax.yaxis.set_major_formatter(ticker.NullFormatter())
                ax.yaxis.set_minor_locator(ticker.FixedLocator(0.5+ind))
                ax.yaxis.set_minor_formatter(ticker.FixedFormatter(catRange))
 
                ax.set_xlim(0,len(keyRange))
                colorBar    =   fig.colorbar(p,values=numpy.arange(101),boundaries=numpy.arange(101),ticks=[0,25,50,75,100],orientation='horizontal')
                colorBar.ax.set_xticklabels(['0%','25%' ,'50%','75%' ,'100%'])
                sys.stderr.write("boundries - %s\n" % str(colorBar._boundaries))
                pyplot.title(graph.getTitle())
            elif isinstance(graph,MultiGraph) and graph.getVertical():
                #keys        =   [("%s" % (x)).encode('utf8','ignore') for x in graph.getKeys()]
                keys        =   graph.getKeys()
                uniKeys     =   self.unicodeKeys(keys)
                cats        =   graph.getCats()
                ind         =   numpy.arange(len(keys))*len(cats)
                fig, ax     =   pyplot.subplots()
                for idx in range(len(graph.getCats())):
                    cat     =   cats[idx]
                    colour  =   self.__getColour(cat)
                    values  =   [ graph.getValue(cat,key) for key in keys ]
                    rect    =   ax.bar(ind+idx,values,color=colour,edgecolor = "none",label=cat)
                ax.set_title(graph.getTitle())
                ax.set_xticks(ind)
                ax.set_xticklabels(uniKeys)
                ax.set_xlim(0,len(keys)*len(cats))
                ax.tick_params('both', length=0, width=0, which='minor')
                ax.xaxis.set_major_formatter(ticker.NullFormatter())
                ax.xaxis.set_minor_locator(ticker.FixedLocator(len(cats)/2.0 + ind))
                ax.xaxis.set_minor_formatter(ticker.FixedFormatter(uniKeys))
                ax.legend(title=graph.getLegend(),loc="upper right")
            elif isinstance(graph,MultiGraph) and not graph.getVertical():
                #keys        =   [("%s" % (x)).encode('utf8','ignore') for x in graph.getKeys()]
                keys        =   graph.getKeys()
                #uniKeys     =   [unicode(x.decode("utf-8")) for x in keys]
                uniKeys     =   self.unicodeKeys(keys)
                cats        =   graph.getCats()
                ind         =   numpy.arange(len(keys))*(len(cats))
                fig, ax     =   pyplot.subplots()
                for idx in range(len(graph.getCats())):
                    cat     =   cats[idx]
                    colour  =   self.__getColour(cat)
                    values  =   [ graph.getValue(cat,key) for key in keys ]
                    rect    =   ax.barh(ind+idx, values, color=colour,edgecolor = "none",label=cat)
                ax.set_title(graph.getTitle())
                ax.set_yticks(ind)
                ax.set_yticklabels( uniKeys )
                ax.set_ylim(0,len(keys)*len(cats))
                ax.tick_params('both', length=0, width=0, which='minor')
                ax.yaxis.set_major_formatter(ticker.NullFormatter())
                ax.yaxis.set_minor_locator(ticker.FixedLocator(len(cats)/2.0 + ind))
                ax.yaxis.set_minor_formatter(ticker.FixedFormatter(uniKeys))
                ax.legend(title=graph.getLegend(),loc="lower right")
 
            pdf.savefig(fig)
            pyplot.close()
        pdf.close()
        
        log("Done Doc Creation")

    def unicodeKeys(self,keys):
        rv  =   []
        for key in keys:
            if key is None:
                rv.append(unicode("None"))
            elif isinstance(key,int):
                rv.append(unicode("%s" % key))
            else:
                rv.append(unicode(key.decode("utf-8")))
        return rv

    def displayReport(self):
        """
        import webbrowser
        fileName    =   os.path.join(self.__reportPath,"fetlife_report.pdf")
        controller = webbrowser.get()
        controller.open_new("file:" + os.path.abspath(fileName))
        """
