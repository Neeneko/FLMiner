import os
import sys
from Profile import Profile


def log(message):
    sys.stderr.write("%s\n" % message)
    sys.stderr.flush()

class MultiGraph(object):

    def __init__(self,title,rows=[],vertical=True):
        self.__title        =   title
        self.__data         =   {}
        self.__keys         =   set()
        self.__vertical     =   vertical
        for row in rows:
            self.incValue(row[0],row[1],1)

    def setVertical(self,vertical):
        self.__vertical     =   vertical

    def getVertical(self):
        return self.__vertical

    def getTitle(self):
        return self.__title

    def incValue(self,z,key,value):
        if z not in self.__data:
            self.__data[z]      =   {}
        if key not in self.__data[z]:
            self.__data[z][key] =   0
            if key not in self.__keys:
                self.__keys.add(key)
        self.__data[z][key]     +=  value

    def getValue(self,cat,key):
        return self.__data.get(cat,{}).get(key,0)

    def getCats(self):
        return self.__data.keys()

    def getKeys(self):
        return self.__keys

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

    def __init__(self,title,preserve_order=False,rows=[],default_colour=None):
        self.__title            =   title
        self.__Data             =   {}
        if preserve_order:
            self.__order        =   []
        else:
            self.__order        =   None
        self.__defaultColour    =   default_colour
        for row in rows:
            self.incValue(row[0],1)

    def getTitle(self):
        return self.__title

    def getDefaultColour(self):
        return self.__defaultColour

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
        log("Initing matplotlib")
        import numpy
        import pylab
        from matplotlib import pyplot,colors,ticker
        from matplotlib.backends.backend_pdf import PdfPages
        log("Done Init")


        log("Starting Doc Creation")
        #data = numpy.random.randn(7, 1024)
        fileName    =   os.path.join(self.__reportPath,"fetlife_report.pdf")
        pdf = PdfPages(fileName)
        """
        # Generate the pages
        nb_plots = data.shape[0]
        nb_plots_per_page = 5
        nb_pages = int(numpy.ceil(nb_plots / float(nb_plots_per_page)))
        grid_size = (nb_plots_per_page, 1)
 
        for i, samples in enumerate(data):
          # Create a figure instance (ie. a new page) if needed
          if i % nb_plots_per_page == 0:
              fig = plot.figure(figsize=(8.27, 11.69), dpi=100)
 
          # Plot stuffs !
          plot.subplot2grid(grid_size, (i % nb_plots_per_page, 0))
          plot.hist(samples, 32, normed=1, facecolor='#808080', alpha=0.75)
 
          # Close the page if needed
          if (i + 1) % nb_plots_per_page == 0 or (i + 1) == nb_plots:
            plot.tight_layout()
            pdf.savefig(fig)
 
        # Write the PDF document to the disk
        pdf.close()

        """
        
        pdf = PdfPages(fileName)
 
        pyplot.rc('xtick', labelsize=3) 
        pyplot.rc('ytick', labelsize=5)
        for graph in data.Graphs:
            if isinstance(graph,SimpleGraph):
                keys        =   graph.getKeys()
                ind         =   numpy.arange(len(keys))
                fig, ax     =   pyplot.subplots()
                values  =   [ graph.getValue(key) for key in keys ]
                colours =   [ self.__getColour(key,graph.getDefaultColour()) for key in keys]
                rect    =   ax.barh(ind, values, color=colours,edgecolor = "none")
                ax.set_title(graph.getTitle())
                ax.set_yticks(ind)
                ax.set_yticklabels( keys )
                ax.set_ylim(0,len(keys))
                ax.tick_params('both', length=0, width=0, which='minor')
                ax.yaxis.set_major_formatter(ticker.NullFormatter())
                ax.yaxis.set_minor_locator(ticker.FixedLocator(0.5 + ind))
                ax.yaxis.set_minor_formatter(ticker.FixedFormatter(keys))
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
                keys        =   sorted(graph.getKeys())
                cats        =   sorted(graph.getCats())
                ind         =   numpy.arange(len(keys))*len(cats)
                fig, ax     =   pyplot.subplots()
                for idx in range(len(graph.getCats())):
                    cat     =   cats[idx]
                    colour  =   self.__getColour(cat)
                    values  =   [ graph.getValue(cat,key) for key in keys ]
                    rect    =   ax.bar(ind+idx,values,color=colour,edgecolor = "none")
                ax.set_title(graph.getTitle())
                ax.set_xticks(ind)
                ax.set_xticklabels( keys )
                ax.set_xlim(0,len(keys)*len(cats))
                ax.tick_params('both', length=0, width=0, which='minor')
                ax.xaxis.set_major_formatter(ticker.NullFormatter())
                ax.xaxis.set_minor_locator(ticker.FixedLocator(len(cats)/2.0 + ind))
                ax.xaxis.set_minor_formatter(ticker.FixedFormatter(keys))
            elif isinstance(graph,MultiGraph) and not graph.getVertical():
                keys        =   sorted(graph.getKeys())
                cats        =   sorted(graph.getCats())
                ind         =   numpy.arange(len(keys))*(len(cats))
                fig, ax     =   pyplot.subplots()
                for idx in range(len(graph.getCats())):
                    cat     =   cats[idx]
                    colour  =   self.__getColour(cat)
                    values  =   [ graph.getValue(cat,key) for key in keys ]
                    rect    =   ax.barh(ind+idx, values, color=colour,edgecolor = "none")
                ax.set_title(graph.getTitle())
                ax.set_yticks(ind)
                ax.set_yticklabels( keys )
                ax.set_ylim(0,len(keys)*len(cats))
                ax.tick_params('both', length=0, width=0, which='minor')
                ax.yaxis.set_major_formatter(ticker.NullFormatter())
                ax.yaxis.set_minor_locator(ticker.FixedLocator(len(cats)/2.0 + ind))
                ax.yaxis.set_minor_formatter(ticker.FixedFormatter(keys))
 
            pdf.savefig(fig)
            pyplot.close()
        pdf.close()
        
        log("Done Doc Creation")

    def displayReport(self):
        import webbrowser
        fileName    =   os.path.join(self.__reportPath,"fetlife_report.pdf")
        controller = webbrowser.get()
        controller.open_new("file:" + os.path.abspath(fileName))


        pass


