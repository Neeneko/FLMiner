import os
import sys
from Profile import Profile


def log(message):
    sys.stderr.write("%s\n" % message)
    sys.stderr.flush()

class MultiGraph(object):

    def __init__(self,title):
        self.__title        =   title
        self.__data         =   {}
        self.__keys         =   set()
        self.__vertical     =   True

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
        return self.__data[cat].get(key,0)

    def getCats(self):
        return self.__data.keys()

    def getKeys(self):
        return self.__keys

class SimpleGraph(object):

    def __init__(self,title):
        self.__title            =   title
        self.__Data             =   {}

    def getTitle(self):
        return self.__title

    def setValue(self,x,value):
        self.__Data[x]      =   value

    def incValue(self,x,value):
        if x not in self.__Data:
            self.__Data[x]  =   0
        self.__Data[x]      +=  value

    def getValue(self,x):
        return self.__Data.get(x,0)

    def hasValue(self,x):
        return x in self.__Data.keys()

    def getKeys(self):
        return self.__Data.keys()

class ReportData(object):

    def __init__(self):
        self.Graphs     =   []
        self.HeatMaps       =   []
        self.AllProfiles    =   []
        self.ActiveProfiles =   []

class ReportManager(object):

    COLOURS =   {
                    "Male"      :   "Red",
                    "Female"    :   "Blue",
                    "Other"     :   "Green"
                }

    def __init__(self):
        self.__reportPath      =    os.path.join(os.path.dirname(sys.modules[__name__].__file__), "Report")

    def __getColour(self,label):
        if label in ReportManager.COLOURS:
            return ReportManager.COLOURS[label]
        elif label in Profile.GENDER_GROUP_MALE:
            return ReportManager.COLOURS["Male"]
        elif label in Profile.GENDER_GROUP_FEMALE:
            return ReportManager.COLOURS["Female"]
        else:
            return "Black"


    def writeReport(self,data):
        log("Initing matplotlib")
        import numpy
        from matplotlib import pyplot
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
                keys    =   graph.getKeys()
                y_pos = numpy.arange(len(keys))
                values  =   [ graph.getValue(x) for x in keys ]

                pyplot.rc('text', usetex=False)
                #fig = pyplot.figure(figsize=(4, 5))
                fig = pyplot.figure()
                colours =   [ self.__getColour(x) for x in keys ]

                pyplot.barh(y_pos, values, align='center', alpha=0.4, color=colours)
                pyplot.yticks(y_pos, keys)
                pyplot.title(graph.getTitle())
            elif isinstance(graph,MultiGraph) and graph.getVertical():
                keys    =   sorted(graph.getKeys())
                cats    =   sorted(graph.getCats())
                ind = numpy.arange(len(keys))*len(cats)
                fig, ax = pyplot.subplots()
                for idx in range(len(graph.getCats())):
                    cat     =   cats[idx]
                    colour  =   self.__getColour(cat)
                    values  =   [ graph.getValue(cat,key) for key in keys ]
                    rect    =   ax.bar(ind+idx, values, color=colour,edgecolor = "none")
                ax.set_title(graph.getTitle())
                ax.set_xticks(ind)
                ax.set_xticklabels( keys )
            elif isinstance(graph,MultiGraph) and not graph.getVertical():
                keys    =   sorted(graph.getKeys())
                cats    =   sorted(graph.getCats())
                ind = numpy.arange(len(keys))*len(cats)
                fig, ax = pyplot.subplots()
                for idx in range(len(graph.getCats())):
                    cat     =   cats[idx]
                    colour  =   self.__getColour(cat)
                    values  =   [ graph.getValue(cat,key) for key in keys ]
                    rect    =   ax.barh(ind+idx, values, color=colour,edgecolor = "none")
                ax.set_title(graph.getTitle())
                ax.set_yticks(ind)
                ax.set_yticklabels( keys )
            pdf.savefig(fig)  # or you can pass a Figure object to pdf.savefig
            pyplot.close()
        pdf.close()
        
        log("Done Doc Creation")

    def displayReport(self):
        import webbrowser
        fileName    =   os.path.join(self.__reportPath,"fetlife_report.pdf")
        controller = webbrowser.get()
        controller.open_new("file:" + os.path.abspath(fileName))


        pass


