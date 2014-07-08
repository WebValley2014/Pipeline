import numpy as np
import matplotlib.pyplot as plt
import random
import os
import sys

def get_randColor():
    #RETURN A EXADECIMAL RANDOM COLOR ie #ff45e2
    r = lambda: random.randint(0, 255)
    return '#%02X%02X%02X' % (r(), r(), r())

def percentage_row(matrix, style='row'):
    mc = matrix.copy()
    if style != 'row':
        mc = mc.T
    #computing sum row by row
    return mc / mc.sum(axis=1).reshape(-1, 1)
    
class BacteriaGraph(object):
    """
    the srcTable need a format samples X bacteria.
    this class can be used for receive, as a output,
    some graphics which analyze the data:
    *percentagehistogramm:
        from the table it can create the percentage 
        of the presence of the bacteria for each sample
    *metricsplot:
        from the metrics it create a plot that shows 
        the precision of the results of the machine 
        learning. "Style" can be 1, for only the first 
        10 misurations, 2, for 100, 3, for 1000, 
        or 4, for all.
    *classbacteriahistogramm:
        from the class.txt it produce a histogramm which 
        represent the average of the percentage of 
        the bacteria for each class
    """
    def __init__(self, srcMetrics='sourcemetrics.txt'):
        self.srcMetrics = srcMetrics
        self._loadData()

    def _loadData(self):
        if os.path.lexists(self.srcMetrics) == True:
            self.metrics = np.loadtxt(self.srcMetrics, skiprows = 2)
        else:
            raise Exception("wrong source file insert")
   
    def printSinglePlot(self, **kwargs):
        """
        Read the data stored in self.metrics, using column 0 as
        x axis values. Select the columns specified by *valueCol*,
        *minCol*, *maxCol* as Y values and print a png chart.
        
        args:
        *vCol*
            (int)
            number of the column with the Y values to plot
        *minCol*
            (int)
            number of the column with the min Y values
        *maxCol*
            (int)
            number of the column with the max Y values
        
        -----------------------
        optional args:
        *xLim*
            (int)
            x axis maximum value (scale). If xLim=None then x represents all the repetitions. Defaults to 10.
        *color*
            (str)
            The color of the func line (Y values). It defaults to "red".
        *oudDir*
            (str)
            output dir. Defaults to `graphs'.
        *outFile*
            (str)
            output filename. Defaults to `testGraph.png'.
        """
        # manage args
        vCol = kwargs.get('vCol')
        minCol = kwargs.get('minCol')
        maxCol = kwargs.get('maxCol')
        xLim = kwargs.get('xLim', 10)
        color = kwargs.get('color', 'red')
        outDir = kwargs.get('outDir', 'graphs') 
        outFile = kwargs.get('outFile', 'testGraph.png')
        filename = kwargs.get('filename', 'testGraph')

        # managment of the 'xlim=None'
        if xLim == None:
            xLIm = self.metrics.shape[0]

        # init 3 empty arrays for the sets: value, min, max
        xArray = self.metrics[:xLim, 0]
        vArray = self.metrics[:xLim, vCol]
        minArray = self.metrics[:xLim, minCol]
        maxArray = self.metrics[:xLim, maxCol]

        # set file path
        if not os.path.exists(outDir):
            os.makedirs(outDir)
        filePath = os.path.join(outDir, outFile)

        # print the png image using matplotlib
        plt.ylim((0, 1))
        plt.xscale("log")
        plt.xlim((0.1, xLim + 1))
        plt.plot(xArray, vArray, color=color)
        plt.plot(xArray, minArray, color='#C0C0C0')
        plt.plot(xArray, maxArray, color='#C0C0C0')
        plt.title(filename + ' graphic')
        plt.xlabel('repetitions')
        plt.ylabel('prrecision')
        plt.savefig(filePath, format="png")
        plt.clf()

    def printAllPlots(self, outputPath):
        """
        Define a dict with the configuration of the plots I want to
        print on a graph, and call many times self.printSinglePlot
        to generate the png.
        """
        myConf = [
            {
                'vCol'       : 1,
                'minCol'     : 2,
                'maxCol'     : 3,
                'filename'   : "MCC",
                'outFile'    : "",
                'color'      : "blue",
                'xLim'       : None
            },
            {
                'vCol'       : 4,
                'minCol'     : 5,
                'maxCol'     : 6,
                'filename'   : "SENS",
                'outFile'    : "",
                'color'      : "red",
                'xLim'       : None
            },          
            {
                'vCol'       : 7,
                'minCol'     : 8,
                'maxCol'     : 9,
                'filename'   : "SPEC",
                'outFile'    : "",
                'color'      : "green",
                'xLim'       : None
            },
            {
                'vCol'       : 10,
                'minCol'     : 11,
                'maxCol'     : 12,
                'filename'   : "PPV",
                'outFile'    : "",
                'color'      : "yellow",
                'xLim'       : None
            },
            {
                'vCol'       : 13,
                'minCol'     : 14,
                'maxCol'     : 15,
                'filename'   : "NPV",
                'outFile'    : "",
                'color'      : "purple",
                'xLim'       : None
            },
            {
                'vCol'       : 16,
                'minCol'     : 17,
                'maxCol'     : 18,
                'filename'   : "AUC",
                'outFile'    : "",
                'color'      : "orange",
                'xLim'       : None
            },
            {
                'vCol'       : 19,
                'minCol'     : 20,
                'maxCol'     : 21,
                'filename'   : "ACC",
                'outFile'    : "",
                'color'      : "brown",
                'xLim'       : None
            }
        ]

        allLeng = self.metrics.T
        allLeng = allLeng[0]
        allLeng = allLeng[len(allLeng) -1]
        
        for conf in myConf:
            for xLim in [10, 100, 1000, allLeng]:
                conf['xLim'] = xLim
                filename = "{0}_{1}.png".format(conf['filename'], int(xLim))
                conf['outFile'] = filename
                conf['outDir'] = outputPath
                self.printSinglePlot(**conf)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print('Usage: %s [metrics] [out dir]' % sys.argv[0])
        sys.exit()

    graph = BacteriaGraph(sys.argv[1])
    graph.printAllPlots(sys.argv[2])
