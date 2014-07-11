#######################################################
## this is the class Net.                            ##
## it has been proudly imagined and happily coded by ##
## Davide Leonessi & Stefano Valentini      (c) 2014 ##
#######################################################

import numpy as np
from scipy.stats import pearsonr
import distance_functions_2 as df ### him(G,H) with output (hamming, ipsen, him)!
import igraph
import random
import os

class Net:

    def __init__(self, dataname, labelsname, samplesname, featuresname, rankingname, metricsname, outputpath, thre): ### ('X.txt', 'Y.txt', 'sampleIDs.txt', 'names.txt', 'X_l2r_l2loss_svc_SVM_std_featurelist.txt', 'X_l2r_l2loss_svc_SVM_std_metrics.txt', 'outfolder', 0.95) (last one as a folder)
        self.dataname = dataname
        self.labelsname = labelsname
        self.samplesname = samplesname
        self.featuresname = featuresname
        self.rankingname = rankingname
        self.metricsname = metricsname
        self.outputpath = outputpath
        self.thre = thre

########

    def run(self):
        self.loadfiles()
        self.findsubmatrixes()
        self.mkadjmatrixes()
        self.networkList()
        return self.outDict

########

    def loadfiles(self):
        self.mdata = np.loadtxt(self.dataname)          ## data
        self.setlabels = np.loadtxt(self.labelsname)    ## labels


        filesamples = open(self.samplesname)            ## samples
        self.setsamples = filesamples.read()
        filesamples.close()
        self.setsamples = self.setsamples.split('\n')	#now, setsamples is a list of (the right) strings
        self.setsamples.pop(-1)	#fixes an error due to the split function

        filefeatures = open(self.featuresname)
        thesetfeatures = filefeatures.read()
        filefeatures.close()
        thesetfeatures = thesetfeatures.split('\n')
        thesetfeatures.pop(-1)

        self.setfeatures = []                           ## set of features (only numbers)
        self.legendfeatures = []                        ## legend of features (only names, in the same order)

        for i in range(len(thesetfeatures)):
            j = thesetfeatures[i]
            t = j.index('\t')
            self.setfeatures.append(thesetfeatures[i][:t])
            d = self.setfeatures[i].index('d')
            self.setfeatures[i] = self.setfeatures[i][d+1:]
            self.legendfeatures.append(thesetfeatures[i][t+1:])

        lsmpl, lsftr = self.mdata.shape
        if len(self.setsamples) != len(self.setlabels) or lsmpl != len(self.setlabels) or lsftr != len(self.setfeatures):
            print 'error, invalid input: data not coherent'

        fileranking = open(self.rankingname)
        self.setrank = fileranking.read()
        self.setrank = self.setrank.split('\n')
        self.setrank.pop(-1)
        self.setrank.pop(0)
        for i in range(len(self.setrank)):
            self.setrank[i].split('\t')
            self.setrank[i] = self.setrank[i][0]

        filemetrics = open(self.metricsname)
        self.nmetrics = filemetrics.readline()
        filemetrics.close()
        self.nmetrics = self.nmetrics.split('\t')
        self.nmetrics = self.nmetrics[1]
        self.nmetrics = self.nmetrics[:-1]
        self.nmetrics = int(self.nmetrics)

########

    def get_randColor(self):
        #RETURN A EXADECIMAL RANDOM COLOR ie #ff45e2
        r = lambda: random.randint(0, 255)
        return '#%02X%02X%02X' % (r(), r(), r())

########

    def findsubmatrixes(self):

        self.aunilabels = np.unique(self.setlabels)	### array of different labels which are in setlabels

        self.alabels = []	# list of 2d-matrixes (i.e. one sub-matrix for each label)

        ok = 0	# for the condition of the while loop
        while ok < len(self.aunilabels):
            r2 = 0
            maux = np.zeros((len(np.where(self.setlabels == self.aunilabels[ok])[0]), self.nmetrics))
            for r in np.where(self.setlabels == np.array(list(set(self.setlabels)))[ok])[0]:	#this is a very strange 2d-array with the positions of the ok-th different element of setlabels in setlabels itself
                #print aunilabels
                c2 = 0
                for i in range(self.nmetrics):
                    c = self.setrank[i]
                    maux[r2, c2] = self.mdata[r, c]
                    c2 += 1
                r2 += 1
            self.alabels.append(maux)
            ok += 1
        ### alabels is now the complete list of the sub-matrices of each label!

########

    def mkadjmatrixes(self):
        self.adjmatrixes = []
        for i in range(len(self.aunilabels)):	# sgrulla down le labels
            self.adjmatrixes.append(self.mknetfeatures(self.alabels[i]))	# uses features, not samples! there is the threshold: check it!
        self.adjmatrixes = np.array(self.adjmatrixes)
		        ### now, the list adjmatrixes is filled in with the adjacency matrices of each different label
        self.himadjmatrix = np.zeros((len(self.aunilabels), len(self.aunilabels)))

        for i in range(1, len(self.aunilabels)): #loop on label indexes

            for j in range(i):	#loop on previous labels
		
    	        hamming, ipsen, self.himadjmatrix[i, j] = df.him(self.adjmatrixes[i], self.adjmatrixes[j])	#calculates the him distance between two networks
                self.himadjmatrix[j, i] = self.himadjmatrix[i, j]	#makes symmetric the 'adjacency' matrix

########

    def mknetfeatures(self,M):	#M is our dear big matrix
                #attempt: to make it work on all columns, instead than on a given set
        nRow, nCol = M.shape #define dimensions

        ### let's create the network matrix
        self.mNet = np.zeros((nCol, nCol))

        # check of missing elements in the matrix
        if nRow == 0 or nCol == 0:
            print 'null input'
            return None

        for i in np.arange(nCol): #loop on feature indexes

            for k in np.arange(nCol): #loop on feature indexes, again

                if i < k: #not repeated nodes of features
                    L1 = [M[j,i] for j in range(nRow)]
                    L2 = [M[j,k] for j in range(nRow)]

                    if np.var(L1)==0 and np.var(L2)==0:
                        pear=[1.0,123]
                    elif np.var(L1)==0 or np.var(L2)==0:
                        pear=[0.,123]
                    else:
                        pear = pearsonr(L1,L2)    #output as array

                    pear = abs(pear[0])
                    if pear > self.thre:
                        self.mNet[i,k] = pear
                        self.mNet[k,i] = pear
        ### mNet is now our network matrix

        return self.mNet

########

    def drawNetwork(self, **kwargs):
        """
        Read the data stored in self.metrics, using column 0 as
        x axis values. Select the columns specified by *valueCol*,
        *minCol*, *maxCol* as Y values and print a png chart.

        args:
        *matrix*
        (numpy matrix)
        matrix of adjacency    
        -----------------------
        optional args:
        *nodeColor*
        (color)
        color of the nodes. Defaults to "red".
        *lineColor*
        (color)
        The color of the func line (Y values). It defaults to "grey - #787878".
        *oudDir*
        (str)
        output dir. Defaults to `networks'.
        *outFile*
        (str)
        output filename. Defaults to `testNetwork.png'.
	    """
	    # manage args
        matrix = kwargs.get('matrix')
        title = kwargs.get('title', 'test network')
        nodeColor = kwargs.get('nodeColor', 'red')
        lineColor = kwargs.get('lineColor', '#D8D8D8')
        outDir = kwargs.get('outDir', 'networks') 
        outFile = kwargs.get('outFile', 'testNetwork.png')
        if not os.path.exists(outDir):
            os.makedirs(outDir)
        filePath = os.path.join(outDir, outFile)
        g = igraph.Graph.Weighted_Adjacency(list(matrix),mode=igraph.ADJ_MAX)
        visual_style = {}
        visual_style["title"] = title
        visual_style["vertex_size"] = 20
        visual_style["vertex_color"] = nodeColor
        visual_style["vertex_label"] = self.setfeatures
        visual_style["edge_width"] = g.es["weight"]
        a = g.degree()
        for i in range(len(a)):
            a[i] = a[i]/10
        visual_style["vertex_size"] = a
        
        #visual_style["layout"] = layout_kamada_kawai
        visual_style["bbox"] = (5000, 5000)       #FIXME
        visual_style["margin"] = 20
        #plotting the network
        igraph.plot(g, filePath, **visual_style)

########

    def networkList(self):
        """
        set the data for create the network

        args:
        *srcListMatrix*
            (str) 
        """
        myConf = {}
        n = 0
        self.listtitles = []
        self.listgraphpaths = []
        for mtr in self.adjmatrixes:
            myConf['matrix'] = mtr
            myConf['nodeColor'] = self.get_randColor()
            myConf['lineColor'] = '#D8D8D8'
            saveDirectory = os.path.join(self.outputpath, 'img')
            if not os.path.exists(saveDirectory):
                os.makedirs(saveDirectory)
            myConf['outDir'] = saveDirectory
            myConf['outFile'] = 'graph_label_' + str(int(self.aunilabels[n])) + '.png'
            myConf['label'] = self.aunilabels[n]
            myConf['title'] = 'Graph of the label ' + str(self.aunilabels[n])
            self.listtitles.append('Graph of the label ' + str(self.aunilabels[n]))
            self.listgraphpaths.append(os.path.join(myConf['outDir'], myConf['outFile']))
            self.drawNetwork(**myConf)
            n += 1

        self.outDict = {}
        self.outDict['img'] = [f for f in self.listgraphpaths]
        self.outDict['titles'] = [t for t in self.listtitles]
        saveDirectory1 = os.path.join(self.outputpath, 'data')
        if not os.path.exists(saveDirectory1):
            os.makedirs(saveDirectory1)
        np.savetxt(os.path.join(saveDirectory1, 'himmatrix.txt'), self.himadjmatrix)
        self.outDict['matrix'] = os.path.join(self.outputpath, 'data/himmatrix.txt')

########

if __name__ == '__main__':
    n = Net('X.txt', 'Y.txt', 'sampleIDs.txt', 'names.txt', 'X_l2r_l2loss_svc_SVM_std_featurelist.txt', 'X_l2r_l2loss_svc_SVM_std_metrics.txt', 'outfolder', 0.95)
    n.run()
