# DISCARD features that are null in more than P% samples.
# Marco Chierici <chierici@fbk.eu>
from __future__ import division
import numpy as np
import csv
import sys
import argparse

parser = argparse.ArgumentParser(description='Feature filter.')
parser.add_argument('-i', '--input', dest='DATAFILE', type=str, help='Input data matrix')
parser.add_argument('-o', '--output', dest='OUTFILE', type=str, help='Output file')
parser.add_argument('-p', '--perc', dest='PERC', type=np.float, help='Min. percentage of samples that must have counts >= C')

if len(sys.argv) < 3:
    parser.print_help()
    sys.exit(1)

args = parser.parse_args()
dataFile = args.DATAFILE
outFile = args.OUTFILE
# min. PERCENTAGE of samples that have zero values for each feature
p = args.PERC # e.g., 25
minCount = 0.0

if (p<0) or (p>100):
	raise ValueError("p must be in [0, 100]")

inputFile = open(dataFile, 'r')

header = ''
while not header.startswith('#OTU ID'):
    header = inputFile.readline()

data_otu = np.loadtxt(inputFile, dtype = str)
# transpose OTU table
data = np.transpose(data_otu)
# exclude last row with taxonomy
x = data[1:-1,1:].astype(np.float)
# OTU ID
feat = data[0, 1:]
# samples
samp = data[1:, 0]
# taxonomy 
taxa = data[data.shape[0]-1, 1:]

cutoff = np.int( np.round(0.01*p*x.shape[0]) )
print "Sample cutoff: %s" % cutoff
# number of samples with counts == minCount per feature
ns = np.sum( x == minCount, axis=0)

print "Original feature size: %s" % x.shape[1]

# keep only features with #samples < cutoff
x = x[:, ns < (x.shape[0] - cutoff) ]
feat = feat[ ns < (x.shape[0] - cutoff) ]
taxa = taxa[ ns < (x.shape[0] - cutoff) ]

print "New feature size: %s" % x.shape[1]

outw = open(outFile, 'w')
outw.write(header)
writer = csv.writer(outw, delimiter = '\t', lineterminator = '\n')

# write the header
#writer.writerow( [data[0,0]] + feat.tolist() )
#for row in range(0, x.shape[0]):
#	writer.writerow( [ samp[row] ] + x[row,:].tolist() )

# write taxonomy 
#writer.writerow( [data[data.shape[0],0]] + taxa.tolist() )

# transpose data
x_T = np.transpose(x)

writer.writerow( [data[0,0]] + samp.tolist() )
for row in range(0, x_T.shape[0]):
	writer.writerow( [ feat[row] ] + x_T[row,:].tolist() + [ taxa[row] ] )

outw.close()
