# This code is written by Marco Chierici and Alessandro Zandona'.
# Based on code previously written by Davide Albanese.
# Requires Python >= 2.7, mlpy >= 3.5
# CHANGELOG
# 2014-03-12: [AZ] added title to metric plot
# 2014-02-24: [AZ] added data and labels file paths to log file
# 2014-02-10: [AZ] added options 'cv_k' and 'cv_n' for the number of CV folds and CV cycles respectively
# 2014-02-06: [MC] added possibility to parse command line parameters from a configuration file
# 2014-02-05: [MC] added option 'reliefk' for the number of neighbors to be used in combination with ReliefF
# 2014-02-05: [MC] moved option for random ranking from boolean flag to positional (RANK_METHOD = 'random')
# 2014-02-05: [MC] fixed behaviour for random ranking, which now excludes other ranking methods instead of replacing their rankings with a random permutation of indexes
# 2014-02-05: [MC] added creation of a log file with Python and packages versions
# 2014-01-09: [AZ] added input parameter to function svmlin_t in order to decide which scaling method (norm L2, standard, minmax) to apply
# 2013-11-21: [AZ] added input parameter to decide which scaling method (norm L2, standard, minmax) to apply
# 2013-11-21: [AZ] added function metplot in order to plot one metric over all training cycles
# 2013-11-20: [MC] added to the ranking file: median values (on all samples), per-class median values and fold-change (computed as median ratio)
# 2013-11-19: [AZ] renamed io.py in input_output.py (avoiding error while importing matplotlib)
# 2013-06-20: [CZ] change random label, to single run
# 2013-06-20: [CZ] added random ranking
# 2013-04-04: [MC] added: data normalization in place of minmax_scaling
# 2013-03-25: [MC] added: flag for feature ranking method (SVM, RFE)
# 2013-03-22: [MC] added: flag for random labels
# 2013-03-22: [MC] added: save RANKING to file
# 2013-03-22: [MC] added: stability computation
# 2013-03-21: [MC] moved SVM tuning OUTSIDE the CV loop
# 2013-02-27: [MC] added computation of approximated Odds Ratio from average SENS and SPEC
# 2013-02-27: [MC] rearranged metrics order so to have MCC first, then SENS, SPEC, PPV, NPV, AUC, ACC, OR, OR_APPROX
# 2013-02-27: [MC] added code comments
# 2013-02-26: [MC] added output of all metrics for each CV step
from __future__ import division
import numpy as np
import itertools
import csv
from svmlin_tuning import svmlin_t
import os.path
from scaling import *
import mlpy
from input_output import load_data
import performance as perf
import sys
import argparse

class myArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super(myArgumentParser, self).__init__(*args, **kwargs)

    def convert_arg_line_to_args(self, line):
        for arg in line.split():
            if not arg.strip():
                continue
            if arg[0] == '#':
                break
            yield arg

def compute_weights(y):
    classes = np.unique(y)
    weight = {}
    for c in classes:
        weight[c] = y.shape[0] / np.sum(y==c)
    return weight

parser = myArgumentParser(description='Run a training experiment (10x5-CV fold) using LibLinear.',
        fromfile_prefix_chars='@')
parser.add_argument('DATAFILE', type=str, help='Training datafile')
parser.add_argument('LABELSFILE', type=str, help='Sample labels')
parser.add_argument('SCALING', type=str, choices=['norm_l2', 'std', 'minmax'], default='norm_l2', help='Scaling method')
parser.add_argument('SVM_TYPE', type=str, choices=['l2r_l2loss_svc', 'l2r_l2loss_svc_dual', 'l2r_l1loss_svc_dual', 'l2r_lr_dual', 'l1r_l2loss_svc'], help='SVM type')
parser.add_argument('RANK_METHOD', type=str, choices=['SVM', 'RFE', 'ReliefF', 'tree', 'randomForest', 'KBest', 'random'], help='Feature ranking method: SVM (SVM weights), RFE, ReliefF, extraTrees, Random Forest, Anova F-score, random ranking')
parser.add_argument('OUTDIR', type=str, help='Output directory')
parser.add_argument('--random', action='store_true', help='Run with random sample labels')
parser.add_argument('--cv_k', type=np.int, default=5, help='Number of CV folds')
parser.add_argument('--cv_n', type=np.int, default=10, help='Number of CV cycles')
parser.add_argument('--reliefk', type=np.int, default=3, help='Number of nearest neighbors for ReliefF')
parser.add_argument('--rfep', type=np.float, default=0.2, help='Fraction of features to remove at each iteration in RFE (p=0 one variable at each step, p=1 naive ranking)')
parser.add_argument('--plot', action='store_true', help='Plot metric values over all training cycles' )

if len(sys.argv)==1:
    parser.print_help()
    sys.exit(1)

args = parser.parse_args()
DATAFILE = args.DATAFILE
LABELSFILE = args.LABELSFILE
SCALING = args.SCALING
SVM_TYPE = args.SVM_TYPE
RANK_METHOD = args.RANK_METHOD
OUTDIR = args.OUTDIR
plot_out = args.plot
random_labels = args.random
CV_K = args.cv_k
CV_N = args.cv_n
relief_k = args.reliefk
rfe_p = args.rfep

BASEFILE = os.path.splitext(os.path.basename(DATAFILE))[0]
OUTFILE = os.path.join(OUTDIR, '_'.join([BASEFILE, SVM_TYPE, RANK_METHOD, SCALING]))

# load modules
if RANK_METHOD == 'ReliefF':
    from relief import ReliefF
    # add ReliefF K to OUTFILE
    OUTFILE = os.path.join(OUTDIR, '_'.join([BASEFILE, SVM_TYPE, RANK_METHOD + str(relief_k), SCALING]))
elif RANK_METHOD == 'tree' :
    from sklearn.ensemble import ExtraTreesClassifier
elif RANK_METHOD == 'randomForest' :
    from sklearn.ensemble import RandomForestClassifier
elif RANK_METHOD == 'KBest':
    from sklearn.feature_selection import SelectKBest, f_classif

# number of CV folds
#CV_K = 5
# number of CV cycles
#CV_N = 10
# number of Montecarlo CV cycles (for SVM tuning)
TUN_CV_K = 10
# fraction of the dataset to keep apart as test split (for SVM tuning)
TUN_CV_P = 50
# list of C values for SVM tuning
TUN_SVM_C = [10**k for k in np.arange(-7, 5)]

sample_names, var_names, x = load_data(DATAFILE)
y = np.loadtxt(LABELSFILE, dtype=np.int)

# build FSTEPS according to dataset size
nfeat = x.shape[1]
ord = np.int(np.log10(nfeat))
fs = np.empty(0, dtype=np.int)
for p in range(ord+1):
    fs = np.concatenate( (fs, np.dot(10**p, np.arange(10))) )
fs = np.unique(fs)[1:]
# cap FSTEPS at 10000 features, if applicable
FLIM = 10000 if nfeat>10000 else nfeat
FSTEPS = fs[ fs <= FLIM ].tolist() + [nfeat]

### FSTEPS = range(1,10) + range(10, 100, 10) + range(100, 1000, 100) + range(1000, 10000, 1000) + [10000] + [x.shape[1]]

logf = open(OUTFILE + ".log", 'w')
log_w = csv.writer(logf, delimiter='\t', lineterminator='\n')
log_w.writerow(["SOFTWARE VERSIONS"])
log_w.writerow(["Python", sys.version.replace('\n', '')])
log_w.writerow(["Numpy", np.__version__])
log_w.writerow(["MLPY", mlpy.__version__])
log_w.writerow(["CV PARAMETERS"])
log_w.writerow(["Folds", CV_K])
log_w.writerow(["Iterations", CV_N])
log_w.writerow(["INPUT FILES"])
log_w.writerow(["Data file", DATAFILE])
log_w.writerow(["Labels file", LABELSFILE])
logf.close()

metricsf = open(OUTFILE + "_metrics.txt", 'w')
metrics_w = csv.writer(metricsf, delimiter='\t', lineterminator='\n')

rankingf = open(OUTFILE + "_featurelist.txt", 'w')
ranking_w = csv.writer(rankingf, delimiter='\t', lineterminator='\n')
ranking_w.writerow(["FEATURE_ID", "FEATURE_NAME", "MEAN_POS", "MEDIAN_ALL", "MEDIAN_0", "MEDIAN_1", "FOLD_CHANGE", "LOG2_FOLD_CHANGE"])

internalf = open(OUTFILE + "_internal.txt", 'w')
internal_w = csv.writer(internalf, delimiter='\t', lineterminator='\n')
internal_w.writerow(["TUN_CV_K: %d, TUN_CV_P: %d" % (TUN_CV_K, TUN_CV_P)])
internal_w.writerow(["C", "MCC (test)", "ERR (test)", "MCC (train)", "ERR (train)"])

stabilityf = open(OUTFILE + "_stability.txt", 'w')
stability_w = csv.writer(stabilityf, delimiter='\t', lineterminator='\n')

RANKING = np.empty((CV_K*CV_N, x.shape[1]), dtype=np.int)
NPV = np.empty((CV_K*CV_N, len(FSTEPS)))
PPV = np.empty_like(NPV)
SENS = np.empty_like(NPV)
SPEC = np.empty_like(NPV)
MCC = np.empty_like(NPV)
AUC = np.empty_like(NPV)
DOR = np.empty_like(NPV)
ACC = np.empty_like(NPV)

print "Tuning SVM..."
C, mcc, err, mcc_tr, err_tr = svmlin_t(x, y, svm_type=SVM_TYPE, scaling=SCALING, list_C=TUN_SVM_C, cv_k=TUN_CV_K, cv_p=TUN_CV_P)
print "Best C: %s (MCC: %.3f)" % (C, mcc)
internal_w.writerow([C, mcc, err, mcc_tr, err_tr])
internalf.close()

ys=[]

if random_labels:
    np.random.seed(0)
    tmp = y.copy()
    np.random.shuffle(tmp)
    for i in range(CV_N):
        ys.append(tmp)
else:
    for i in range(CV_N):
        ys.append(y)

for n in range(CV_N):
    idx = mlpy.cv_kfold(n=x.shape[0], k=CV_K, strat=ys[n], seed=n)
    print "%d over %d experiments" % (n+1, CV_N)

    for i, (idx_tr, idx_ts) in enumerate(idx):

        x_tr, x_ts = x[idx_tr], x[idx_ts]
        y_tr, y_ts = ys[n][idx_tr], ys[n][idx_ts]

        # centering and normalization
        if SCALING == 'norm_l2':
            x_tr, m_tr, r_tr = norm_l2(x_tr) 
            x_ts, _, _ = norm_l2(x_ts, m_tr, r_tr) 
        elif SCALING == 'std':
            x_tr, m_tr, r_tr = standardize(x_tr) 
            x_ts, _, _ = standardize(x_ts, m_tr, r_tr)            
        elif SCALING == 'minmax':
            x_tr, m_tr, r_tr = minmax_scaling(x_tr) 
            x_ts, _, _ = minmax_scaling(x_ts, m_tr, r_tr)

        w = compute_weights(y_tr)
        svm = mlpy.LibLinear(solver_type=SVM_TYPE, C=C, weight=w)
        
        if RANK_METHOD == 'random':
            ranking_tmp = np.arange(len(var_names))
            np.random.shuffle(ranking_tmp)
        elif RANK_METHOD == 'SVM':
            svm.learn(x_tr, y_tr)
            w = svm.w()
            ranking_tmp = np.argsort(np.abs(w))[::-1]
        elif RANK_METHOD == 'RFE':
            ranking_tmp = mlpy.rfe_w2(x_tr, y_tr, rfe_p, svm)
        elif RANK_METHOD == 'ReliefF':
            relief = ReliefF(relief_k, seed=n)
            relief.learn(x_tr, y_tr)
            w = relief.w()
            ranking_tmp = np.argsort(w)[::-1]
        elif RANK_METHOD == 'tree' :
            forest = ExtraTreesClassifier(n_estimators=250, criterion='gini', random_state=n)
            forest.fit(x_tr, y_tr)
            ranking_tmp = np.argsort(forest.feature_importances_)[::-1]
        elif RANK_METHOD == 'randomForest' :
            forest = RandomForestClassifier(n_estimators=250, criterion='gini', random_state=n)
            forest.fit(x_tr, y_tr)
            ranking_tmp = np.argsort(forest.feature_importances_)[::-1]
        elif RANK_METHOD == 'KBest':
            selector = SelectKBest(f_classif)
            selector.fit(x_tr, y_tr)
            ranking_tmp = np.argsort( -np.log10(selector.pvalues_) )[::-1]
            
        RANKING[(n * CV_K) + i] = ranking_tmp

        for j, s in enumerate(FSTEPS):
            v = RANKING[(n * CV_K) + i][:s]
            x_tr_fs, x_ts_fs = x_tr[:, v], x_ts[:, v]
            svm.learn(x_tr_fs, y_tr)
            p = svm.pred(x_ts_fs)
            pv = svm.pred_values(x_ts_fs)
            NPV[(n * CV_K) + i, j] = perf.npv(y_ts, p)
            PPV[(n * CV_K) + i, j] = perf.ppv(y_ts, p)
            SENS[(n * CV_K) + i, j] = perf.sensitivity(y_ts, p)
            SPEC[(n * CV_K) + i, j] = perf.specificity(y_ts, p)
            MCC[(n * CV_K) + i, j] = perf.KCCC_discrete(y_ts, p)
            AUC[(n * CV_K) + i, j] = perf.auc_wmw(y_ts, -pv)
            DOR[(n * CV_K) + i, j] = perf.dor(y_ts, p)
            ACC[(n * CV_K) + i, j] = perf.accuracy(y_ts, p)

# write metrics for all CV iterations
np.savetxt(OUTFILE + "_allmetrics_MCC.txt", MCC, fmt='%.4f', delimiter='\t')
np.savetxt(OUTFILE + "_allmetrics_SENS.txt", SENS, fmt='%.4f', delimiter='\t')
np.savetxt(OUTFILE + "_allmetrics_SPEC.txt", SPEC, fmt='%.4f', delimiter='\t')
np.savetxt(OUTFILE + "_allmetrics_PPV.txt", PPV, fmt='%.4f', delimiter='\t')
np.savetxt(OUTFILE + "_allmetrics_NPV.txt", NPV, fmt='%.4f', delimiter='\t')
np.savetxt(OUTFILE + "_allmetrics_AUC.txt", AUC, fmt='%.4f', delimiter='\t')
np.savetxt(OUTFILE + "_allmetrics_ACC.txt", ACC, fmt='%.4f', delimiter='\t')
np.savetxt(OUTFILE + "_allmetrics_DOR.txt", DOR, fmt='%.4f', delimiter='\t')

# write all rankings
np.savetxt(OUTFILE + "_ranking.csv", RANKING, fmt='%d', delimiter='\t')

# average values
AMCC = np.mean(MCC, axis=0)
ASENS = np.mean(SENS, axis=0)
ASPEC = np.mean(SPEC, axis=0)
APPV = np.mean(PPV, axis=0)
ANPV = np.mean(NPV, axis=0)
AAUC = np.mean(AUC, axis=0)
AACC = np.mean(ACC, axis=0)
ADOR = np.mean(DOR, axis=0)
# approximated Odds Ratio, computed from ASENS and ASPEC (to avoid inf and nan values)
ADOR_APPROX = (ASENS / (1 - ASPEC)) / ((1 - ASENS) / ASPEC)

# confidence intervals
NPVCI = []
for i in range(NPV.shape[1]):
    NPVCI.append(mlpy.bootstrap_ci(NPV[:, i]))
PPVCI = []
for i in range(PPV.shape[1]):
    PPVCI.append(mlpy.bootstrap_ci(PPV[:, i]))
SENSCI = []
for i in range(SENS.shape[1]):
    SENSCI.append(mlpy.bootstrap_ci(SENS[:, i]))
SPECCI = []
for i in range(SPEC.shape[1]):
    SPECCI.append(mlpy.bootstrap_ci(SPEC[:, i]))
MCCCI = []
for i in range(MCC.shape[1]):
    MCCCI.append(mlpy.bootstrap_ci(MCC[:, i]))
AUCCI = []
for i in range(AUC.shape[1]):
    AUCCI.append(mlpy.bootstrap_ci(AUC[:, i]))
DORCI = []
for i in range(DOR.shape[1]):
    DORCI.append(mlpy.bootstrap_ci(DOR[:, i]))
ACCCI = []
for i in range(ACC.shape[1]):
    ACCCI.append(mlpy.bootstrap_ci(ACC[:, i]))

# Borda list
BORDA_ID, _, BORDA_POS = mlpy.borda_count(RANKING)
# optimal number of features (yielding max MCC)
opt_feats = FSTEPS[np.argmax(AMCC)]

# Canberra stability indicator
STABILITY = []
PR = np.argsort( RANKING )
for ss in FSTEPS:
    STABILITY.append( mlpy.canberra_stability(PR, ss) )

metrics_w.writerow(["FS_WITH_BEST_MCC", opt_feats])
metrics_w.writerow(["STEP",
                    "MCC", "MCC_MIN", "MCC_MAX",
                    "SENS", "SENS_MIN", "SENS_MAX",
                    "SPEC", "SPEC_MIN", "SPEC_MAX",
                    "PPV", "PPV_MIN", "PPV_MAX",
                    "NPV", "NPV_MIN", "NPV_MAX",
                    "AUC", "AUC_MIN", "AUC_MAX",
                    "ACC", "ACC_MIN", "ACC_MAX",
                    "DOR", "DOR_MIN", "DOR_MAX",
                    "DOR_APPROX"])

stability_w.writerow(["STEP", "STABILITY"])

for j, s in enumerate(FSTEPS):
    metrics_w.writerow([s,
                        AMCC[j], MCCCI[j][0], MCCCI[j][1],
                        ASENS[j], SENSCI[j][0], SENSCI[j][1],
                        ASPEC[j], SPECCI[j][0], SPECCI[j][1],
                        APPV[j], PPVCI[j][0], PPVCI[j][1],
                        ANPV[j], NPVCI[j][0], NPVCI[j][1],
                        AAUC[j], AUCCI[j][0], AUCCI[j][1],
                        AACC[j], ACCCI[j][0], ACCCI[j][1],
                        ADOR[j], DORCI[j][0], DORCI[j][1],
                        ADOR_APPROX[j] ])
    stability_w.writerow( [s, STABILITY[j]] )

metricsf.close()
stabilityf.close()

for i, pos in zip(BORDA_ID, BORDA_POS):
    classes = np.unique(y)
    med_all = np.median(x[:, i])
    med_c = np.zeros(np.shape(classes)[0])
    for jj,c in enumerate(classes):
        med_c[jj] = np.median(x[y==c, i])
    with np.errstate(divide='ignore'):
        fc = med_c[1] / med_c[0]
    log2fc = np.log2(fc)
    ranking_w.writerow([ i, var_names[i], pos+1, med_all, med_c[0], med_c[1], fc, log2fc ])
rankingf.close()

if plot_out:
    from metrics_plot import *
    plt_title = (' ').join( [os.path.basename(DATAFILE).replace('.txt', ''), SCALING, SVM_TYPE] )
    if random_labels:
        metplot(RLFile = (OUTFILE + "_metrics.txt"), title = plt_title)
    elif RANK_METHOD=='random':
        metplot(RRFile = (OUTFILE + "_metrics.txt"), title = plt_title)
    else: 
        metplot(normFile = (OUTFILE + "_metrics.txt"), title = plt_title)
