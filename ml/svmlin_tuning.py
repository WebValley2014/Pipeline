## This code is written by Davide Albanese <albanese@fbk.eu> and Marco Chierici <chierici@fbk.eu>
# CHANGELOG
# 2014-01-23 [MC] added: train and test errors, train MCC
# 2014-01-09 [AZ] added: new normalization methods (norm_l2, std)
# 2013-04-04 [MC] added: data normalization in place of minmax_scaling
# 2013-03-27 [MC] added: print MCC for each C iteration
from __future__ import division
import numpy as np
import mlpy
from scaling import *
import performance as perf
#from performance import KCCC_discrete
import sys
import argparse

def compute_weights(y):
    classes = np.unique(y)
    weight = {}
    for c in classes:
        weight[c] = y.shape[0] / np.sum(y==c)
    return weight

def svmlin_t(x, y, svm_type, scaling, list_C, cv_k, cv_p):
    
    print 'Scaling: %s' %scaling
    idx = mlpy.cv_random(n=x.shape[0], k=cv_k, p=cv_p, strat=y)
    
    # average MCC on test, avg MCC on train, avg error on test, avg error on train
    AMCC_ts, AMCC_tr, AERR_ts, AERR_tr = [], [], [], []
    # vectors containing CV metrics
    mcc_ts = np.zeros(cv_k, dtype=np.float)
    mcc_tr = np.zeros(cv_k, dtype=np.float)
    err_ts = np.zeros(cv_k, dtype=np.float)
    err_tr = np.zeros(cv_k, dtype=np.float)

    for C in list_C:
        for i, (idx_tr, idx_ts) in enumerate(idx):
            x_tr, x_ts = x[idx_tr], x[idx_ts]
            y_tr, y_ts = y[idx_tr], y[idx_ts]

            # centering and normalization            
            if scaling == 'norm_l2':
                #print 'Performing: norm_l2'
                x_tr, m_tr, r_tr = norm_l2(x_tr) 
                #x_ts, _, _ = norm_l2(x_ts, m_tr, r_tr) 
                x_ts, m_ts, r_ts = norm_l2(x_ts, m_tr, r_tr)
            elif scaling == 'std':
                #print 'Performing: std'
                x_tr, m_tr, r_tr = standardize(x_tr) 
                #x_ts, _, _ = standardize(x_ts, m_tr, r_tr)  
                x_ts, m_ts, r_ts = standardize(x_ts, m_tr, r_tr)
            elif scaling == 'minmax':
                #print 'Performing: minmax'
                x_tr, m_tr, r_tr = minmax_scaling(x_tr) 
                #x_ts, _, _ = minmax_scaling(x_ts, m_tr, r_tr)
                x_ts, m_ts, r_ts = minmax_scaling(x_ts, m_tr, r_tr)

            w = compute_weights(y_tr)
            svm = mlpy.LibLinear(solver_type=svm_type, C=C, weight=w)
            # train SVM
            svm.learn(x_tr, y_tr)
            # predict on test, train
            ypts = svm.pred(x_ts)
            yptr = svm.pred(x_tr)
            
            # MCC on test, train
            mcc_ts[i] = perf.KCCC_discrete(y_ts, ypts)
            mcc_tr[i] = perf.KCCC_discrete(y_tr, yptr)

            # error on test, train
            err_ts[i] = perf.error(y_ts, ypts)
            err_tr[i] = perf.error(y_tr, yptr)

        AMCC_ts.append(np.mean(mcc_ts))
        AMCC_tr.append(np.mean(mcc_tr))
        AERR_ts.append(np.mean(err_ts))
        AERR_tr.append(np.mean(err_tr))
        
        print "C: %f -> MCC %.3f, test error %.3f (train MCC %.3f, train error %.3f)" % (C, AMCC_ts[-1], AERR_ts[-1], AMCC_tr[-1], AERR_tr[-1])
    
    # best C maximizes AMCC_ts
    bestC_idx = np.argmax(AMCC_ts)
    # return train/test MCC and Error
    return list_C[bestC_idx], AMCC_ts[bestC_idx], AERR_ts[bestC_idx], AMCC_tr[bestC_idx], AERR_tr[bestC_idx]

