#!/usr/bin/env python

import os
import datetime
import ml_pipeline
import multiprocessing
import sff2otu

from celery import Celery

app = Celery('tasks', broker = 'amqp://wvlab:wv2014@54.72.200.168/', backend = 'amqp')

@app.task(bind = True, name = 'prepro')
def preprocess(self, uniqueJobID, listofSFFfiles, listOfMappingFiles):
    core = max(multiprocessing.cpu_count() - 1, 1)

    start_time = unicode(datetime.datetime.now())
    pipeline = sff2otu.SFF2OTU(uniqueJobID, listofSFFfiles, listOfMappingFiles)
    result = os.path.abspath(pipeline.run(processors = core))
    finish_time = unicode(datetime.datetime.now())

    return {'funct': result, 'st': start_time, 'ft': finish_time}

@app.task
def machine_learning(job_id, otu_file, class_file, *args, **kwargs):
    pipeline = ml_pipeline.ML(job_id, otu_file, class_file)
    return [os.path.abspath(path) for path in pipeline.run(*args, **kwargs)]
