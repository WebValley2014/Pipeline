#!/usr/bin/env python

import os
import ml_pipeline
import multiprocessing
import sff2otu

from celery import Celery

app = Celery('tasks', broker = 'amqp://wvlab:wv2014@54.72.200.168/', backend = 'amqp')

@app.task
def preprocess(job_id, sff, mapping):
    core = max(multiprocessing.cpu_count() - 1, 1)

    pipeline = sff2otu.SFF2OTU(job_id, sff, mapping)
    return os.path.abspath(pipeline.run(processors = core))

@app.task
def machine_learning(job_id, otu_file, class_file, *args, **kwargs):
    pipeline = ml_pipeline.ML(job_id, otu_file, class_file)
    return [os.path.abspath(path) for path in pipeline.run(*args, **kwargs)]
