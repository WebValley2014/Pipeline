#!/usr/bin/env python

import os
import multiprocessing
import sff2otu

from celery import Celery

app = Celery('tasks', broker = 'amqp://wvlab:wv2014@54.72.200.168/', backend = 'amqp')

@app.task
def preprocess(job_id, sff, mapping):
    core = multiprocessing.cpu_count() - 1

    pipeline = sff2otu.SFF2OTU(job_id, sff, mapping)
    return os.path.abspath(pipeline.run(processors = core))
