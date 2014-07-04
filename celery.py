#!/usr/bin/env python

import os
import multiprocessing
import sff2otu

def preprocess(job_id, sff, mapping):
    core = multiprocessing.cpu_count() - 1

    pipeline = sff2otu.SFF2OTU(job_id, sff, mapping)
    return os.path.abspath(pipeline.run(processors = core))
