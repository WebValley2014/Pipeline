#!/usr/bin/env python

import csv
import os
import numpy
import shutil
import subprocess
import sys
import tempfile
import uuid

class ML:
    def __init__(self, job_id, otu_file, class_file):
        self.otu_file = otu_file
        if not os.path.exists(self.otu_file):
            raise IOError, 'file doesn\'t exists: %s' % self.otu_file

        self.class_file = class_file
        if not os.path.exists(self.class_file):
            raise IOError, 'file doesn\'t exists: %s' % self.class_file

        self.job_id = job_id
        self.dir = tempfile.mkdtemp()

    def __del__(self):
        import shutil
        shutil.rmtree(self.dir)

    def run(self, scaling = 'std', solver = 'l2r_l2loss_svc', ranking = 'SVM', *args, **kwargs):
        matrix, classes = self.convert_input()
        return self.machine_learning(matrix, classes, scaling, solver, ranking, kwargs)

    def command(self, args):
        process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        retcode = process.wait()
        if retcode != 0:
            sys.stderr.write(process.stderr.read())
            raise IOError, '%s raises error: %d' % (args[0], retcode)
        return process.stdout.readlines()

    def convert_input(self):
        features = []
        samples = None
        data = None

        with open(self.otu_file) as otu:
            reader = csv.reader(otu, delimiter = '\t')
            samples = reader.next()[1:]

            for line in reader:
                features.append(line[-1])
                if data == None:
                    data = line[1: -1]
                else:
                    data = numpy.vstack((data, line[1: -1]))
        
            data = numpy.transpose(data, (1, 0))
            data = numpy.hstack((numpy.array(samples).reshape(len(samples), 1), data))

        classes = numpy.loadtxt(self.class_file, dtype = str)
        if len(classes[0]) > 1:
            classes.sort(axis = 0)
            data.sort(axis = 0)

        matrix_txt = os.path.join(self.dir, 'matrix.txt')
        classes_txt = os.path.join(self.dir, 'classes.txt')

        with open(matrix_txt, 'w') as output:
            writer = csv.writer(output, delimiter = '\t', lineterminator = '\n')
            writer.writerow(['Sample ID'] + features)
            for line in data:
                writer.writerow(line)

        with open(classes_txt, 'w') as output:
            writer = csv.writer(output, delimiter = '\t', lineterminator = '\n')
            for line in classes:
                writer.writerow(line[-1])

        return matrix_txt, classes_txt

    def machine_learning(self, matrix, classes, scaling, solver, ranking, kwargs):
        script = os.path.join(os.path.dirname(__file__), 'ml', 'svmlin_training.py')

        options = []
        for key, value in kwargs.items():
            options.append('--' + key)
            if not isinstance(value, bool):
                options.append(str(value))

        outdir = os.path.join(self.dir, 'out')
        os.mkdir(outdir)

        process = self.command(['python', script, matrix, classes, scaling, solver, ranking] + options + [outdir])

        prefix = os.path.join(os.path.dirname(self.otu_file), self.job_id + '.')
        result = []

        for filename in os.listdir(outdir):
            for suffix in ['featurelist', 'metrics', 'stability']:
                if filename.endswith(suffix + '.txt'):
                    shutil.copyfile(os.path.join(outdir, filename), prefix + '.' + suffix + '.txt')
                    result.append(prefix + suffix + '.txt')

        return result

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print 'Usage: %s [DATA_FILE] [CLASS_FILE]' % sys.argv[0]
        sys.exit(-1)

    job_id = str(uuid.uuid4())
    ml = ML(job_id, sys.argv[1], sys.argv[2])
    print(ml.run())
