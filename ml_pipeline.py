#!/usr/bin/env python

import csv
import os
import numpy
import shutil
import subprocess
import sys
import tempfile
import uuid

import plot_metrics

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
        self.result = {}

    def __del__(self):
        import shutil
        shutil.rmtree(self.dir)

    def run(self, percentage = 10, n_groups = 10, scaling = 'std', solver = 'l2r_l2loss_svc', ranking = 'SVM', *args, **kwargs):
        otu_table = self.filter_otu(percentage)
        matrix, classes = self.convert_input(otu_table)
        self.machine_learning(matrix, classes, scaling, solver, ranking, kwargs)
        self.process_otu_table(n_groups, classes)
        self.phylo3d()

        self.result['img'] = os.path.join(os.path.dirname(self.otu_file), 'img')
        graph = plot_metrics.BacteriaGraph(self.result['metrics'])
        graph.printAllPlots(self.result['img'])

        return self.result

    def command(self, args):
        process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        retcode = process.wait()
        if retcode != 0:
            sys.stderr.write(process.stderr.read())
            raise IOError, '%s raises error: %d' % (args[0], retcode)
        return process.stdout.readlines()

    def filter_otu(self, percentage):
        script = os.path.join(os.path.dirname(__file__), 'ml', 'feat_filter.py')
        out_file = os.path.join(self.dir, 'filtered_otu.txt')
        process = self.command(['python', script, '-i', self.otu_file, '-o', out_file, '-p', str(percentage)])

        self.result['filtered_otu'] = os.path.join(os.path.dirname(self.otu_file), self.job_id + '.otu_filtered.txt')
        shutil.copyfile(out_file, self.result['filtered_otu'])

        return out_file

    def convert_input(self, otu_file):
        features = []
        samples = None
        data = None

        with open(otu_file) as otu:
            reader = csv.reader(otu, delimiter = '\t')

            line = []
            while len(line) == 0 or line[0] != '#OTU ID':
                line = reader.next()
            samples = line[1: -1]

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
        for filename in os.listdir(outdir):
            for suffix in ['featurelist', 'metrics', 'stability']:
                if filename.endswith(suffix + '.txt'):
                    output = prefix + suffix + '.txt'
                    shutil.copyfile(os.path.join(outdir, filename), output)
                    self.result[suffix] = output

    def process_otu_table(self, n_groups, classes):
        feature_names = numpy.loadtxt(self.result['featurelist'], dtype = str, skiprows = 1, usecols = (0, 1))
        ranked = feature_names[:, 1]

        whole_table = numpy.loadtxt(self.result['filtered_otu'], dtype = str, delimiter = '\t', comments = '^')
        name_column = whole_table[:, -1]

        processed_table = numpy.zeros((n_groups + 2, len(whole_table[1, :])), dtype = 'S512')
        processed_table[0,:] = whole_table[0,:]

        labels = numpy.loadtxt(classes, dtype = str, delimiter = '\n')
        processed_table[1, 0] = 'Label'
        processed_table[1, 1: -1] = labels

        current_row = 2
        for f in ranked[:n_groups]:
            for i in range(1, len(name_column)):
                if f in name_column[i] and current_row <= n_groups:
                    processed_table[current_row, :] = whole_table[i, :]
                    current_row += 1

        processed_table.transpose()

        self.result['otu'] = os.path.join(os.path.dirname(self.otu_file), self.job_id + '.otu_table.txt')
        numpy.savetxt(self.result['otu'], processed_table, delimiter = '\t', fmt = '%s')

    def phylo3d(self):
        script = os.path.join(os.path.dirname(__file__), 'phylo3D', 'import.py')
        xml = os.path.join(self.dir, 'dendro.xml')
        process = self.command(['python', script, self.result['featurelist'], xml])

        script = os.path.join(os.path.dirname(__file__), 'phylo3D', 'process.py')
        self.result['json'] = os.path.join(os.path.dirname(self.otu_file), self.job_id + '.json')
        process = self.command(['python', script, xml, self.result['featurelist'], self.result['json']])

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print 'Usage: %s [DATA_FILE] [CLASS_FILE]' % sys.argv[0]
        sys.exit(-1)

    job_id = str(uuid.uuid4())
    ml = ML(job_id, sys.argv[1], sys.argv[2])
    print(ml.run())
