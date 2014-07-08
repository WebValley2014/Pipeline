#!/usr/bin/env python

import csv
import numpy
import optparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import uuid

class SFF2OTU:
    def __init__(self, job_id, sff, mapping):
        if isinstance(sff, str) or isinstance(sff, unicode):
            sff = [sff]
        if isinstance(mapping, str) or isinstance(mapping, unicode):
            mapping = [mapping]

        if len(sff) != len(mapping):
            raise ValueError, 'the number of sff files and oligo files must be same'

        self.job_id = job_id
        self.sff = sff
        self.mapping = mapping

        self.dir = tempfile.mkdtemp()
        self.fasta_dir = tempfile.mkdtemp()

        self.fasta = []
        self.qual = []
        self.oligo = []

        self.trim_fasta = []
        self.group = []
        self.result = {}

    def __del__(self):
        import shutil
        shutil.rmtree(self.dir)
        shutil.rmtree(self.fasta_dir)

    def run(self, processors = 1, percentage = 10, *args, **kwargs):
        kwargs['processors'] = processors

        self.sffinfo()
        self.map2oligo()
        self.trim(kwargs)
        self.split()

        mapfile = self.merge_map()
        self.merge_fasta(mapfile)
        self.pick_otus(processors)
        biom = self.filter_otu(percentage)
        taxa_otu = self.summarize_taxa(biom)
        self.merge_otu(taxa_otu)

        return self.result

    def command(self, args):
        process = subprocess.Popen(args, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        retcode = process.wait()
        if retcode != 0:
            sys.stderr.write(process.stderr.read())
            raise IOError, '%s raises error: %d' % (args[0], retcode)
        return process.stdout.readlines()

    def mothur(self, outdir, command):
        command = '#set.dir(output=%s); set.logfile(name=%s); %s' % (outdir, os.devnull, command)
        output = self.command(['mothur', command])
        if 'Output File Names: \n' not in output:
            raise IOError, 'mothur didn\'t output anything'
        return output

    def sffinfo(self):
        for sff in self.sff:
            command = 'sffinfo(sff=%s,fasta=T)' % sff
            output = self.mothur(self.dir, command)

            for line in output[output.index('Output File Names: \n') + 1:]:
                if not line:
                    break
                if line.strip().endswith('fasta'):
                    self.fasta.append(line.strip())
                elif line.strip().endswith('qual'):
                    self.qual.append(line.strip())

    def map2oligo(self):
        for mapping in self.mapping:
            data = numpy.loadtxt(mapping, dtype = 'string')
            output = os.path.join(self.dir, os.path.splitext(os.path.basename(mapping))[0] + '.oligo')

            with open(output, 'w') as oligo:
                writer = csv.writer(oligo, delimiter = '\t', lineterminator = '\n')
                writer.writerow(['forward', data[0, 2]])

                for i in xrange(data.shape[0]):
                    writer.writerow(['barcode', data[i, 1], data[i, 0]])
            
            self.oligo.append(output)

    def trim(self, kwargs):
        if not len(self.fasta) == len(self.qual) == len(self.oligo):
            raise ValueError, 'sffinfo and map2oligo must be executed before trim'
        
        for fasta, qual, oligo in zip(self.fasta, self.qual, self.oligo):
            kwargs['fasta'] = fasta
            kwargs['oligos'] = oligo
            kwargs['qfile'] = qual
            options = ','.join([str(i[0]) + '=' + str(i[1]) for i in kwargs.items()])

            command = 'trim.seqs(%s)' % options
            output = self.mothur(self.dir, command)

            for line in output[output.index('Output File Names: \n') + 1:]:
                if not line:
                    break
                if line.strip().endswith('trim.fasta'):
                    self.trim_fasta.append(line.strip())
                elif line.strip().endswith('groups'):
                    self.group.append(line.strip())

    def split(self):
        if len(self.trim_fasta) != len(self.group):
            raise ValueError, 'trim must be executed before split'

        for trim, group in zip(self.trim_fasta, self.group):
            command = 'split.groups(fasta=%s,group=%s)' % (trim, group)
            output = self.mothur(self.fasta_dir, command)

    def merge_map(self):
        if len(self.trim_fasta) != len(self.mapping):
            raise ValueError, 'trim must be executed before split'

        mapfile = os.path.join(self.dir, 'merged.map')
        with open(mapfile, 'w') as output:
            writer = csv.writer(output, delimiter = '\t', lineterminator = '\n')
            writer.writerow(["#SampleID", "BarcodeSequence", "LinkerPrimerSequence", "Path", "Class", "Description"])

            for mapping, trim in zip(self.mapping, self.trim_fasta):
                data = numpy.loadtxt(mapping, dtype = 'string')
                for i in xrange(data.shape[0]):
                    filename = os.path.splitext(os.path.basename(trim))[0] + '.' + data[i, 0] + '.fasta'
                    writer.writerow(data[i, 0: 3].tolist() + [filename] + data[i, 3:].tolist())

        return mapfile

    def merge_fasta(self, mapfile):
        self.command(['add_qiime_labels.py', '-m', mapfile, '-i', self.fasta_dir, '-c', 'Path', '-o', self.dir])

    def pick_otus(self, parallel):
        combined = os.path.join(self.dir, 'combined_seqs.fna')
        self.command(['pick_de_novo_otus.py', '-i', combined, '-o', self.dir, '-f', '-a', '-O', str(parallel)])

        out_dir = os.path.dirname(self.sff[0])
        for filename in ['otu_table.biom', 'rep_set.tre', os.path.join('rep_set', 'combined_seqs_rep_set.fasta')]:
            out_file = os.path.join(out_dir, os.path.basename(filename))
            shutil.copyfile(os.path.join(self.dir, filename), out_file)
            self.result[os.path.splitext(filename)[1][1:]] = out_file

    def filter_otu(self, percentage):
        biom = os.path.join(self.dir, 'otu_table.biom')
        otu_table = os.path.join(self.dir, 'otu_table.txt')
        process = self.command(['biom', 'convert', '-i', biom, '-o', otu_table, '-b', '--header-key=taxonomy'])

        otu_data = numpy.loadtxt(otu_table, dtype = str, delimiter = '\t')
        ncol = otu_data.shape[1] - 2

        filtered = os.path.join(self.dir, 'filter.biom')
        self.command(['filter_taxa_from_otu_table.py', '-i', biom, '-o', filtered, '-n', 'Unassigned'])
        self.command(['filter_otus_from_otu_table.py', '-i', filtered, '-o', biom, '-s', str(ncol * percentage / 100)])

        return biom

    def summarize_taxa(self, biom):
        taxa_out = os.path.join(self.dir, 'taxa_out')
        self.command(['summarize_taxa.py', '-i', biom, '-o', taxa_out, '-L', '2,3,4,5,6,7'])
        return taxa_out

    def merge_otu(self, taxa_out):
        line = None
        data = None

        for otu in os.listdir(taxa_out):
            if not re.search('L[0-9]*.txt$', otu):
                continue

            matrix = numpy.loadtxt(os.path.join(taxa_out, otu), dtype = str, delimiter = '\t')
            if line == None:
                line = matrix[0]

            if data == None:
                data = matrix[1:]
            else:
                data = numpy.concatenate((data, matrix[1:]))

        line[0] = '#OTU ID'
        line = numpy.append(line, 'Label')

        data = numpy.append(data, data[:, 0].reshape(len(data), 1), axis = 1)
        for i in xrange(len(data)):
            data[i, 0] = 'merged' + str(i)

        otu_table = os.path.join(os.path.dirname(self.sff[0]), 'otu_table.txt')
        with open(otu_table, 'w') as output:
            writer = csv.writer(output, delimiter = '\t', lineterminator = '\n')
            writer.writerow(line)
            for line in data:
                writer.writerow(line)

        self.result['txt'] = otu_table

if __name__ == '__main__':
    parser = optparse.OptionParser(usage = 'Usage: %prog [OPTIONS]')
    parser.add_option('-p', '--parallel', help = 'number of jobs for parallelizing in denosing and pick_de_novo_otus.py [default: %default]', type = 'int', default = 1)
    parser.add_option('-s', '--sff-files', help = 'sff files (comma separated)')
    parser.add_option('-m', '--map-files', help = 'map files (comma separated)')

    options, args = parser.parse_args()
    if not options.sff_files:
        parser.error('sff files must be specified')
    if not options.map_files:
        parser.error('map files must be specified')

    sff = options.sff_files.split(',')
    mapping = options.map_files.split(',')
    job_id = str(uuid.uuid4())

    sff2otu = SFF2OTU(job_id, sff, mapping)
    print(sff2otu.run(processors = options.parallel))
