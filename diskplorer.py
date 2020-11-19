#!/usr/bin/python3

from __future__ import print_function

import os
import json
import subprocess
import matplotlib
import math
import optparse
import multiprocessing
import re
import sys

matplotlib.use('svg')  # must come before pyplot import

import matplotlib.pyplot as plt

def set_output_files_names(buffer_size, test_name, output_filename_prefix, fsync_io):
    """
    Returns a tuple of strings that represent names of output SVG, CSV and JSON files correspondingly.

    :param buffer_size: buffer size used for the test
    :param test_name: fio test name
    :param output_filename_prefix: user provided output file prefix
    """
    output_filename = output_filename_prefix+"-{buffer_size}-{test_name}-fsync-{fsync_io}.svg".format(buffer_size=buffer_size,test_name=test_name, fsync_io=fsync_io)
    raw_filename = output_filename_prefix+"-{buffer_size}-{test_name}-fsync-{fsync_io}.csv".format(buffer_size=buffer_size,test_name=test_name, fsync_io=fsync_io)
    json_filename = output_filename_prefix+'-{buffer_size}-{test_name}-fsync-{fsync_io}.fio.json'.format(buffer_size=buffer_size,test_name=test_name, fsync_io=fsync_io)

    return output_filename, raw_filename, json_filename

def create_fio_spec(fname):
    header = '''\
    [global]
    ioengine=libaio
    buffered=0
    rw={test_name}
    bs={buffer_size}
    size={filesize}
    directory={mountpoint}
    runtime=10s
    filename={fio_input_filename}
    group_reporting=1
    fsync={fsync_io}

    '''

    job_template = '''\
    [{jobname}]
    iodepth={depth}
    {new_group}

    '''

    with open(fname, 'w') as f:
        f.write(header.format(**globals()))
        depth = 1
        growth = 1.05
        while depth <= maxdepth:
            depth_remain = depth
            threads_remain = max_threads
            new_group = 'stonewall'
            # distribute load among max_threads
            while depth_remain:
                depth_now = int(depth_remain / threads_remain)
                if depth_now:
                    f.write(job_template.format(jobname=depth, depth=depth_now, new_group=new_group))
                    new_group = ''
                    depth_remain -= depth_now
                threads_remain -= 1
            depth = int(max(math.ceil(depth * growth), depth + 1))

def run_job():
    spec_fname = 'tmp.fio'
    create_fio_spec(spec_fname)
    result_json = subprocess.check_output(['fio', '--output-format=json'] + readonly + [spec_fname])
    result_json = result_json.decode('utf-8')
    open(json_filename, 'w').write(result_json)
    return json.loads(result_json)

tests_choices = ['read', 'write', 'randwrite', 'randread']

optparser = optparse.OptionParser()

optparser.add_option('-d', '--mountpoint', dest='mountpoint', default='.',
                     help='Test disk mounted at MOUNTPOINT', metavar='MOUNTPOINT')
optparser.add_option('-t', '--test', dest='test_name', choices=tests_choices,
                     help='Test to run', metavar='TEST')
optparser.add_option('-b', '--device', dest='device', default=None,
                     help='Test block device DEV (overrides --mountpoint)', metavar='DEV')
optparser.add_option('-s', '--filesize', dest='filesize', default='100G',
                     help='Set SIZE as file size for test', metavar='SIZE')
optparser.add_option('--buffer-size', dest='buffer_size', default='4k',
                     help='Set SIZE as I/O buffer size for test (ex. 4k, 1M)', metavar='SIZE')
optparser.add_option('-m', '--max-concurrency', dest='maxdepth', default=128, type='int',
                     help='Test maximum concurrency level N', metavar='N')
optparser.add_option('-o', '--output-prefix', dest='output_filename_prefix', default='disk-concurrency-response',
                     help='Write output graph and csv to FILE prefixed with this', metavar='FILE')
optparser.add_option('-f','--file', dest='fio_json',
                     metavar='FILE',
                     help='Input file with fio results for processing')
optparser.add_option('--fsync-io', dest='fsync_io', default=0, type='int', metavar='IOP',
                     help="How many I/Os to perform before issuing an fsync(2) of dirty data. If 0, don't sync. Default: 0.")

########################################################################################################################
(options, args) = optparser.parse_args()

mountpoint = options.mountpoint
filesize = options.filesize
maxdepth = options.maxdepth
buffer_size = options.buffer_size
fsync_io = options.fsync_io

if not options.test_name and not options.fio_json:
    sys.exit("Test name is not set - choose one: {}".format(tests_choices))

test_name = options.test_name
output_filename, raw_filename, json_filename = set_output_files_names(buffer_size, test_name, options.output_filename_prefix, fsync_io)

fio_input_filename = options.output_filename_prefix+'-fiotest.tmp'
readonly = []

if options.device:
    fio_input_filename = options.device
    readonly = ['--readonly']
    mountpoint = '/'

max_threads = multiprocessing.cpu_count()
results = ""

# Run the test

if options.fio_json:
    with open(options.fio_json,"r") as json_file:
        results = json.load(json_file)

        if 'global options' not in results:
            sys.exit("Bad format in {}: 'global options' section in not present".format(options.fio_json))

        global_options = results['global options']

        if 'rw' not in global_options:
            sys.exit("Bad format in {}: 'rw' field is in not present".format(options.fio_json))

        test_name = global_options['rw']

        if 'bs' not in global_options:
            sys.exit("Bad format in {}: 'bs' field is in not present".format(options.fio_json))

        buffer_size = global_options['bs']

        if 'fsync' not in global_options:
            fsync_io = 0

        fsync_io = global_options['fsync']

        output_filename, raw_filename, json_filename = set_output_files_names(buffer_size, test_name, options.output_filename_prefix, fsync_io)

else:
    results = run_job()

# Generate result files

concurrencies = [0]  # FIXME: fake 0 element to force axis limit
latencies = [0.]
latencies_05 = [0.]
latencies_95 = [0.]
latencies_99 = [0.]
latencies_9999 = [0.]
latencies_max = [0.]
iopses = [0.]

stat_label = 'read'
if re.search('write', test_name):
    stat_label = 'write'

for job in results['jobs']:
    concurrency = int(job['jobname'])
    latency = float(job[stat_label]['clat_ns']['mean'])
    latency_05 = float(job[stat_label]['clat_ns']['percentile']['5.000000'])
    latency_95 = float(job[stat_label]['clat_ns']['percentile']['95.000000'])
    latency_99 = float(job[stat_label]['clat_ns']['percentile']['99.000000'])
    latency_9999 = float(job[stat_label]['clat_ns']['percentile']['99.990000'])
    latency_max = float(job[stat_label]['clat_ns']['max'])
    latency_stddev = float(job[stat_label]['clat_ns']['stddev'])
    iops = float(job[stat_label]['iops'])
    concurrencies.append(concurrency)
    latencies.append(latency/1000)
    latencies_05.append(latency_05/1000)
    latencies_95.append(latency_95/1000)
    latencies_99.append(latency_99/1000)
    latencies_9999.append(latency_9999/1000)
    latencies_max.append(latency_max/1000)
    iopses.append(iops)

def fix_y_axis(plt):
    plt.ylim(0.0, plt.ylim()[1])

fig, ax1 = plt.subplots()
ax1.plot(concurrencies, iopses, 'b-+')
ax1.set_xlabel('concurrency')
# Make the y-axis label and tick labels match the line color.
ax1.set_ylabel('{buffer_size} {stat_label} iops'.format(**globals()), color='b')
for tl in ax1.get_yticklabels():
    tl.set_color('b')
# FIXME: want log scale on X axis

ax2 = ax1.twinx()
#ax2.plot(concurrencies, latencies, 'r-+')
ax2.errorbar(concurrencies, latencies, yerr=[latencies_05, latencies_95], color='r')
ax2.set_ylabel(u'average latency (us)\n(top is 95p)', color='r')
for tl in ax2.get_yticklabels():
    tl.set_color('r')

plt.tight_layout()
plt.savefig(fname=output_filename)

with open(raw_filename, 'w') as raw:
    print('buffersize,concurrency,iops,lat_avg(us),lat_05(us),lat_95(us),lat_99(us),lat_9999(us),lat_max(us)', file=raw)
    for concurrency, iops, lat_avg, lat_05, lat_95, lat_99,lat_9999,lat_max in zip(
            concurrencies, iopses, latencies, latencies_05, latencies_95, latencies_99, latencies_9999, latencies_max):
        print('{buffer_size},{concurrency},{iops},{lat_avg},{lat_05},{lat_95},{lat_99},{lat_9999},{lat_max}'
              .format(**locals()), file=raw)
