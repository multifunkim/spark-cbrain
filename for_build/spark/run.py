#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Runs a SPARK sub-pipeline
#
# Last revision: May, 2020
# Maintainer: Obai Bin Ka'b Ali @aliobaibk
# License: In the app folder or check GNU GPL-3.0.


from argparse import ArgumentParser, RawTextHelpFormatter
from errno import EEXIST
import os
from shlex import quote
from subprocess import run as sp_run
from sys import argv, stderr
from sys import exit as sys_exit
from textwrap import dedent


def run_pipe(iargs):
    """Runs a SPARK sub-pipeline.
    """

    jobs_patterns = ''
    if iargs['jobs_indices']:
        jobs_patterns = ';'.join([str(x) for x in iargs['jobs_indices']]) + ';'
    if iargs['jobs_patterns']:
        jobs_patterns = ' '.join([quote(s) for s in iargs['jobs_patterns']])

    cmd = '{} run {} {} {}'.format(
        quote(iargs['exe']), quote(iargs['pipe_file']), iargs['stage'], jobs_patterns)
    p = sp_run(cmd, shell=True, cwd=iargs['out_dir'])
    if p.returncode != 0:
        print('\n\nThe process returned a non-zero exit status:\n' +
              str(p.returncode), file=stderr)
        sys_exit(1)

    return None


def check_iargs_integrity(iargs):
    """Integrity of the input arguments
    """

    # MATLAB executable
    if not os.path.isfile(iargs['exe']):
        print('--exe\n' +
              'Invalid or nonexistent file:\n' + iargs['exe'], file=stderr)
        sys_exit(1)

    # Pipeline
    if not os.path.isfile(iargs['pipe_file']):
        print('Pipeline file not found:\n' + iargs['pipe_file'], file=stderr)
        sys_exit(1)

    # Jobs indices
    if iargs['jobs_indices'] and any(x < 1 for x in iargs['jobs_indices']):
        print('--jobs-indices\n' +
              'One of the elements is smaller than 1:\n' + str(iargs['jobs_indices']), file=stderr)
        sys_exit(1)

    return None


def get_bids_fmri_filename(fmri):
    """Extracts filename without extension from a BIDS fMRI file
    """

    filename = os.path.basename(fmri)
    extension = filename.split('_')[-1][4:]
    return filename[:-len(extension)]


def get_pipe_file(out_dir, fmri):
    """Builds the path of the pipeline file corresponding to the input fMRI
    """

    filename = get_bids_fmri_filename(fmri)
    return os.sep.join([out_dir, filename, 'pipelines', filename + '.mat'])


def setup_abspath(iargs):
    """Makes sure all paths are absolute.
    """

    iargs['fmri'] = os.path.abspath(iargs['fmri'])
    iargs['out_dir'] = os.path.abspath(iargs['out_dir'])
    iargs['exe'] = os.path.abspath(iargs['exe'])

    return iargs


def check_iargs_parser(iargs):
    """[For running a SPARK sub-pipeline] Defines the possible arguments of the program,
    generates help and usage messages, and issues errors in case of invalid arguments.
    """

    parser = ArgumentParser(
        prog='spark.py',
        description=dedent('''\
        SParsity-based Analysis of Reliable K-hubness (SPARK) for brain fMRI functional
        connectivity
        ____________________________________________________________________________________
         
           8b    d8 88   88 88     888888 88     888888 88   88 88b 88 88  dP 88 8b    d8
           88b  d88 88   88 88       88   88     88__   88   88 88Yb88 88odP  88 88b  d88
           88YbdP88 Y8   8P 88  .o   88   88     88""   Y8   8P 88 Y88 88"Yb  88 88YbdP88
           88 YY 88 `YbodP' 88ood8   88   88     88     `YbodP' 88  Y8 88  Yb 88 88 YY 88
           ------------------------------------------------------------------------------
                              Multimodal Functional Imaging Laboratory
        ____________________________________________________________________________________
         
        '''),
        add_help=False,
        formatter_class=RawTextHelpFormatter)

    # Required
    required = parser.add_argument_group(
        title='  REQUIRED arguments',
        description=dedent('''\
        __________________________________________________________________________________
        '''))
    required.add_argument('--RUN',
                          action='store_true',
                          required=True,
                          help='\n____________________________________________________________')
    required.add_argument('--exe', nargs=1, type=str,
                          required=True,
                          help=dedent('''\
                          Path (absolute or relative) to the MATLAB generated 
                          standalone application.
                           
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar='XXX',
                          dest='exe')
    required.add_argument('--stage', nargs=1, type=str,
                          choices=['A', 'B', 'C'],
                          required=True,
                          help=dedent('''\
                          Sub-pipeline to run.
                           
                          - A: 'Sparse GLM parameters estimation' and 'Bootstrap 
                          resampling'.
                          - B: 'Sparse dictionary leaning'.
                          - C: 'k-hubness map generation'.
                           
                          (valid values: %(choices)s)
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar='X',
                          dest='stage')
    required.add_argument('--fmri', nargs=1, type=str,
                          required=True,
                          help=dedent('''\
                          Path (absolute or relative) to the fMRI data to analyze.
                           
                          Notes:
                          - This file should be a valid fMRI file of a BIDS dataset.
                          - The filename will be used to name the outputs, for
                            example: 'kmap_sub-01_task-rest_bold.mat'.
                           
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar='XXX',
                          dest='fmri')
    required.add_argument('--out-dir', nargs=1, type=str,
                          required=True,
                          help=dedent('''\
                          Path (absolute or relative) to the output directory (old
                          files might get replaced).
                          This directory should have been previously set up with
                          --SETUP and at least contain the pipeline '.mat' file
                          corresponding to the input --fmri.

                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar=('XXX'),
                          dest='out_dir')

    # Optional
    optional = parser.add_argument_group(
        title='  OPTIONAL arguments',
        description=dedent('''\
        __________________________________________________________________________________
        '''))
    optional.add_argument('-h', '--help',
                          action='help',
                          help=dedent('''\
                          Shows this help message and exits.
                          ____________________________________________________________
                          '''))
    optional.add_argument('--jobs-patterns', nargs='+', type=str,
                          default=[],
                          help=dedent('''\
                          For expert users. Used to filter the jobs of interest in the
                          chosen pipeline using strings. All jobs are run by default.
                          If --jobs-patterns and --jobs-indices are both specified,
                          then --jobs-patterns takes precedence.
                           
                          (default: %(default)s)
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar=('X'),
                          dest='jobs_patterns')
    optional.add_argument('--jobs-indices', nargs='+', type=int,
                          default=[],
                          help=dedent('''\
                          For expert users. Used to filter the jobs of interest in the
                          chosen pipeline using integers. All jobs are run by default.
                          If --jobs-indices and --jobs-patterns are both specified,
                          then --jobs-patterns takes precedence.
                           
                          (valid values: %(metavar)s>=1)
                          (default: %(default)s)
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar=('X'),
                          dest='jobs_indices')
    optional.add_argument('-v', '--verbose',
                          action='store_true',
                          help=dedent('''\
                          If set, the program will provide some additional details.
                           
                          (default: %(default)s)
                          ____________________________________________________________
                          '''),
                          dest='verbose')

    oargs = vars(parser.parse_known_args(iargs)[0])

    # Hack: when (nargs=1) a list should not be returned
    for k in ['exe', 'stage', 'fmri', 'out_dir', 'verbose']:
        if type(oargs[k]) is list:
            oargs[k] = oargs[k][0]

    return oargs


def check_iargs(iargs):
    """Checks the integrity of the input arguments and returns the options if successful
    """

    oargs = check_iargs_parser(iargs)
    oargs = setup_abspath(oargs)
    oargs['pipe_file'] = get_pipe_file(oargs['out_dir'], oargs['fmri'])
    check_iargs_integrity(oargs)
    return oargs


def run(iargs):
    """Main function, checks the inputs and runs a SPARK sub-pipeline
    """

    run_pipe(check_iargs(iargs))

    return sys_exit(0)


# Main
if __name__ == "__main__":
    run(argv[1:])
