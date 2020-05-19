#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Wraps-up a SPARK analysis
#
# Last revision: May, 2020
# Maintainer: Obai Bin Ka'b Ali @aliobaibk
# License: In the app folder or check GNU GPL-3.0.


from argparse import ArgumentParser, RawTextHelpFormatter
import os
from re import search, sub
from shutil import move
from sys import argv, stderr
from sys import exit as sys_exit
from textwrap import dedent


def move_outputs(out_dir, pipe_file):
    """Moves raw outputs to the root output directory --out-dir
    """

    src_dir = os.sep.join([out_dir, get_bids_filename(pipe_file)])
    for (root, dirs, files) in os.walk(src_dir, topdown=False):
        dst_dir = os.path.join(out_dir, os.path.relpath(root, src_dir))
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        for f in files:
            os.rename(os.path.join(root, f), os.path.join(dst_dir, f))
        for dirname in dirs:
            os.rmdir(os.path.join(root, dirname))
    os.rmdir(src_dir)

    return None


def get_spark_filename(pipe_file):
    """Extracts filename used by SPARK for naming raw outputs
    """

    filename = ''
    with open(pipe_file, 'r', newline='\n') as file:
        for line in file:
            if line.startswith('fmri_data '):
                filename = '_'.join(line.split(' ')[1: 4])
                break

    if not filename:
        print('Failed to read option fmri_data from the pipeline file:\n' + pipe_file,
              file=stderr)
        sys_exit(1)

    return filename


def get_bids_filename(pipe_file):
    """Extracts (BIDS) filename used by SPARK for naming final outputs
    """

    return os.path.splitext(os.path.basename(pipe_file))[0]


def rename_outputs(out_dir, pipe_file):
    """Rename the raw outputs of SPARK using the input fMRI filename
    """

    bids_filename = get_bids_filename(pipe_file)
    spark_filename = get_spark_filename(pipe_file)

    for (root, _, files) in os.walk(os.sep.join([out_dir, bids_filename])):
        rename_root = False
        for name in files:
            if search('.+' + spark_filename + '.+', name):
                new_name = sub('_' + spark_filename, '_' + bids_filename, name)
                move(os.sep.join([root, name]), os.sep.join([root, new_name]))
                rename_root = True
        if rename_root and (os.path.basename(root) == spark_filename):
            move(root, os.sep.join([os.path.dirname(root), bids_filename]))

    return None


def check_iargs_integrity(iargs):
    """Integrity of the input arguments
    """

    # Pipeline
    if not os.path.isfile(iargs['pipe_file']):
        print('Pipeline file not found:\n' + iargs['pipe_file'], file=stderr)
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
    return os.sep.join([out_dir, filename, 'pipelines', filename + '.opt'])


def setup_abspath(iargs):
    """Makes sure all paths are absolute.
    """

    iargs['fmri'] = os.path.abspath(iargs['fmri'])
    iargs['out_dir'] = os.path.abspath(iargs['out_dir'])

    return iargs


def check_iargs_parser(iargs):
    """[For wrapping-up SPARK] Defines the possible arguments of the program, generates help
    and usage messages, and issues errors in case of invalid arguments.
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
    required.add_argument('--WRAP-UP',
                          action='store_true',
                          required=True,
                          help='\n____________________________________________________________')
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
                          This directory should have been previously populated with
                          --SETUP and --RUN and at least contain the pipeline '.opt'
                          file corresponding to the input --fmri.

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
    optional.add_argument('--move-outputs',
                          action='store_true',
                          help=dedent('''\
                          If set, all outputs for this analysis will be moved to the
                          specified output directory --out-dir.
                          This flag is useful to merge in a single directory the
                          results of different analyses where the input fMRI were
                          different.
                           
                          (default: %(default)s)
                          ____________________________________________________________
                          '''),
                          dest='move_outputs')
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
    for k in ['fmri', 'out_dir', 'move_outputs', 'verbose']:
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


def wrapup(iargs):
    """Main function, checks the inputs, makes the necessary files to wrap-up SPARK
    """
    oargs = check_iargs(iargs)

    rename_outputs(oargs['out_dir'], oargs['pipe_file'])

    if oargs['move_outputs']:
        move_outputs(oargs['out_dir'], oargs['pipe_file'])

    return sys_exit(0)


# Main
if __name__ == "__main__":
    wrapup(argv[1:])
