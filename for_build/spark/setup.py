#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Sets up SPARK
#
# Last revision: May, 2020
# Maintainer: Obai Bin Ka'b Ali @aliobaibk
# License: In the app folder or check GNU GPL-3.0.


from argparse import ArgumentParser, RawTextHelpFormatter
from bids_validator import BIDSValidator
from errno import EEXIST
import os
from re import sub
from shlex import quote
from subprocess import run as sp_run
from sys import argv, stderr
from sys import exit as sys_exit
from textwrap import dedent


def make_dirs(dir_path):
    """Creates a directory
    """

    try:
        os.makedirs(dir_path)
    except OSError as e:
        if e.errno == EEXIST:
            print('Old files might get replaced in the already existing directory:\n' +
                  dir_path + '\n', file=stderr)
        else:
            print('Failed to create the directory:\n' +
                  dir_path + '\n' + str(e), file=stderr)
            sys_exit(1)

    return None


def setup_pipes(iargs):
    """Builds the list of options for running the SPARK analyses with GNU Octave or MATLAB and 
    creates the corresponding full SPARK pipeline files.
    """

    out_dir = os.sep.join([iargs['out_dir'], iargs['fmri'][0]])
    make_dirs(out_dir)

    pipes_dir = os.sep.join([out_dir, 'pipelines'])
    make_dirs(pipes_dir)

    pipe_opt = os.sep.join([pipes_dir, iargs['fmri'][0] + '.opt'])
    with open(pipe_opt, 'w', newline='\n') as file:
        file.write(
            'pipe_file ' + pipe_opt[:-4] + '.mat' + '\n' +
            'fmri_data ' + ' '.join(iargs['fmri'][1:]) + '\n' +
            'out_dir ' + out_dir + '\n' +
            'mask ' + iargs['mask'] + '\n' +
            'nb_resamplings ' + str(iargs['nb_resamplings']) + '\n' +
            'network_scales ' + ' '.join([str(x) for x in iargs['network_scales']]) + '\n' +
            'nb_iterations ' + str(iargs['nb_iterations']) + '\n' +
            'p_value ' + str(iargs['p_value']) + '\n' +
            'resampling_method ' + iargs['resampling_method'] + '\n' +
            'block_window_length ' + ' '.join([str(x) for x in iargs['block_window_length']]) + '\n' +
            'dict_init_method ' + iargs['dict_init_method'] + '\n' +
            'sparse_coding_method ' + iargs['sparse_coding_method'] + '\n' +
            'preserve_dc_atom ' + str(int(iargs['preserve_dc_atom'])) + '\n' +
            'verbose ' + str(int(iargs['verbose'])) + '\n'
        )

    if not os.path.isfile(pipe_opt):
        print('Failed to create/edit the SPARK pipeline options file:\n' +
              pipe_opt, file=stderr)
        sys_exit(1)

    cmd = '{} setup {}'.format(quote(iargs['exe']), quote(pipe_opt))
    p = sp_run(cmd, shell=True, cwd=pipes_dir)
    if p.returncode != 0:
        print('\n\nThe process returned a non-zero exit status:\n' +
              str(p.returncode), file=stderr)
        sys_exit(1)

    return None


def setup_fmri(fmri):
    """Builds the format subject/session/run from the provided BIDS-data
    """

    filename = os.path.basename(fmri)
    tokens = filename.split('_')

    if not tokens or not tokens[0].startswith('sub-'):
        print("Invalid BIDS file, the filename does not start with 'sub-X_':\n" +
              filename, file=stderr)
        sys_exit(1)
    elif not BIDSValidator().is_bids(os.sep.join([os.sep + tokens[0], 'func', filename])):
        print("Invalid BIDS file:\n" + filename, file=stderr)
        sys_exit(1)

    sub_id = sub(r'\W+', '_', tokens[0])

    if tokens[1].startswith('ses-'):
        ses_id = sub(r'\W+', '_', tokens[1])
    else:
        ses_id = 'ses_cspark_1'

    if tokens[-2].startswith('run-'):
        run_id = sub(r'\W+', '_', tokens[-2])
    else:
        run_id = 'run_cspark_1'

    return [filename[:-len(tokens[-1][4:])], sub_id, ses_id, run_id, fmri]


def check_iargs_integrity(iargs):
    """Integrity of the input arguments
    """

    # MATLAB executable
    if not os.path.isfile(iargs['exe']):
        print('--exe\n' +
              'Invalid or nonexistent file:\n' + iargs['exe'], file=stderr)
        sys_exit(1)

    # fMRI
    if not os.path.isfile(iargs['fmri']):
        print('--fmri\n' +
              'Invalid or nonexistent file:\n' + iargs['fmri'], file=stderr)
        sys_exit(1)

    # Grey-matter mask
    if not os.path.isfile(iargs['mask']):
        print('--mask\n' +
              'Invalid or nonexistent file:\n' + iargs['mask'], file=stderr)
        sys_exit(1)
    elif not (iargs['mask'].endswith('.mnc') or iargs['mask'].endswith('.nii')):
        print('--mask\n' +
              'File is not MINC (.mnc) or NIfTI (.nii):\n' + iargs['mask'], file=stderr)
        sys_exit(1)

    # Number of resamplings
    if iargs['nb_resamplings'] < 2:
        print('--nb-resamplings\n' +
              'Number of resamplings smaller than 2:\n' + str(iargs['nb_resamplings']), file=stderr)
        sys_exit(1)

    # Network scales
    if any(x < 1 for x in iargs['network_scales']):
        print('--network-scales\n' +
              'One element: [begin] [step] [end], is smaller than 1:\n' + str(iargs['network_scales']), file=stderr)
        sys_exit(1)
    elif iargs['network_scales'][2] < iargs['network_scales'][0]:
        print('--network-scales\n' +
              '[begin] is greather than [end]:\n' + str(iargs['network_scales']), file=stderr)
        sys_exit(1)

    # Number of iterations
    if iargs['nb_iterations'] < 2:
        print('--nb-iterations\n' +
              'Number of iterations smaller than 2:\n' + str(iargs['nb_iterations']), file=stderr)
        sys_exit(1)

    # P-value
    if (iargs['p_value'] < 0 or iargs['p_value'] > 1):
        print('--p-value\n' +
              'P-value not between 0 and 1:\n' + str(iargs['p_value']), file=stderr)
        sys_exit(1)

    # Block window length
    if any(x < 1 for x in iargs['block_window_length']):
        print('--block-window-length\n' +
              'One element: [begin] [step] [end], is smaller than 1:\n' + str(iargs['block_window_length']), file=stderr)
        sys_exit(1)
    elif iargs['block_window_length'][2] < iargs['block_window_length'][0]:
        print('--block-window-length\n' +
              '[begin] is greather than [end]:\n' + str(iargs['block_window_length']), file=stderr)
        sys_exit(1)

    return None


def setup_abspath(iargs):
    """Makes sure all paths are absolute.
    """

    iargs['fmri'] = os.path.abspath(iargs['fmri'])
    iargs['out_dir'] = os.path.abspath(iargs['out_dir'])
    iargs['mask'] = os.path.abspath(iargs['mask'])
    iargs['exe'] = os.path.abspath(iargs['exe'])

    return iargs


def check_iargs_parser(iargs):
    """[For setting up SPARK] Defines the possible arguments of the program, generates help
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
    required.add_argument('--SETUP',
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
                          Path (absolute or relative) to the output directory
                          (old files might get replaced).
                          By default, a new directory named after the specified input
                          --fmri is created relative this output directory --out-dir
                          to avoid conflicts. To change this default setting, use
                          --WRAP-UP --move-outputs (useful to merge results of
                          multiple analyses).
                           
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar=('XXX'),
                          dest='out_dir')
    required.add_argument('--mask', nargs=1, type=str,
                          required=True,
                          help=dedent('''\
                          Path (absolute or relative) to the grey-matter mask.
                           
                          (file formats: MINC, NIfTI)
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar=('XXX'),
                          dest='mask')

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
    optional.add_argument('--nb-resamplings', nargs=1, type=int,
                          default=100,
                          help=dedent('''\
                          Number of bootstrap resamplings at the individual level.
                           
                          (valid values: %(metavar)s>=2)
                          (default: %(default)s)
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar=('X'),
                          dest='nb_resamplings')
    optional.add_argument('--network-scales', nargs=3, type=int,
                          # The display below is hacked, sorry
                          default=[10, 2, 30],
                          help=dedent('''\
                          Three integers, respectively: [begin] [step] [end], used to
                          create a regularly-spaced vector. In order to specify a
                          single number, for instance '12', enter the same number for
                          [begin] and [end], as: '--network-scales 12 1 12'.
                          The numbers in the vector correspond to the range of network
                          scales to be tested. An optimal network scale will be
                          automatically estimated from the vector.
                           
                          (valid values: %(metavar)s>=1)
                          (default: 10 2 30)
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar=('X'),
                          dest='network_scales')
    optional.add_argument('--nb-iterations', nargs=1, type=int,
                          default=20,
                          help=dedent('''\
                          Number of iterations for the sparse dictionary learning.
                           
                          (valid values: %(metavar)s>=2)
                          (default: %(default)s)
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar=('X'),
                          dest='nb_iterations')
    optional.add_argument('--p-value', nargs=1, type=float,
                          default=0.05,
                          help=dedent('''\
                          Significance level, using a Z-test, for removing
                          inconsistent elements in the average sparse coefficients
                          (considered as Gaussian noise) after spatial clustering.
                           
                          (valid values: 0<=%(metavar)s<=1)
                          (default: %(default)s)
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar=('X'),
                          dest='p_value')
    optional.add_argument('--resampling-method', nargs=1, type=str,
                          choices=['CBB', 'AR1B', 'AR1G'],
                          default='CBB',
                          help=dedent('''\
                          Method (from NIAK) used to resample the data under the null
                          hypothesis.
                           
                          Note: If 'CBB' is selected, the option --block-window-length
                          is used.
                           
                          - CBB: Circular-block-bootstrap sample of multiple time
                          series.
                          - AR1B: Bootstrap sample of multiple time series based on a
                          semiparametric scheme mixing an auto-regressive temporal
                          model and i.i.d. bootstrap of the "innovations".
                          - AR1G: Bootstrap sample of multiple time series based on a
                          parametric model of Gaussian data with arbitrary spatial
                          correlations and first-order auto-regressive temporal
                          correlations.
                           
                          (valid values: %(choices)s)
                          (default: %(default)s)
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar='X',
                          dest='resampling_method')
    optional.add_argument('--block-window-length', nargs=3, type=int,
                          # The display below is hacked, sorry
                          default=[10, 1, 30],
                          help=dedent('''\
                          Three numbers, respectively: [begin] [step] [end], used to
                          create a regularly-spaced vector. In order to specify a
                          single number, for instance '12', enter the same number for
                          [begin] and [end], as: '--block-window-length 12 1 12'.
                          A number in the vector corresponds to a window length used
                          in the circular block bootstrap. The unit of the window
                          length is ‘time-point’ with each time-point indicating a 3D
                          scan at each TR. If the vector contains multiple numbers,
                          then a number will be randomly selected from it at each
                          resampling.
                           
                          It is recommended to use window lengths greater or equal to
                          sqrt(T), where T is the total number of time points in the
                          fMRI time-course. It is also recommended to randomize the
                          window length used at each resampling to reduce a bias by
                          window size.
                           
                          (valid values: %(metavar)s>=1)
                          (default: 10 1 30)
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar='X',
                          dest='block_window_length')
    optional.add_argument('--dict-init-method', nargs=1, type=str,
                          choices=['GivenMatrix', 'DataElements'],
                          default='GivenMatrix',
                          help=dedent('''\
                          If 'GivenMatrix' is selected, then the dictionary will be
                          initialized by a random permutation of the raw data obtained
                          in step 1.
                          If 'DataElements' is selected, then the dictionary will be
                          initialized by the first N (number of atoms) columns in the
                          raw data obtained in step 1.
                           
                          (valid values: %(choices)s)
                          (default: %(default)s)
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar='X',
                          dest='dict_init_method')
    optional.add_argument('--sparse-coding-method', nargs=1, type=str,
                          choices=['OMP', 'Thresholding'],
                          default='Thresholding',
                          help=dedent('''\
                          Sparse coding method for the sparse dictionary learning.
                           
                          (valid values: %(choices)s)
                          (default: %(default)s)
                          (type: %(type)s)
                          ____________________________________________________________
                          '''),
                          metavar='X',
                          dest='sparse_coding_method')
    optional.add_argument('--preserve-dc-atom',
                          action='store_true',
                          help=dedent('''\
                          If set, then the first atom will be set to a constant and
                          will never change, while all the other atoms will be trained
                          and updated.
                           
                          (default: %(default)s)
                          ____________________________________________________________
                          '''),
                          dest='preserve_dc_atom')
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
    for k in ['exe', 'fmri', 'out_dir', 'mask',
        'nb_resamplings', 'nb_iterations', 'p_value',
        'resampling_method', 'dict_init_method', 'sparse_coding_method', 'preserve_dc_atom', 'verbose']:
        if type(oargs[k]) is list:
            oargs[k] = oargs[k][0]

    return oargs


def check_iargs(iargs):
    """Checks the integrity of the input arguments and returns the options if successful
    """

    oargs = check_iargs_parser(iargs)
    oargs = setup_abspath(oargs)
    check_iargs_integrity(oargs)
    oargs['fmri'] = setup_fmri(oargs['fmri'])
    return oargs


def setup(iargs):
    """Main function, checks the inputs and sets up SPARK
    """

    setup_pipes(check_iargs(iargs))

    return sys_exit(0)


# Main
if __name__ == "__main__":
    setup(argv[1:])
