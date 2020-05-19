#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Entrypoint for SPARK analyses using a MATLAB generated
# standalone application.
#
# Last revision: May, 2020
# Maintainer: Obai Bin Ka'b Ali @aliobaibk
# License: In the app folder or check GNU GPL-3.0.


import os
from sys import argv, stderr
from sys import exit as sys_exit
from textwrap import dedent

from spark.setup import setup
from spark.run import run
from spark.wrapup import wrapup


def show_help():
    """Generates help and usage messages for this program
    """

    print(dedent('''\
        usage: spark.py --SETUP ... [--exe XXX]
               OR
               spark.py --RUN ... [--exe XXX]
               OR
               spark.py --WRAP-UP ... [--exe XXX]

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

          REQUIRED arguments:
          __________________________________________________________________________________

          --SETUP ...           Sets up SPARK pipelines. See --SETUP --help for more info.
                                --SETUP and all other arguments are mutually exclusive.
                                ____________________________________________________________
          --RUN ...             Runs a SPARK sub-pipeline. See --RUN --help for more info.
                                --RUN and all other arguments are mutually exclusive.
                                ____________________________________________________________
          --WRAP-UP ...         Wraps-up a SPARK analysis. See --WRAP-UP --help for more
                                info.
                                --WRAP-UP and all other arguments are mutually exclusive.
                                ____________________________________________________________

          OPTIONAL arguments:
          __________________________________________________________________________________

          --exe XXX             Path (absolute or relative) to the MATLAB generated 
                                standalone application.
                                 
                                (default: dirname(_THIS_FILE_)/spark.samapp)
                                (type: str)
                                ____________________________________________________________
        '''))

    return sys_exit(0)


def get_default_exe():
    """Default path to the MATLAB generated standalone application
    """

    return os.sep.join([os.path.dirname(os.path.realpath(__file__)), 'spark_samapp'])


def spark(iargs):
    """Main function
    """

    iargs = ['--exe', get_default_exe()] + iargs
    do_setup = '--SETUP' in iargs
    do_run = '--RUN' in iargs
    do_wrapup = '--WRAP-UP' in iargs
    if sum([do_setup, do_run, do_wrapup]) > 1:
        print('--SETUP, --RUN and --WRAP-UP are mutually exclusive arguments, only specify one of them.\n' +
              'For more info, rerun the program with no argument.', file=stderr)
        sys_exit(1)
    elif do_setup:
        setup(iargs)
    elif do_run:
        run(iargs)
    elif do_wrapup:
        wrapup(iargs)
    else:
        show_help()

    return sys_exit(0)


# Main
if __name__ == "__main__":
    spark(argv[1:])
