

from __future__ import absolute_import, division, print_function

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


from cdci_magic_plugin import conf_file, conf_dir

from cdci_data_analysis.analysis.queries import *
from cdci_data_analysis.analysis.instrument import Instrument
from .magic_dataserver_dispatcher import MAGICDispatcher

from .magic_table_query import MAGICTableQuery


def common_instr_query():
   # not exposed to frontend
   # TODO make a special class
   # max_pointings=Integer(value=50,name='max_pointings')

    E1_keV = SpectralBoundary(value=0., E_units='keV', name='E1_keV')
    E2_keV = SpectralBoundary(value=10000., E_units='keV', name='E2_keV')
    spec_window = ParameterRange(E1_keV, E2_keV, 'spec_window')
    instr_query_pars = [spec_window]

    return instr_query_pars


def magic_factory():
    print('--> MAGIC Factory')
    src_query = SourceQuery('src_query')

    instr_query_pars = common_instr_query()

    instr_query = InstrumentQuery(name='magic_parameters',
                                  extra_parameters_list=None,
                                  input_prod_list_name=None,
                                  input_prod_value=None,
                                  catalog=None,
                                  catalog_name='user_catalog')

    magic_table_query = MAGICTableQuery('magic_table_query')

    query_dictionary = {}
    query_dictionary['magic_table'] = 'magic_table_query'
    # query_dictionary['update_image'] = 'update_image'

    print('--> conf_file', conf_file)
    print('--> conf_dir', conf_dir)

    return Instrument('magic', asynch=False,
                      data_serve_conf_file=conf_file,
                      src_query=src_query,
                      instrumet_query=instr_query,
                      product_queries_list=[magic_table_query],
                      data_server_query_class=MAGICDispatcher,
                      query_dictionary=query_dictionary)

