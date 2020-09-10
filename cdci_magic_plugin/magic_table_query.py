

from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy
import os

# Dependencies
# eg numpy
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f



# Project
# relative import eg: from .mod import f
import  numpy as np
import pandas as pd
from astropy.table import Table
import  json
from pathlib import Path
from astropy.io import ascii
import base64

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
import pickle



from astropy.io import fits as pf
from cdci_data_analysis.analysis.io_helper import FitsFile
from cdci_data_analysis.analysis.queries import ProductQuery
from cdci_data_analysis.analysis.products import BaseQueryProduct,QueryProductList,QueryOutput
from cdci_data_analysis.analysis.io_helper import FilePath
from oda_api.data_products import NumpyDataProduct,NumpyDataUnit,BinaryData
from cdci_data_analysis.configurer import DataServerConf

from .magic_dataserver_dispatcher import MAGICDispatcher
from .magic_dataserver_dispatcher import  MAGICAnalysisException




class AstropyTable(object):
    def __init__(self,
                 name,
                 table,
                 meta_data,
                 src_name):

        self.src_name=src_name
        self.name=name
        self._table=table
        self.meta_data=meta_data






    @property
    def table(self):
        return self._table


    def decode(self,enc_table):
        pass




    def encode(self, use_binary=False, to_json=False):

        _o_dict = {}
        _o_dict['binary'] = None
        _o_dict['ascii'] = None

        if use_binary is True:
            _binarys = base64.b64encode(pickle.dumps(self.table, protocol=2)).decode('utf-8')
            _o_dict['binary'] = _binarys
        else:
            fh = StringIO()
            self.table.write(fh, format='ascii.ecsv')
            _text = fh.getvalue()
            fh.close()
            _o_dict['ascii'] = _text

        _o_dict['name'] = self.name
        _o_dict['meta_data'] = json.dumps(self.meta_data)

        if to_json == True:
            _o_dict = json.dumps(_o_dict)
        return _o_dict


    def write(self,name,format='fits',overwrite=True):
        self._table.write(name,format=format,overwrite=overwrite)

    @classmethod
    def from_ecsv_file(cls, file_name):
        return cls.from_table(Table.read(file_name, format='ascii.ecsv'))


    @classmethod
    def from_fits_file(cls,file_name):
        return cls.from_table(Table.read(file_name,format='fits'))

    @classmethod
    def from_file(cls,file_name):
        format_list=['ascii.ecsv','fits']
        cat=None
        for f in format_list:
            try:
                cat= cls.from_table(Table.read(file_name,format=f))
            except:
                pass

        if cat is None:
            raise RuntimeError('file format for catalog not valid')
        return cat



class MAGICTable(AstropyTable):
    def __init__(self,
                 name,
                 table,
                 src_name='None',
                 meta_data={}):


        if meta_data == {} or meta_data is None:
            self.meta_data = {'product': 'MAGIC_TABLE', 'instrument': 'MAGIC', 'src_name': src_name}
        else:
            self.meta_data = meta_data

        #self.meta_data['time'] = 'time'
        #self.meta_data['rate'] = 'rate'
        #self.meta_data['rate_err'] = 'rate_err'



        super(MAGICTable, self).__init__(name=name,
                                          table=table,
                                          src_name=src_name,
                                          meta_data=meta_data)


    @classmethod
    def build_from_res(cls,res):


        MWL_files=[]
        MAGIC_files=[]
        #for


        prod_list = []

        #if out_dir is None:
        #    out_dir = './'

        #if prod_prefix is None:
        #    prod_prefix=''

        _o_dict = json.loads(res.json())

        for _kw in ['MAGIC_files', 'MWL_files']:
            for p in _o_dict[_kw]:
                print('p ->', p)
                t_rec = ascii.read(_o_dict[_kw][p]['astropy_table']['ascii'])
                t_rec.meta['paper_id']=_o_dict[_kw][p]['paper_id']
                print('->',t_rec,_o_dict['src_name'],t_rec.meta)
                magic_table = cls(name='magic_table', table=t_rec, src_name=_o_dict['src_name'], meta_data=t_rec.meta)

                prod_list.append(magic_table)

        return prod_list






class MAGICTableQuery(ProductQuery):

    def __init__(self, name):

        super(MAGICTableQuery, self).__init__(name)

    def build_product_list(self, instrument, res, out_dir, prod_prefix='polar',api=False):

        #delta_t = instrument.get_par_by_name('time_bin')._astropy_time_delta.sec
        print('-> res',res.json())
        prod_list = MAGICTable.build_from_res(res)

        # print('spectrum_list',spectrum_list)

        return prod_list


    def get_data_server_query(self, instrument,config=None):

        #scwlist_assumption, cat, extramodules, inject=OsaDispatcher.get_osa_query_base(instrument)
        #E1=instrument.get_par_by_name('E1_keV').value
        #E2=instrument.get_par_by_name('E2_keV').value
        src_name = instrument.get_par_by_name('src_name').value
        #paper_id = instrument.get_par_by_name('paper_id').value
        #T1=instrument.get_par_by_name('T1')._astropy_time.unix
        #T2=instrument.get_par_by_name('T2')._astropy_time.unix
        #delta_t = instrument.get_par_by_name('time_bin')._astropy_time_delta.sec
        param_dict=self.set_instr_dictionaries(target_name=src_name)

        #print ('build here',config,instrument)
        q = MAGICDispatcher(instrument=instrument,config=config,param_dict=param_dict,task='api/v1.0/magic/search-by-name')

        return q


    def set_instr_dictionaries(self, target_name,paper_id=None):
        return  dict(
            target_name=target_name,
            paper_id=paper_id,
            get_products=True
        )


    def process_product_method(self, instrument, prod_list,api=False):

        _names = []
        _table_path = []
        _html_fig = []

        _data_list=[]
        _binary_data_list=[]
        for query_prod in prod_list.prod_list:
            #print('->name',query_lc.name)
            #query_lc.add_url_to_fits_file(instrument._current_par_dic, url=instrument.disp_conf.products_url)
            print('query_prod',vars(query_prod))
            query_prod.write()

            #if api == False:
            #    _names.append(query_lc.name)
            #    _lc_path.append(str(query_lc.file_path.name))
            #    if query_lc.root_file_path is not None:
            #        _root_path.append(str(query_lc.root_file_path.name))
            #    #print ('_root_path',_root_path)
            #    #x_label='MJD-%d  (days)' % mjdref,y_label='Rate  (cts/s)'
            #    _html_fig.append(query_lc.get_html_draw(x=query_lc.data.data_unit[1].data['time'],
            #                                            y=query_lc.data.data_unit[1].data['rate'],
            #                                            dy=query_lc.data.data_unit[1].data['rate_err'],
            #                                            title='Start Time: %s'%instrument.get_par_by_name('T1')._astropy_time.utc.value,
            #                                            x_label='Time  (s)',
            #                                            y_label='Rate  (cts/s)'))

            if api==False:
                _names.append(query_prod.meta_data['src_name'])
                _table_path.append(str(query_prod.file_path.name))
                _html_fig.append(query_prod.get_html_draw())

            if api==True:
                _data_list.append(query_prod.encode(use_binary=False))
                #try:
                #    open(root_file_path.path, "wb").write(BinaryData().decode(res_json['root_file_b64']))
                #    lc.root_file_path = root_file_path
                #except:
                #    pass
                #if query_lc.root_file_path is not None:
                #    _d,md=BinaryData(str(query_lc.root_file_path)).encode()
                #    _binary_data_list.append(_d)

        query_out = QueryOutput()

        if api == True:
            query_out.prod_dictionary['astropy_table_product_ascii_list'] = _data_list
            #query_out.prod_dictionary['binary_data_product_list'] = _binary_data_list
        else:
            query_out.prod_dictionary['name'] = _names
            query_out.prod_dictionary['file_name'] = _table_path
            query_out.prod_dictionary['image'] = _html_fig
            query_out.prod_dictionary['download_file_name'] = 'magic_table.fits.gz'

        query_out.prod_dictionary['prod_process_message'] = ''


        return query_out









