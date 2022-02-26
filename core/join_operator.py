'''
Use this module for working on datasets.
Supports below opearations:
    - Creating Datasets
    - Joining datasets using joiner 
'''
from __future__ import (absolute_import, division, annotations,
                        print_function, unicode_literals)
from builtins import *

__version__ = '1.0'
__author__ = ['Animesh Srivastava']

from datetime import date, datetime
from collections import namedtuple
from itertools import product
from typing import List, Tuple, Union, NamedTuple

class Row:
    def __init__(self, header:List[str], row:tuple=None)->None:
        '''
        A wrapper on top of namedtuple to create a Row object.
            :param header - List of column names
            :param row - A tuple of row [optional]
        '''
        self.header = header
        self.schema = namedtuple("Row", [col_name.lower() for col_name in header])
        if row:
            self.row = self.schema(*row)
        else:
            self.row = self.schema(*[None]*len(self.header))
    
    def __repr__(self) -> str:
        return self.show()

    def show(self):
        string = "+ "
        field_size = [len(col) for col in self.header]
        for f_n, f_v, f_s in zip(self.row._fields, self.row, field_size):
            string += "| {f_n}={f_v!r:<{f_s}} |".format(f_n=f_n,f_v=f_v,f_s=f_s)
        return string+" +"

class DataSet:
    def __init__(self, header:List[str], rows:List[Tuple[Union[int, str, date, datetime, float]]]) -> None:
        '''
        A wraper on top of Row class to create a complete dataset.
            :param header - List of column names
            :param rows - List of tuples of rows
        '''
        self.header = header
        self.rows = rows
        self.data_set = self.__generate_data_set()
    
    def __generate_data_set(self)->List[NamedTuple]:
        '''
        Row object factory; generates a list of row objects

        '''
        row_set = []
        for row in self.rows:
            if not len(self.header) == len(row):
                raise Exception(f"Columns mismatched expected : [{len(self.header)}] actual : [{len(row)}]")
            else:
                row_set.append(Row(self.header, row))
        else:
            return row_set
    
    def size(self)->int:
        '''
        get the number of rows
        '''
        return len(self.data_set)
    
    def show(self)->None:
        '''
        Prints dataset
        '''
        for row in self.data_set:
            print(row.show())
        else:
            print()
    
    def count(self)->None:
        '''
        Prints count
        '''
        print(self.size())


def joiner(left_data_set:DataSet, right_data_set:DataSet, on:List[str])->DataSet:
    '''
    Performs inner join on datasets
        :param left_data_set - dataset with left data
        :param right_data_set - dataset with right data
        :param on - join on key 

    Returns -> joined DataSet 
    '''

    left_rows = left_data_set.data_set
    right_rows = right_data_set.data_set
    
    data_set = {}
    joined_set = []
        
    # form all the right rows
    for left_row in left_rows:
        left_key = "".join([str(getattr(left_row.row, key)) for key in on])
        data_set.setdefault(left_key, ([],[]))[0].append(left_row.row)

    # form all the left rows 
    for right_row in right_rows:
        right_key = "".join([str(getattr(right_row.row, key)) for key in on])
        data_set.setdefault(right_key, ([],[]))[1].append(right_row.row)
    
    # logic for inner join
    for ds_cols in data_set.values():
        for left_cols, right_cols in product(*ds_cols):
            joined_set.append(left_cols+right_cols)

    result_headers = list(left_rows[0].row._fields) + [col+"_" for col in right_rows[0].row._fields]

    return DataSet(result_headers, joined_set)