# -*- coding: utf-8 -*-
"""
Created on Mon Feb 25 17:45:22 2019

@author: Administrator
"""

from hdfs3 import HDFileSystem
from fastparquet import ParquetFile

IP = '192.168.48.71'
filename = 'file:///user/hdfs/tmp/source/20190225/fdm_audit_type/201902250000/8f2c29e0-5e06-4fab-9026-f7f10cdfe0b2/part-r-00000-f626ba57-1f98-499f-a109-f2c5bfd83f8f.snappy.parquet'


hdfs = HDFileSystem(host=IP, port=50070)
sc = hdfs.open

pf = ParquetFile(filename, open_with=sc)
df = pf.to_pandas()
