# encoding: utf-8
'''
@project: __oswatch__
@modules: oswatch.pathget
@description:
    
@created:Aug 2, 2016

@author: abelit
@email: ychenid@live.com

@licence: GPL

'''
import os
    
def get_filepath(filename):
    separator = os.sep
    path = os.getcwd()
    path = path.split(separator)
    while len(path) > 0:
        filepath = separator.join(path)+separator+filename
        leng = len(path)
        if os.path.exists(filepath):
            return os.path.dirname(filepath)
        path.remove(path[leng-1])
    