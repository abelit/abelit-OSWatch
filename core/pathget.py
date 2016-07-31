# encoding: utf-8
'''
Created on Jul 31, 2016
@modules: database.pathget

@author: abelit
@email: ychenid@live.com
@description:

@licence: GPL

'''

import os

class PathGet(object):
    """docstring for PathGet"""
    def __init__(self, file):
        super(PathGet, self).__init__()
        self.file = file
    
    def get_fullpath(self):
        separator = os.sep
        path = os.getcwd()
        path = path.split(separator)
        while len(path) > 0:
            fpath = separator.join(path)+separator+self.file
            leng = len(path)
            if os.path.exists(fpath):
                return os.path.dirname(fpath)
            str.remove(str[leng-1])
   

if __name__ == '__main__':
    print(PathGet('__oswatch__.py').get_fullpath())