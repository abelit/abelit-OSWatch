# encoding: utf-8
'''
@project: __oswatch__
@modules: core.pathget
@description:
    
@created:Aug 2, 2016

@author: abelit
@email: ychenid@live.com

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
            path.remove(path[leng-1])
   

if __name__ == '__main__':
    print(PathGet('__oswatch__.py').get_fullpath())