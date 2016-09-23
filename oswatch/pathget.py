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

class PathGet(object):
    """docstring for PathGet"""
    def __init__(self, filename):
        super(PathGet, self).__init__()
        self.filename = filename
    
    def get_filepath(self):
        separator = os.sep
        path = os.getcwd()
        path = path.split(separator)
        while len(path) > 0:
            filepath = separator.join(path)+separator+self.filename
            leng = len(path)
            if os.path.exists(filepath):
                return os.path.dirname(filepath)
            path.remove(path[leng-1])
   

if __name__ == '__main__':
    print(PathGet('__oswatch__.py').get_filepath())
    