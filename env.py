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
import sys
from genericpath import isfile

# define project name
PROJECT_NAME = '__oswatch__.py'

# Get fullpath of the file.
def get_fullpath(filename):
    # Get the separator sign of the os, such as '/' in linux/unix or '\' in windows
    separator = os.sep
    path = os.getcwd()
    path = path.split(separator)
    while len(path) > 0:
        filepath = separator.join(path)+separator+filename
        leng = len(path)
        if os.path.exists(filepath):
            return os.path.dirname(filepath)
        path.remove(path[leng-1])
     
def work_allpath(path):
    allpath = []
    # os.walk to search all path under the file or dir you want.
    for dirpath,dirnames,filenames in os.walk(path):
        for file in dirnames:
            fullpath = os.path.join(dirpath,file)
            allpath.append(fullpath)
    return allpath

def add_envpath(path):
    # Add package folder to searching path
    # Search the dir that contains file '__init__.py'
    for dirname in work_allpath(path):
        if isfile(dirname+os.sep+'__init__.py') and dirname not in sys.path:
            sys.path.append(dirname) 

if __name__ == '__main__':
    print(get_fullpath(PROJECT_NAME))
    print(work_allpath(get_fullpath(PROJECT_NAME)))
    add_envpath(get_fullpath(PROJECT_NAME))
    print(sys.path)  

    
    