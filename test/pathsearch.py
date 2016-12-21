'''
Created on Dec 21, 2016

@author: Abelit
'''

from oswatch import pathget
from config import dbconfig

if __name__ == '__main__':
    filepath = pathget.get_filepath("test.py")
    print(filepath)

