#!/usr/bin/python
## -*- coding: utf-8 -*-

#Function:Auto generate reports for Oracle Server based on latex file
#Description:
#Author:Abelit
#Email:ychenid@live.com
#Date:2016-04-11

#Import modules
import os,sys
import re
import time
import json
import osdict

#Read template file
# def file_handler(infile='oswatch.tex',outfile='osreport.tex'):
#     print(osdict.vs_dict())
#     try:
#         #Read and write file
#         input_file=open(infile,'r')
#         output_file=open(outfile,'w')

#         lines=input_file.read()
#         for key in osdict.vs_dict():
#             lines=lines.replace(key,osdict.vs_dict()[key])
#         output_file.write(lines)
#         print("File has been Handled successfully!")
#     except IOError:
#         print("Failed to read file,please recheck whether the file you input exists.")
#     finally:
#         #Release resouce
#         output_file.close()
#         input_file.close()
    
class FileHandler:
    def __init__(self,infile,outfile):
        self.infile=infile
        self.outfile=outfile

    def reader(self):
        #global lines
        input_file=open(self.infile,'r',encoding='utf-8')
        lines=input_file.read()
        input_file.close()
        return lines

    def file_handler(self):
        #Get lines dict from function reader
        lines=self.reader()
        for key in osdict.vs_dict():
            lines=lines.replace(key,osdict.vs_dict()[key])
        return lines


    def writer(self):
        output_file=open(self.outfile,'w',encoding='utf-8')
        lines=self.file_handler()
        output_file.write(lines)
        output_file.close()

    def show(self,src_file,dst_file):
        lines=self.reader()
        print(lines)




