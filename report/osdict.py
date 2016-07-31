#coding:utf-8
import json
import time
def vs_dict():
	#import json file which store the configuration of basic information
	json_dict=json.load(open('osinfo.json','r'))

	#define global dict variable
	vs_dict={
	    'vs_month':time.ctime(),
	}

	#merge dict json_dict on dict vs_dict
	vs_dict=dict(vs_dict,**json_dict)
	#return dict vs_dict
	return vs_dict