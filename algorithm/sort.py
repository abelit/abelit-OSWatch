# -*- coding: utf-8 -*-
#python插入排序
def insertSort(a):
  for i in range(len(a)-1):
    #print a,i 
    for j in range(i+1,len(a)):
      if a[i]>a[j]:
        temp = a[i]
        a[i] = a[j]
        a[j] = temp
  return a
 
#Python的冒泡排序  
def bubbleSort(alist):
  for passnum in range(len(alist)-1,0,-1):
    #print alist,passnum
    for i in range(passnum):
      if alist[i]>alist[i+1]:
        temp = alist[i]
        alist[i] = alist[i+1]
        alist[i+1] = temp
  return alist
 
#Python的选择排序 
def selectionSort(alist):
  for i in range(len(alist)-1,0,-1):
    maxone = 0
    for j in range(1,i+1):
      if alist[j]>alist[maxone]:
        maxone = j
    temp = alist[i] 
    alist[i] = alist[maxone]
    alist[maxone] = temp 
  return alist
 
alist = [54,26,93,17,77,31,44,55,20]
print(bubbleSort(alist))
alist = [54,26,93,17,77,31,44,55,20]
print(selectionSort(alist))