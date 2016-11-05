import datetime
import os
import sys
import time
from pprint import pprint

def readable(size):
    si=('B','KB','MB','GB','TB', 'PB', 'EB', 'ZB', 'YB')
    div = [n for n, m in enumerate(si) if pow(1024, n+1)>size][0]
    return "%.1f%s"%(size/float(pow(1024, div)), si[div])

total = {"log":0, "trace":0}
for path, dirs, files in os.walk(sys.argv[1]):
    for f in files:
        filepath = path+os.sep+f
        if os.stat(filepath).st_mtime>time.time()-(3600*24*int(sys.argv[2])):
            size = readable(os.path.getsize(filepath))
            age = datetime.datetime.fromtimestamp(os.stat(filepath).st_mtime)
            if f in ("log.xml", "alert_gzgszxk1.log", "listener.log"):
                filetype = "log"
            elif f.endswith("trc") or f.endswith("trm"):
                filetype = "trace"
            else:
                filetype = None
            if filetype:
                total[filetype] += os.path.getsize(filepath)


for a, b in total.items():
    total[a] = readable(b)

pprint(total)