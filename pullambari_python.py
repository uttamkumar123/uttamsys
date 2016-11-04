#!/usr/local/bin/python2.7

import os,re, requests, json, time, socket, subprocess, sys, string
from multiprocessing.pool import ThreadPool
from urllib2 import urlopen
import argparse


def build_arg_parser():
    """
    Builds a standard argument parser with arguments for taking inputs for ambari rest api url and host_component
    -u url
    -m metric_name
    -s hostnames <not mandatory based on your requirement>
    """
    parser = argparse.ArgumentParser(
        description='Standard Arguments for pullimg metrics via Ambari REST API')

    parser.add_argument('-l', '--url',
                        required=True,
                        action='store',
                        help='Ambari Rest API url')
     
    parser.add_argument('-m', '--metric',
                        required=True,
                        action='store',
                        help='metric of host_component')

    parser.add_argument('-s', '--hostname',
                        required=False,
                        action='store',
                        help='List of hostname to be entered in case of master service metric retrival like namenode, for e.g., for namenode, both hosts of namenode seperated by comma')

    return parser

def getmetric(url):
    try:
        headers = {'X-Requested-By': 'ambari'}
        r = requests.get(url, auth=('<ambari user id>', '<ambari user password>'), headers=headers, verify=False)
        responseObj = json.loads(r.text)
        text = json.dumps(responseObj, sort_keys=False, indent=4)
        mstart = 0
        nestedhash3 = ""
        nestedhash2 = ""
        nestedhash1 = ""
        array = ""
        mname = ""
        host = ""
        cluster = ""
        comp = ""
        mvalue = 0
        for linee in text.splitlines():
            line = linee.strip()
            if re.match( "\"host_name\"\s*\:\s*\"(\w*\d*\.\w*\.\w*\.\w*)\",$", line, re.I):
               hfield = line.split(" ")
               host = hfield[1].replace("\"", "").replace(".gid.gap.com", "").replace(",", "")

            if re.match( "\"cluster_name\"\s*\:\s*\"(\w*)\",$", line, re.I):
               cfield = line.split(" ")
               cluster = cfield[1].replace("\"", "").replace(",", "")

            if re.match( "\"component_name\"\s*\:\s*\"(\w*)\"$", line, re.I):
               comfield = line.split(" ")
               comp = comfield[1].replace("\"", "").replace(",", "")

            if re.match( "\"metrics\":\s*\{$", line, re.I):
               mstart = 1;
               continue
            elif mstart == 0:
               continue
            
            if re.match( "\"\w*\":\s*\{$", line, re.I):
               if nestedhash2:
                  field = line.split(" ")
                  nestedhash3 = field[0].replace(":", "").replace("\"", "")
               elif nestedhash1:
                  field = line.split(" ")
                  nestedhash2 = field[0].replace(":", "").replace("\"", "")
               elif array:
                  field = line.split(" ")
                  nestedhash1 = field[0].replace(":", "").replace("\"", "")
               else:
                  field = line.split(" ")
                  array = field[0].replace(":", "").replace("\"", "")
            elif re.match( "^\},?$", line, re.I):
               nestedhash3 = ""
               nestedhash2 = ""
               nestedhash1 = ""
               array = ""  
            elif re.match("\"(\w*)\":\s(\d*\.?\d*),?$", line, re.I) or re.match( "\"(\w*)\"\s*:\s(\d*\.?\d*\D*\d*),?$", line, re.I):
               field = line.split(" ")
               mname = field[0].replace(":", "").replace("\"", "")
               mvalue = field[1].replace(",","")
               if nestedhash3:
                  metric_names = str(""+array+"-"+nestedhash1+"-"+nestedhash2+"-"+nestedhash3+"-"+mname+"")
                  metric = str('PUTVAL "%s/%s_%s_%s/gauge-curdepth" N:%s' %(host, cluster, comp, metric_names, mvalue))
                  print metric
               elif nestedhash2:
                  metric_names = str(""+array+"-"+nestedhash1+"-"+nestedhash2+"-"+mname+"")
                  metric = str('PUTVAL "%s/%s_%s_%s/gauge-curdepth" N:%s' %(host, cluster, comp, metric_names, mvalue))          
                  print metric
               elif nestedhash1:
                  metric_names = str(""+array+"-"+nestedhash1+"-"+mname+"")
                  metric = str('PUTVAL "%s/%s_%s_%s/gauge-curdepth" N:%s' %(host, cluster, comp, metric_names, mvalue))
                  print metric
               elif array:
                  metric_names = str(""+array+"-"+mname+"")
                  metric = str('PUTVAL "%s/%s_%s_%s/gauge-curdepth" N:%s' %(host, cluster, comp, metric_names, mvalue))
                  print metric

    except Exception as e:
        print e


hosts = ""
PARSER = build_arg_parser()
MY_ARGS = PARSER.parse_args()
urll = MY_ARGS.url
metric_name = MY_ARGS.metric
hosts = MY_ARGS.hostname

#Removing the lock if previous run got into an issue, after one min to unblock the script run so that metric gets populated again
one_hour_ago = time.time() - 60 
somefile = "/var/run/pullambari_python_"+metric_name+".lock"

if os.path.exists("/var/run/pullambari_python_"+metric_name+".lock"):
   st=os.stat("/var/run/pullambari_python_"+metric_name+".lock")
   mtime=st.st_mtime
   if mtime < one_hour_ago:
      print('remove %s'%somefile)
      os.unlink("/var/run/pullambari_python_"+metric_name+".lock")


#Creating the lock file and checking if any previous lock file present
if os.path.isfile("/var/run/pullambari_python_"+metric_name+".lock"):
    print "Another instance of the script is still running or the lock file /var/run/pullambari_python_"+metric_name+".lock still exist...Exiting"
    sys.exit()
cmd1 = "touch /var/run/pullambari_python_"+metric_name+".lock"
q = subprocess.Popen(cmd1, stdout=subprocess.PIPE, shell=True)
(output1, err1) = q.communicate()

if hosts is None:
   host = []
   cmd = "curl -s --globoff -H 'X-Requested-By: ambari' -X GET -u <userid>:<passowrd> "+urll+"|grep -i host_name|awk -F: '{print $2}'|sed 's/\"//g'|grep -i <slave nodes hostname>"
   p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
   (host, err) = p.communicate()
   #print host
   urllist = []
   for h in host.split():
       urllist.append(""+urll+"/"+h+"/host_components/"+metric_name+"?fields=metrics")
else:
   host = hosts.split(",")
   urllist = []
   for h in host:
       urllist.append(""+urll+"/"+h+"/host_components/"+metric_name+"?fields=metrics")


ThreadPool(50).map(getmetric, urllist)

#Removing the lock file
cmd2 = "rm /var/run/pullambari_python_"+metric_name+".lock"
s = subprocess.Popen(cmd2, stdout=subprocess.PIPE, shell=True)
(output2, err2) = s.communicate()
