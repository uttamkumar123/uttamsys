#!/usr/bin/python

####Script to setup quota on glusterfs folders
#   Author : Uttam Kumar
#   Date   : 10-Feb-2016
####

import os, sys, subprocess, getopt

def glusterquota(g,q,u,d):
    cmd2 = "/usr/sbin/gluster volume  quota "+g+" limit-usage /"+d+" "+q+""+u+""
    print "Executing quota setup command : "+cmd2+""
    r = subprocess.Popen(cmd2, stdout=subprocess.PIPE, shell=True)
    (output, err) = r.communicate()
    print  output


def main(argv):
   glusterfs = ''
   quotasize = ''
   unitsize  = ''
   try:
      opts, args = getopt.getopt(sys.argv[1:],"f:s:u:h",["fs=","size=","unit=","help"])
   except getopt.GetoptError:
      print 'Usage : gluster_quota_setup.py -f <glusterfs_name> -s <size in number> -u <unit of size, i.e. gb, tb> -h <help>'
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print 'Usage : gluster_quota_setup.py -f <glusterfs_name> -s <size in number> -u <unit of size, i.e. gb, tb> -h <help>'
         sys.exit(2)
      elif opt in ("-f", "--fs"):
         glusterfs = arg
      elif opt in ("-s", "--size"):
         quotasize = arg
      elif opt in ("-u", "--unit"):
         unitsize = arg
   cmd = "/usr/sbin/gluster volume info "+glusterfs+"|grep Brick1|awk -F: '{print $3}'"
   p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
   (output, err) = p.communicate()
   traverse = output.rstrip('\n')
   cmd1 = "/bin/ls -l "+traverse+"|awk '{print $9}'|sed '/^$/d'"
   q = subprocess.Popen(cmd1, stdout=subprocess.PIPE, shell=True)
   (output, err) = q.communicate()
   for line in output.split():
     glusterquota(glusterfs,quotasize,unitsize,line)
   cmd3 = "/usr/sbin/gluster volume quota "+glusterfs+" list"
   s = subprocess.Popen(cmd3, stdout=subprocess.PIPE, shell=True)
   (output, err) = s.communicate()
   print output

if __name__ == "__main__":
   main(sys.argv[1:])
