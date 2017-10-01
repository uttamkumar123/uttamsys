#!/usr/bin/python
# -*- coding: utf-8 -*-
import os,re, requests, json, time, socket, subprocess, sys, string
from time import time as timer
from urllib2 import urlopen
from Crypto.Cipher import AES
import base64
import argparse
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

def build_arg_parser():
    """
    Builds a standard argument parser with arguments for taking inputs for ambari rest api url and host_component
    -u url
    -m host_component
    """
    parser = argparse.ArgumentParser(
        description='Standard Arguments for pullimg metrics via Ambari REST API')

    parser.add_argument('-l', '--url',
                        required=True,
                        action='store',
                        help='Ambari Rest API url')
     
    parser.add_argument('-u', '--userid',
                        required=True,
                        action='store',
                        help='Ambari Userid with which metrics will be retrieved from Ambari API')

    parser.add_argument('-p', '--password',
                        required=True,
                        action='store',
                        help='Ambari Userid with which metrics will be retrieved from Ambari API')

    return parser

def passwddecrypt(passw):
    # the block size for the cipher object; must be 16 per FIPS-197
    BLOCK_SIZE = 16

    PADDING = '{'

    # one-liner to sufficiently pad the text to be encrypted
    pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * PADDING

    # one-liners to decrypt/decode a string
    DecodeAES = lambda c, e: c.decrypt(base64.b64decode(e)).rstrip(PADDING)
    # Using the below secret key
    secret = '1>G??XRL;e?;۠?n'

    # create a cipher object using the random secret
    cipher = AES.new(secret)
    encoded = passw
    # decode the encoded string
    decoded = DecodeAES(cipher, encoded)
    return decoded

def check_hdinsight(url):
    try:
        headers = {'X-Requested-By': 'ambari'}
        r = requests.get(url, auth=(""+user+"", ""+passw+""), headers=headers, verify=False)
        responseObj = json.loads(r.text)
        text = json.dumps(responseObj, sort_keys=False, indent=4)
        for linee in text.splitlines():
            line = linee.strip()
            if line.startswith("\"service_name\""):
               cfield = line.split(" ")
               service = cfield[1].replace("\"", "").replace(",", "")
               continue
            elif re.match( "\"state\"\s*\:\s*\"(\w*)\",$", line, re.I):
               hfield = line.split(" ")
               check_state = hfield[1].replace("\"", "").replace(",", "")
               continue
            elif line.startswith("\"text\""):
               cfield = line.split(" ")
               message = cfield[1:10]
               alert_message = ' '.join(str(e) for e in message)
               alert = alert_message.replace("\"", "").replace(",", "")
               print ""+check_state+" alert on "+service+" - "+alert+""


    except Exception as e:
        print "CRITICAL - Unable to hit Ambari URL ... "+url+""


#user = ""
#passw = ""
PARSER = build_arg_parser()
MY_ARGS = PARSER.parse_args()
urll = MY_ARGS.url
user = MY_ARGS.userid
#useridd = passwddecrypt(user)
passwd = MY_ARGS.password
passw = passwddecrypt(passwd)

check_hdinsight(""+urll+"/alerts?Alert/state.in(WARNING,CRITICAL)&Alert/maintenance_state=OFF&fields=Alert/state,Alert/text")
