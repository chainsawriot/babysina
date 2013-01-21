#!/usr/bin/env python
# -*- coding: utf-8 -*-

from weibo import APIClient
import weibo
import mypassx as mypass
import sys
#import pg
import httplib
#import simplejson
import time
import string

#import oauth2 as oauth
import pprint

import datetime
import csv
import types

#import weibopy2 # we use our own slightly-extended version of weibopy
import unittest
import socket
#from weibopy.auth import OAuthHandler, BasicAuthHandler
#from weibopy.api import API

#import lucene
#import sinaweibolucene


import re
import os

class API():
    sinaweiboOauth = mypass.getSinaWeiboOauth()
    max_api_misses = 6
    pgconn = None
    toleranceNotToBeginning = 5 # in fetching timelines, break from loop when toleranceNotToBeginning consecutive statuses already exist in the DB
    toleranceNotToBeginningLong = 150 # for reposts
    max_gotall_count = 3
    api_wait_secs = 5
    max_api_misses_half = 3
    max_api_misses = 6
    max_reposts_pages = max_comments_pages = 1000
    max_reposts_blanks = max_comments_blanks = 3
    max_reposts_tries = max_comments_tries = 3
    usage = "sinaweibo2.oauth.py [id or file with ids] [primary opts] [sec opts]"
    rp_dir = "/var/data/sinaweibo/rp"
    comments_dir = "/var/data/sinaweibo/comments"
    reposts_dir = "/var/data/sinaweibo/reposts"
    timelines_dir = "/var/data/sinaweibo/timelines"
    timeline_ids = list()
    verbose = False
    getall = False
    force_screenname = False
    checkonly = False
    doupdate = False
    saveRP = False
    rt = False # Don't store the retweet
    index = False
    indexer = None
    doublecheck = False # If we get a blank timeline, it may just be an error, so we log it


    #def __init__(self, auth):
    def setToken(self):
        self.api2 = APIClient(app_key=self.sinaweiboOauth['app_key'], app_secret=self.sinaweiboOauth['app_secret'], redirect_uri=self.sinaweiboOauth['redirect_uri'])
        self.api2.set_access_token(self.sinaweiboOauth['access_token'], self.sinaweiboOauth['expires_in'])
    def getAtt(self, obj, key):
        try:
            return obj.__getattribute__(key)
        except Exception, e:
            return None
    def setAtt(self, obj, key, value):
        try:
            return obj.__setattribute__(key, value)
        except Exception, e:
            return None

    def fixdate(self, textdate):
        '''
        fix the date(string) returned from Sina to the standard format 
        '''
        textdate = re.sub(r' \+....', '', textdate) # kill the +0800, not supported by strptime
        datedt = datetime.datetime.strptime(textdate, "%a %b %d %H:%M:%S %Y")
        return datedt.strftime("%Y-%m-%d %H:%M:%S")
    #def get_rateLimit(self):
        
    def get_status(self, id, getUser=False, toDB=False):
        time_db = 0
        time_db_u = 0
        start_time_api = time.time()
        api_misses = 0
        while api_misses < self.max_api_misses:
            try:
                status = self.api2.statuses.show.get(id=id)
                break
            except weibo.APIError as e: ## Need more exception handling.
                print e.message
                api_misses += 1
                if api_misses >= self.max_api_misses or ("target weibo does not exist" in e.message.lower() or "permission denied" in e.message.lower()):
                    return { "id": id, "msg": e.message } ## aka toxicbar
                time.sleep(self.api_wait_secs * 1)
        time_api = time.time() - start_time_api
        # status is just a glorified dict, not an object like weibopy2
        # So don't need to use getAtt
        row = self.status_to_row(status) 
        # TODO: push row to db
        return row

    def status_to_row(self, status):
        x = dict()
        x["created_at"] = self.fixdate(status["created_at"])
        for a in ["text", "source", "location", "thumbnail_pic", "bmiddle_pic", "original_pic", "screen_name", "in_reply_to_screen_name"]:
            try:
                att = status[a]
            except:
                att = None
            try:
                x[a] = att.encode("utf8")
            except:
                x[a] = att
        for a in ["id", "in_reply_to_user_id", "in_reply_to_status_id", "truncated", "reposts_count", "comments_count", "attitudes_count", "mlevel", "deleted"]:
            try:
                x[a] = status[a]
            except:
                x[a] = None
        try:
            rts = status['retweeted_status']['id']
        except:
            rts = None # This message is original
        try:
            rts_user_id = status['retweeted_status']['user']['id']
        except:
            rts_user_id = None
        if rts is not None:
            if self.rt:
                rt_dict = status['retweeted_status']
                if rt_dict['created_at'] is not None:
                    x['rt'] = self.status_to_row(rt_dict)
            x['retweeted_status'] = rts
        if rts_user_id is not None:
            x['retweeted_status_user_id'] = rts_user_id
        try:
            user_id = status['user']['id']
        except:
            user_id = None
        try:
            screen_name = status['user']['screen_name'].encode("utf-8")
        except:
            screen_name = None
        if user_id is not None:
            x['user_id'] = user_id
        if screen_name is not None:
            x['screen_name'] = screen_name
        try:
            geo = status['geo']
            coord = geo["coordinates"]
        except:
            geo = None
        if geo is not None and coord is not None and len(coord) >0:
            lat = coord[0]
            lng = coord[1]
            wkt_point = "POINT(" + str(lng) + " " + str(lat) + ")"
	    #print wkt_point
	    x["geo"] = "SRID=4326;" + wkt_point
	return x

    def dispatch(self, opt, id, output_counts=False):
        if opt == 9:
            out = self.get_status(id, getUser = True)
        else:
            out = None
            #        print out
        return out


if __name__ == "__main__":
    api = API()
    api.setToken()
    if len(sys.argv) <= 2:
        print "good bye\n"
        sys.exit()
    else:
        try:
            id = long(sys.argv[1])
        except:
            id = 0
            fname = str(sys.argv[1])
    if len(sys.argv) > 2:
        opt = sys.argv[2]
        if opt == "-ss" or opt == "--single-status":
            opt = 9
        elif opt == "-as": # display API status, rate limit and token expiry
            opt = 2046
        else:
            print "good bye\n"
            sys.exit()
    if id > 0:
        out = api.dispatch(opt, id)
    output = { "data": out, "opt": opt, "count": len(out), "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") }
    print output
            #    print api.get_status(id=3481475946781445)
