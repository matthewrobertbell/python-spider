import greenlet

import random
from lxml import etree
import web
import urlparse
import re

import json
import os
import urllib2
import sys
import time

from collections import defaultdict

import gevent
from gevent import monkey
from gevent import queue
from gevent import pool
monkey.patch_all(thread=False)

class GenericSpider(object):
	def __init__(self,domain,threads=30,min_delay=10,max_delay=30,timeout=20,proxy=False,retries=3,positive_url_filters=[],negative_url_filters=[],first_urls=None,random_ref=True,domain_ref=False,debug=False,redis_key=None,filename=None,seen_urls_filename=None,max_queue_size=1000000,max_seen_size=10000000, max_pages=10000000):
		self.max_queue_size = max_queue_size
		self.max_seen_size = max_seen_size
		self.max_pages = max_pages
		self.scrape_counter = 0
		self.data_counter = 0
		self.domain = domain
		self.threads = threads
		self.pool = pool.Pool()
		self.queue = queue.Queue()
		self.min_delay = min_delay
		self.max_delay = max_delay
		self.timeout = timeout
		self.proxy = proxy
		self.retries = retries
		self.positive_url_filters = positive_url_filters
		self.negative_url_filters = negative_url_filters + ['.jpg','.gif','.pdf','.png','.css','.js']
		self.data = []
		self.blank_regex = re.compile('')
		self.random_ref = random_ref
		self.domain_ref = domain_ref
		self.recent_urls = []
		self.debug = debug
		self.redis_key = redis_key
		self.filename = filename
		self.seen_urls_filename = seen_urls_filename
		self.seen_urls = None
		if self.seen_urls_filename:
			if os.path.isfile(self.seen_urls_filename):
				import pybloom
				self.seen_urls = pybloom.ScalableBloomFilter.fromfile(open(self.seen_urls_filename))
		if not self.seen_urls:
			import pybloom
			self.seen_urls = pybloom.ScalableBloomFilter(initial_capacity=100, error_rate=0.001, mode=pybloom.ScalableBloomFilter.SMALL_SET_GROWTH)
		if filename:
			self.file_handle = open(filename,'w')
		if self.redis_key:
			import redis
			self.redis = redis.Redis()
		
		if first_urls:
			if isinstance(first_urls,basestring):
				first_urls = [first_urls]
			for first_url in first_urls:
				self.queue.put(first_url)
				self.seen_urls.add(first_url)
				self.recent_urls.append(first_url)
		else:
			self.queue.put(domain)
			self.seen_urls.add(domain)
			self.recent_urls.append(domain)
		
	def fix_url(self,url):
		while '../' in url:
			url = '/'.join(url.split('/')[:-1])
		url = urlparse.urljoin(self.domain,url)
		if url.startswith(self.domain):
			if '#' in url:
				url = url.split('#')[0]
			if self.positive_url_filters:
				for filter in self.positive_url_filters:
					if isinstance(filter,basestring):
						if not filter in url:
							return None
					elif isintance(filter,self.blank_regex):
						if not filter.search(url):
							return None
			if self.negative_url_filters:
				for filter in self.negative_url_filters:
					if isinstance(filter,basestring):
						if filter in url:
							return None
					elif isintance(filter,self.blank_regex):
						if filter.search(url):
							return None			
			return url
		
	def urlopen(self):
		while self.max_pages > self.scrape_counter:
			try:
				url = self.queue.get(timeout=self.timeout*2)
			except:
				break
			for i in range(self.retries):
				try:
					with gevent.Timeout(self.timeout):
						if self.random_ref:
							data = web.grab(url,proxy=self.proxy,ref=random.sample(self.recent_urls,1)[0])
						elif self.domain_ref:
							data = web.grab(url,proxy=self.proxy,ref=self.domain)
						else:
							data = web.grab(url,proxy=self.proxy)
						self.process(data, url)
						break
				except:
					gevent.sleep(random.randrange(self.min_delay,self.max_delay))
			gevent.sleep(random.randrange(self.min_delay,self.max_delay))
				
	def process(self,data, data_url):
		self.scrape_counter += 1
		doc = etree.HTML(data)
		urls = set([url for url in [self.fix_url(url) for url in set(doc.xpath('//a/@href'))] if url])
		new_urls = set([url for url in urls if not url in self.seen_urls])
		if len(new_urls) + self.queue.qsize() > self.max_queue_size:
			new_urls = list(new_urls)[:self.max_queue_size-self.queue.qsize()]
		if self.max_seen_size > len(self.seen_urls) and self.max_queue_size < self.max_seen_size:
			[self.seen_urls.add(url) for url in new_urls]
			[self.queue.put(url) for url in new_urls]
		self.recent_urls += new_urls
		self.recent_urls = self.recent_urls[-10:]
		if self.debug:
			print '*' * 20
			print 'Just Grabbed: %s' % data_url
			print '%s Pages Scraped ' % self.scrape_counter
			print 'New URLs: %s' % len(new_urls)
			print 'Seen URLs: %s' %  len(self.seen_urls)
			print 'Bloom Capacity: %s' % self.seen_urls.capacity
			print 'URLs in Queue: %s' % self.queue.qsize()
			print 'Data scraped so far: %s' % int(self.data_counter + len(self.data))
			if self.scrape_counter > 0 and self.queue.qsize() > 0:
				if len(self.seen_urls) < self.max_pages:
					page_counter = len(self.seen_urls)
				else:
					page_counter = self.max_pages
				print '%.2f%% Complete' % float(self.scrape_counter / float(page_counter) * 100)
			print '*' * 20
			print '\n'
		return doc
		
	def save(self):
		if self.redis_key or self.filename:
			gevent.sleep(self.timeout)
			while True:
				if self.data:
					if self.seen_urls_filename:
						self.seen_urls.tofile(open(self.seen_urls_filename,'w'))
					if self.filename:
						filedata = ''
					while self.data:
						item = self.data.pop()
						self.data_counter += 1
						if len(item) == 1 and isinstance(item,list):
							item = item[0]
						json_item = json.dumps(item)
						if self.redis_key:
							self.redis.rpush(self.redis_key,json_item)
						if self.filename:
							filedata += json_item+'\n'
					if self.filename:
						self.file_handle.write(filedata)
						filedata = ''
					
				else:
					gevent.sleep(self.timeout*3)
					if not self.data:
						break		
		
	def run(self):
		self.pool.spawn(self.save)
		for i in range(self.threads):
			self.pool.spawn(self.urlopen)
		self.pool.join()
		return self.data
		
class ImageSpider(GenericSpider):
	def process(self,data,url):
		doc = super(ImageSpider, self).process(data,url)
		images = doc.xpath('//img')
		self.data += [image.items() for image in images]
		print '%s images' % len(self.data)
				
class PageRankSpider(GenericSpider):
	def __init__(self,*args,**kwargs):
		super(PageRankSpider, self).__init__(*args,**kwargs)
		self.outbound_links = defaultdict(list)
		self.inbound_links = defaultdict(list)
		self.inbound_link_anchors = defaultdict(list)

	def process(self,data,url):
		doc = super(PageRankSpider, self).process(data,url)
		links = doc.xpath('//a')
		for link in links:
			link_anchor = link.text
			link_url = None
			for k,v in link.items():
				if k.lower() == 'href':
					link_url = v
					break
			if link_url:
				self.outbound_links[url].append(link_url)
				self.inbound_links[link_url].append(url)
				self.inbound_link_anchors.append(link_anchor)
