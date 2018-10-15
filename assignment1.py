import os
from pyspark import SparkConf, SparkContext, SparkFiles

class mapreduce:
	def __init__(self, path):
		conf = SparkConf()
		self.sc = SparkContext(conf = conf)
		filelist = []
		for filename in os.listdir(path):
			if filename != '.DS_Store': #for testing locally on Mac
				filelist.append(path+filename)
		self.doc = self.sc.textFile(','.join(filelist))

	def wordCount(self):
		self.counts = self.doc.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)
		return self.counts

	def doubleWordCount(self):
		
		def doubleWords(line):
			line = line.split()
			doubleWords = ()
			for i in range(len(line) - 1):
				doubleWords += (line[i] + ' ' + line[i + 1],)
			return doubleWords

		self.double_counts = self.doc.flatMap(doubleWords).map(lambda doubleWord: (doubleWord, 1)).reduceByKey(lambda x, y: x + y)
		return self.double_counts

	def findFreq(self, filepath, filename):
		self.sc.addFile(filepath)

		def isTarget(word):
			targetList = []
			with open(SparkFiles.get(filepath.split('/')[-1])) as publicF:
				targetList = publicF.read().split()
			if word[0] in targetList:
				return True
			else:
				return False

		self.find_freq_counts = self.doc.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).filter(isTarget)
		return self.find_freq_counts

#basePath = '/Users/jyguo/Desktop/Courses/Cloud_Computing/'
#basePath = 's3://aws-logs-815402967860-us-east-2/elasticmapreduce/JY-CC/Assignment1/'
basePath = './'
biblePath = basePath + 'bibles/'
findFileName = 'word-patterns.txt'
findPath = basePath + findFileName

mr = mapreduce(biblePath)
mr.wordCount().saveAsTextFile(basePath + 'wordCountResult')
mr.doubleWordCount().saveAsTextFile(basePath + 'doubleWordCountResult')
mr.findFreq(findPath, findFileName).saveAsTextFile(basePath + 'findFreqResult')
