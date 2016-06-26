"""The classic MapReduce job: count the frequency of words.
"""
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
	items = line.split(',')

	if items[5].replace('.','',1).isdigit():
		yield (items[4], float(items[8]))


    def reducer(self, word, counts):
	
	total = 0.0
	nonzero =0.0	
	for i in counts:
		if i >0 :
			nonzero +=1.0
		total+=1.0
        percent =float( (nonzero/total)*100);

	yield (word, percent)


if __name__ == '__main__':
     MRWordFreqCount.run()

