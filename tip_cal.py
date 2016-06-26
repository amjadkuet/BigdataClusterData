"""The classic MapReduce job: 

This program is to report following information for each payment type ( CASH, Credit Card etc):
1. Total number of trip paid for
2. Total nunber of trip having non-zero tips
3. average amount of fare paid
4. average amount of tip paid
5. percentage of trips where tip was given 

"""
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class TipPercent(MRJob):

    def mapper(self, _, line):
	items = line.split(',')

	if items[5].replace('.','',1).isdigit():  # ignoring files with trip information , process only fare files
		yield (items[4], ( float( items[5]),float(items[8])))


    def reducer(self, payment_type, counts):
	
	
	total = 0.0
	nonzero =0.0
	totalfare = 0.0	
	totaltips = 0.0
	for (fare,tip) in counts:
		if tip >0 :
			nonzero += 1.0
			totaltips += tip
		total += 1.0
		totalfare += fare
        tippercent =float( (nonzero/total)*100)
	averagefare = float (totalfare/total)
	if nonzero>0:
		averagetip = float(totaltips/nonzero)
	else:
		averagetip = 0.0

	yield (payment_type, (total,nonzero,averagefare,averagetip, tippercent)) 


if __name__ == '__main__':
     TipPercent.run()

