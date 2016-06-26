"""The classic MapReduce job:calulate average income per day for a taxi driver in New york
"""
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")


class AverageIncome(MRJob):

    def mapper(self, _, line):
	items = line.split(',')

	if items[5].replace('.','',1).isdigit():  # ignoring files with trip information , process only fare files
		date_time = items[3].split(" ")
		yield (items[1], ( date_time[0], float( items[8]),float(items[10])))


    def combiner(self, h_license, data): # yield average daily tips and income for individual license holder
	print " in Combiner "
	income_per_date={}
	for (date,tip,totalamount) in data:
		if income_per_date.has_key(date):
			L = income_per_date[date]
			t,tm = L[0]
			K = []
			K.append((t+tip, tm+totalamount))
			income_per_date[date]= K
		else:
			L = []
			L.append((tip,totalamount))
			income_per_date[date]=L

	totaltips = 0.0
	total_income = 0.0
	count = 0.0
	for dates in income_per_date.keys():
		L = income_per_date[dates]
		t, tm = L[0]
		totaltips += t
		total_income += tm
		count +=1
	
	avg_tip = totaltips/count
	avg_income = total_income/count
	yield ( "Avg daily tips and income", (avg_tip, avg_income))
	#print ( h_license, avg_tip, avg_income)
	
    def reducer(self,text, data):  # Yield overall daily average tips and income for a taxi driver
	
	count = 0.0
	total_avg_tips = 0.0
	total_avg_income = 0.0
	for (ind_avg_tip, ind_avg_in) in data:
		count = count + 1
		total_avg_tips += ind_avg_tip
		total_avg_income += ind_avg_in
	
	overall_avg_tip = total_avg_tips/count
	overall_avg_income = total_avg_income/count
	yield ( text, (overall_avg_tip, overall_avg_income) )


if __name__ == '__main__':
     AverageIncome.run()

