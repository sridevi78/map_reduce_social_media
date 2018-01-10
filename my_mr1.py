from mrjob.job import MRJob
import heapq as hp
import re

class FirstStep(MRJob):
    def mapper(self,_,line):
        lis=line.split(' ')
        for num in lis:
            yield (int(num),1)
    def reducer(self,word,values):
        yield ('red',(sum(values),word))
            
class SecondStep(MRJob):
    def reducer(self,key,records):
        self.heap1=[] 
        for item in records:
            hp.heappush(self.heap1,(int(item[0]),int(item[1])))
        #print hp.nlargest(100,self.heap1)
        yield ('key',(hp.nlargest(100,self.heap1)))
        #yield ('key',self.heap1)
        
class ThirdStep(MRJob):
    def reducer(self,key,items):
        self.com_heap=[]
        for item in items:
            hp.heappush(self.com_heap,item)
        yield ('key2',hp.nlargest(100,self.com_heap))        

class SteppedJob(MRJob):
    def steps(self):
        return FirstStep().steps()+SecondStep().steps()+ThirdStep().steps()

if __name__ == '__main__':
    SteppedJob.run()