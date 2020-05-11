# Python 3 program to build Bloom Filter
# Install mmh3 and bitarray 3rd party module first
# pip3 install mmh3
# pip3 install bitarray
# In MacOS, "pip3 install mmh3" might fail,
# Run "xcode-select --install" first, then run "pip3 install mmh3"
import math
import mmh3
from bitarray import bitarray
from random import shuffle

class BloomFilter(object):

    def __init__(self, items_count, fp_prob):
        #items_count(n) : Number of items expected to be stored in bloom filter 
        #fp_prob(p) : False Positive probability in decimal
        
        self.fp_prob = fp_prob

        # Size of bit array to use (get m)
        self.size = self.get_size(items_count,fp_prob)

        # number of hash functions to use (get k)
        self.hash_count = self.get_hash_count(self.size,items_count)

        # Bit array of given size
        self.bit_array = bitarray(self.size)

        # initialize all bits as 0
        self.bit_array.setall(0)


    #Add a item into filter
    def add(self, item):
        digests = []
        for i in range(self.hash_count):
  
            # create digest for given item.
            digest = mmh3.hash(item,i) % self.size
            digests.append(digest)

            # set the bit True in bit_array
            self.bit_array[digest] = True


    #Check the existence of an item in filter
    def is_member(self, item):
        for i in range(self.hash_count):
            digest = mmh3.hash(item,i) % self.size
            if self.bit_array[digest] == False:
                # if any of bit is False then,its not present
                # in filter
                # else there is probability that it exist
                return False
        return True


    '''
    get the size of bit array(m) based on equation m = -(n * lg(p)) / (lg(2)^2)
    n : number of items expected to be stored in filter
    p : False Positive probability in decimal
    '''
    @classmethod
    def get_size(self,n,p):

        m = -(n * math.log(p))/(math.log(2)**2)
        return int(m)

    '''
    get the number of hash function(k) based on equation k = (m/n) * lg(2)
    m : size of bit array
    n : number of items expected to be stored in filter
    '''
    @classmethod
    def get_hash_count(self, m, n):

        k = (m/n) * math.log(2)
        return int(k)

# uncommetn and run python3 bloom_filter.py to test code below 
'''
n = 20 
p = 0.05 
  
bloomf = BloomFilter(n,p) 
print("Size of bit array:{}".format(bloomf.size)) 
print("False positive Probability:{}".format(bloomf.fp_prob)) 
print("Number of hash functions:{}".format(bloomf.hash_count)) 
  
# words to be added 
word_present = ['katy','jerry','lily','yuhong','jessica', 
                'sri','max','acc','john','jennifer','lisa', 
                'yohna','chris','tina','stefan'] 
  
# word not added 
word_absent = ['popeyes','burgerking','fiveguys','starwar','marvel', 
               'love','brandy','urban'] 
  
for item in word_present: 
    bloomf.add(item) 
  
shuffle(word_present) 
shuffle(word_absent) 
  
test_words = word_present[:10] + word_absent 
shuffle(test_words) 
for word in test_words: 
    if bloomf.is_member(word): 
        if word in word_absent: 
            print("'{}' is a false positive!".format(word)) 
        else: 
            print("'{}' is probably present!".format(word)) 
    else: 
        print("'{}' is definitely not present!".format(word)) 




'''