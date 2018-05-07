from operator import add

srcDir = <set srcDir to the absolute path of the source data directory location>

file = srcDir + "/sherlock-holmes.txt"
text = sc.textFile(file)

import re

# use regular expression to split lines into words and translate them into lower case
words = text.flatMap(lambda w : re.split('[ \],.:;?!\-@#\(\)\\\*\"]*', w)).map(lambda w : w.lower())

# map each word to a pair with word as key and initial count 1
# then reduce by key getting the total counts for each word
pairs = words.map(lambda w : (w, 1)).reduceByKey(add)

# collect pairs into dictionary for random access
list = pairs.collectAsMap()

# print the distinct word count
print("distinct word count " + str(pairs.count()))

# print the counts for given words
print("if  count " + str(list["if"]))
print("and count " + str(list["and"]))
print("but count " + str(list["but"]))

# define the function to get the pair with the higher counts of the two
def getMax(r, c):
  if (r[1] > c[1]):
    return r
  else:
    return c

# get the word with the highest count
max = pairs.reduce(lambda r, c: getMax(r, c))
print("word with max count " + str(max))
