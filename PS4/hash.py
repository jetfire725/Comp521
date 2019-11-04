import sqlite3
import math
db = sqlite3.connect("NFL.db")
db.row_factory = sqlite3.Row
cursor = db.cursor()

#get page id
def getPageId(tid, year):
    tid, year = str(tid)[2:], str(year)[2:]
    return int(tid + year)

#example index: {location: {localDepth: 2, bucket1:[], location2: {localDepth: 3, bucket1:[], bucket2:[]}
index = {}

#default dictionary size
dictionarySize = 256

#default global depth
globalDepth=2

#Number of times the dictionary doubles
numTimesSplit=0

#inserts the tuple into the index. 
def insertIntoIndex(tuple, location):
    if location in index.keys():
        pid=tuple["pid"]

        # times this index split
        numDoublesForLocation=index[location]["localDepth"]-2
        
        #this stuff gets the bucket that the value would have been assigned to, 
        # had i used the full dictionary size.
        dic = 256 #dictionary size with respect to location
        n=0 #counter
        while n < numDoublesForLocation:
            dic = dic*2
            n+=1
        hash = pid % dic
        bucket = math.floor(hash/dic)
        
        localDepth = index[location]["localDepth"]
        global globalDepth
        currentBucket = index[location][bucket]

        #if bucket full
        if len(currentBucket) == 400:
            global dictionarySize
            global numTimesSplit
            numTimesSplit+=1
            #create new bucket, split elements.
            index[location][localDepth-1] = currentBucket[200:399]
            del currentBucket[200:399]
            currentBucket.append(tuple)
            
            #increment depths
            index[location]["localDepth"]+=1
            if index[location]["localDepth"] > globalDepth:
                globalDepth = globalDepth+1
                dictionarySize = dictionarySize*2
        else:
            currentBucket.append(tuple)
    else:
        index[location] = {"localDepth": 2, 0:[tuple]}

#main, go through records, building index
cursor.execute('select * from PlayedFor;')
records=cursor.fetchall()
for row in records:
    pid = row[0]
    tid = row[1]
    year = row[2]
    pageId = getPageId(tid, year)
    tuple = {"pid": pid, "pageId": pageId}
    
    #this is the raw hash
    hash = pid % dictionarySize
    
    # if the dictionary doubles, this still gives me a value of 
    #0-255 since my index only simulates the dictionry size.
    location = hash-(math.floor(hash/256)*256)
    insertIntoIndex(tuple, location)
print("Num of bucket splits: "+str(numTimesSplit))
print("Dictionary size: " +str(dictionarySize))

#get max and mins
max=0
maxBucket=[]
min=400
minBucket=[]
totalRecords =0
for location, value in index.items():
    for bucket in range(len(value)-1):
        if len(value[bucket]) > max:
            max = len(value[bucket])
            maxBucket = [location, bucket]
        if len(value[bucket]) < min:
            min = len(value[bucket])
            minBucket = [location, bucket]
        totalRecords += len(value[bucket])

#results, if bucket at an index is greater that 0, 1 for ex. then index is actually index+1*256.
#so bucket[164, 1] = index 164+256 = 420, +1 cause indexes start at 0 = index 421
print("Number of records: "+str(totalRecords))
print("Max bucket size: "+str(max))
print("Max bucket: "+str(maxBucket))
print("Min bucket size: "+str(min))
print("Min bucket: "+str(minBucket))

