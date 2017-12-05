import os
import pandas as pd
import numpy as np
import time

from itertools import islice
#import os.path


#outputFileName = 'capone_omnichannel_01_07_feb_17_processed_v1.csv'
outputFileName = 'capone_omnichannel_01_17_may17_processed_23jun17.csv'
outputFilePath =  "../output/"
outputFile = outputFilePath + outputFileName
#outputFile = 'capone_omnichannel_test_v2.csv'
#inputFile = 'cap1test.json'
inputFile = 'cap1_omni_channel_1may_17may_2017_json_23june17'
inputFilePath = "../data/omni_may_data/" + inputFile

n_days = 2
n_micro_sec = n_days*24*60*60*1000

chunk_size = 1

def pre_process_json(chunk_json):
    #json_row_str = "[" + json_row + "]"
    chunk_json_str = "[" + ','.join(chunk_json) + "]"

    return(chunk_json_str)


def process_json(row_json):

    #df = pd.read_json(json_row)
    #dfWebPages = df['webPages'].apply(pd.Series)

    #dfWebPages = pd.DataFrame(df['webPages'][0])
    dfWebPages = pd.DataFrame(row_json['webPages'])

    speech_start_time = row_json['speechStartTime']

    dfWebPagesTmp = dfWebPages.ix[((speech_start_time-dfWebPages.time)>0) & ((speech_start_time-dfWebPages.time)<n_micro_sec)]
    dfWebPagesTmp = dfWebPagesTmp.reset_index(drop=True)


    if dfWebPagesTmp.shape[0] > 0:

        maxWebTimeStamp = np.max(dfWebPagesTmp.time)

        dfWebPagesFiltered = dfWebPages.ix[dfWebPages.time<=maxWebTimeStamp]
	dfConcurrency = dfWebPages.ix[dfWebPages.time>maxWebTimeStamp]


        if dfWebPagesFiltered.shape[0]>0:
    	    dfFinal = dfWebPagesFiltered.tail(1)
	    dfFinal['numPageVisits'] = dfWebPagesFiltered.shape[0]
	    #dfFinal['queue'] = queue
    	    dfFinal['accountId'] = row_json['w_acctid']
    	    dfFinal['speech_start_time'] = speech_start_time
    	    dfFinal['speech_uuid'] = row_json['speech_uuid']

            if dfWebPagesFiltered.shape[0]>1:
                dfFinal['prevPageTitle'] = dfWebPagesFiltered.tail(2).head(1).pageTitle.tolist()[0]
            else:
        	dfFinal['prevPageTitle'] = ""

            if dfConcurrency.shape[0]>0:
                dfFinal['concurrency_flag'] = 1
            else:
        	dfFinal['concurrency_flag'] = 0

	    #if os.path.isfile(outputFile):
            dfFinal.to_csv(outputFile, mode='a', header=False, index=False, encoding='utf-8')
	    #else:
		#dfFinal.to_csv(outputFile, mode='w', header=True, index=False)


#with open('../data/cap1test.json', 'rb') as f:

start_time = time.time()
print "Start Time: ", start_time

with open(inputFilePath, 'rb') as f:
    while True:
	chunk_json = list(islice(f, chunk_size))

	chunk_json = pre_process_json(chunk_json)
	#print chunk_json
	try:
	    jsonToDf = pd.read_json(chunk_json)
	except Exception, e:
	    print "Error ... Skipped LINE!!!"
	    print str(e)
	    #print chunk_json
	    #import pdb; pdb.set_trace()
	#print df
	jsonToDf.apply(lambda row: process_json(row), axis=1)

	if len(chunk_json) < chunk_size:
            break

print("--- %s seconds ---" % (time.time() - start_time))

print "End Time: ", time.time()

