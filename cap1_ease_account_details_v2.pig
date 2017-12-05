REGISTER 'hdfs:///lib/avro/1.7.5/avro-mapred-1.7.5-hadoop2.jar'
REGISTER 'hdfs:///lib/idm-pig/idm-pig-hadoop2-ha-1.1.3.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/elephant-bird-pig-4.5.jar';
REGISTER 'hdfs:///user/rnarendra/supportLibs/json-simple-1.1.jar';
REGISTER 'hdfs:///user/rnarendra/scripts/json_parser.py' using jython as jsonParse;
REGISTER 'hdfs:///user/rnarendra/scripts/valid_eos.py' using jython as valid_event;

REGISTER 'hdfs:///user/preddy/CapOne/get_all_account_details_v2.py' using jython as get_all_account_details_v2;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');

%DECLARE YEAR '2017'
%DECLARE MONTH '{10}'
--%DECLARE DAYS '{01,02,03,04,05,06,07}'
%DECLARE DAYS '{01}'

--data = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH/day=$DAYS/hour={10}/*/*.avro' using LOAD_IDM;
data = LOAD '/raw/prod/rtdp/idm/events/cap1enterprise/year=$YEAR/month=$MONTH/day=$DAYS/*/*/*.avro' using LOAD_IDM;

f1 = FILTER data BY specificEventType == 'WebCustomEvent';

f2 = FOREACH f1 GENERATE header.timeEpochMillisUTC as time, header.channelSessionId as bsid, 
custom#'tfsPageURL' as pageurl, 
custom#'tfsAcctIdStatusAndProductIdentifier' as account_ref_details,
custom#'tfsCollectionAcctData' as collec_cat_code_data;

f3 = FILTER f2 BY (account_ref_details !='' and account_ref_details is not null);

f4 = FILTER f3 BY pageurl == 'https://myaccounts.capitalone.com/accountSummary';

f5 = GROUP f4 BY bsid;

f6 = FOREACH f5 {
	sort = ORDER f4 BY time;
	first = LIMIT sort 1;
	GENERATE FLATTEN(first);
	};

f7 = FOREACH f6 GENERATE time, bsid, 
get_all_account_details_v2.extract_account_info(account_ref_details, collec_cat_code_data) as cust_account_details;

STORE f7 INTO 'cap1_ease_account_details.tsv';
