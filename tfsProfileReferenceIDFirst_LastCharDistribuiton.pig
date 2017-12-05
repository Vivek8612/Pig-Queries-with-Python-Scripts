--Character Distribution

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');
-- data = LOAD 'hdfs:///raw/prod/rtdp/idm/events/cap1enterprise/year=2017/month=06/day={09,10,11,12,13,14,15,16,17,18,19,20}/*/*/*.avro' USING LOAD_IDM;
data = LOAD 'hdfs:///raw/prod/rtdp/idm/events/cap1enterprise/year=2017/month=08/day={15,16,17,18,19,21}/*/*/*.avro' USING LOAD_IDM;
data = FILTER data BY specificEventType == 'WebCustomEvent';
generateEvents = FOREACH data GENERATE custom as custom;
tfsProfileReferenceIDFilter = FILTER generateEvents BY custom#'tfsProfileReferenceID' != '' AND custom#'tfsProfileReferenceID' IS NOT NULL;
tfsProfileReferenceIDFilter = FOREACH tfsProfileReferenceIDFilter GENERATE custom#'tfsProfileReferenceID' AS tfsProfileReferenceID:chararray;
tfsProfileReferenceIDFilter = DISTINCT tfsProfileReferenceIDFilter;
-- tfsProfileReferenceIDs = FOREACH tfsProfileReferenceIDFilter GENERATE SUBSTRING(tfsProfileReferenceID, (int)SIZE(tfsProfileReferenceID) - 1, (int)SIZE(tfsProfileReferenceID)); -- uncomment this line this get last char dist
-- tfsProfileReferenceIDs = FOREACH tfsProfileReferenceIDFilter GENERATE SUBSTRING(tfsProfileReferenceID, 0, 1); -- uncomment this line to get first character dist
tfsProfileReferenceIDGroups = FOREACH (GROUP tfsProfileReferenceIDs BY $0) GENERATE group, COUNT(tfsProfileReferenceIDs);
DUMP tfsProfileReferenceIDGroups
-- STORE tfsProfileReferenceIDGroups INTO 'abhayshukla/capitalone/newdata/tfsProfileReferenceID_09Jun_20JunFirstChar' USING PigStorage(',', '-schema');
-- STORE tfsProfileReferenceIDGroups INTO 'abhayshukla/capitalone/newdata/tfsProfileReferenceID_15Aug_21AugFirstChar' USING PigStorage(',', '-schema');

-- only CC customers - First Char Distribution
REGISTER 'hdfs:///user/abhayshukla/abhayshukla/udf/cc_or_not.py' USING jython AS capone_ufds;
DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc');
-- data = LOAD 'hdfs:///raw/prod/rtdp/idm/events/cap1enterprise/year=2017/month=06/day={09,10,11,12,13,14,15,16,17,18,19,20}/*/*/*.avro' USING LOAD_IDM;
data = LOAD 'hdfs:///raw/prod/rtdp/idm/events/cap1enterprise/year=2017/month=08/day={15,16,17,18,19,21}/*/*/*.avro' USING LOAD_IDM;
-- tfsProfileReferenceID
-- tfsAcctIdStatusAndProductIdentifier
data = FILTER data BY specificEventType == 'WebCustomEvent';
generateEvents = FOREACH data GENERATE custom as custom;
tfsProfileReferenceIDFilter = FILTER generateEvents BY custom#'tfsProfileReferenceID' != '' AND custom#'tfsProfileReferenceID' IS NOT NULL AND custom#'tfsAcctIdStatusAndProductIdentifier' != '' AND custom#'tfsAcctIdStatusAndProductIdentifier' IS NOT NULL AND custom#'tfsAcctIdStatusAndProductIdentifier' != 'undefined';
-- tfsProfileReferenceIDFilter = FOREACH tfsProfileReferenceIDFilter GENERATE custom#'tfsProfileReferenceID' AS tfsProfileReferenceID:chararray, custom#'tfsAcctIdStatusAndProductIdentifier' AS tfsAcctIdStatusAndProductIdentifier:chararray, capone_ufds.ccORnot(custom#'tfsAcctIdStatusAndProductIdentifier');
tfsProfileReferenceIDFilter = FOREACH tfsProfileReferenceIDFilter GENERATE custom#'tfsProfileReferenceID' AS tfsProfileReferenceID:chararray, capone_ufds.ccORnot(custom#'tfsAcctIdStatusAndProductIdentifier');
-- tfsProfileReferenceIDFilter = DISTINCT tfsProfileReferenceIDFilter;
-- tfsProfileReferenceIDCounts = FOREACH (GROUP tfsProfileReferenceIDFilter BY $1) GENERATE group, COUNT(tfsProfileReferenceIDFilter);
-- DUMP tfsProfileReferenceIDCounts
tfsProfileReferenceIDCC = FILTER tfsProfileReferenceIDFilter BY $1 == 1;
tfsProfileReferenceIDCCInfo = FOREACH tfsProfileReferenceIDCC GENERATE tfsProfileReferenceID AS tfsProfileReferenceID;
tfsProfileReferenceIDCCInfo = DISTINCT tfsProfileReferenceIDCCInfo;
tfsProfileReferenceIDs = FOREACH tfsProfileReferenceIDCCInfo GENERATE SUBSTRING(tfsProfileReferenceID, 0, 1); -- get first character
tfsProfileReferenceIDGroups = FOREACH (GROUP tfsProfileReferenceIDs BY $0) GENERATE group, COUNT(tfsProfileReferenceIDs);
DUMP tfsProfileReferenceIDGroups