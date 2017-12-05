-- visitors who saw impressions and clicked 
SET default_parallel 500
set mapreduce.map.memory.mb 4200
set mapreduce.map.java.opts -Xmx3600m

set mapreduce.reduce.memory.mb 7600
set mapreduce.reduce.java.opts -Xmx7200m

set mapred.task.timeout 7000000

register /home/htated/elephant-bird-pig-3.0.9.jar;
register /home/htated/json_simple-1.1.jar;
REGISTER /home/htated/flatten_json.py using jython as utils;
DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;

DEFINE LOAD_IDM com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc'); 

-- CompleteData = LOAD '/raw/prod/rtdp/idm/events/optus/year={2017}/month=06/day={0[1-7]}/hour=*/min=*/*.avro' USING LOAD_IDM;
CompleteData = LOAD '/raw/prod/rtdp/idm/events/optus/year={2017}/month={05,06,07}/day=*/hour=*/min=*/*.avro' USING LOAD_IDM;
CompleteData = FILTER CompleteData BY NOT ( header.associativeTag MATCHES '.*null.*' ) ;
CompleteData = FILTER CompleteData BY header.associativeTag != '0' AND NOT (header.associativeTag MATCHES '.*tie-app.*' ) AND NOT (header.associativeTag MATCHES '.*UNKNOWN.*' ) AND NOT (header.associativeTag MATCHES '.*pxoe-app.*' ) AND SIZE(header.associativeTag) == 36 AND NOT (header.associativeTag MATCHES '.*INT-.*' ) AND SIZE(header.channelSessionId) > 38 AND (header.channelSessionId != '86fcd1cd-2510-4f04-a462-4624ebe5d1ed::34');

impressionShown = FILTER CompleteData BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760070';
impressionShown = FILTER  impressionShown BY custom is not null;
impressionShown = FOREACH  impressionShown GENERATE custom#'EventData' as eData, header.associativeTag as vid, header.channelSessionId as bsid, header.timeEpochMillisUTC as epoch;
impressionShown = FILTER impressionShown BY eData is not null;
impressionShown = FOREACH impressionShown GENERATE JsonStringToMap(eData) as eDatajson, vid, bsid, epoch;
impressionShown = FILTER impressionShown BY eDatajson is not null ;
impressionShown = FOREACH impressionShown GENERATE eDatajson#'c'  as c, vid, bsid, epoch;
impressionShown = FILTER impressionShown BY c is not null;
impressionShown = FOREACH impressionShown GENERATE JsonStringToMap(c) as cjson, vid, bsid, epoch;
impressionShown = FILTER impressionShown BY cjson is not null;

impressionShown = FOREACH impressionShown GENERATE vid, bsid, epoch, cjson#'optdata' as optdata, cjson#'type' as type, cjson#'adreqid' as adreqid, cjson#'adid' as adid;
impressionShown = FILTER impressionShown BY optdata is not null;
impressionShown = FOREACH impressionShown GENERATE JsonStringToMap(optdata) as optjson, vid, type, bsid, epoch, adreqid, adid;
impressionShown = FILTER impressionShown BY optjson is not null;
impressionShown = FOREACH impressionShown GENERATE vid, type, bsid, epoch, adreqid, adid,
	optjson#'product_id' as prod_id,
	optjson#'segment' as segment_opt, 
	optjson#'creative_reason' as creative_reason,
	optjson#'served' as served, 
	optjson#'currentpageurl' as url,
	optjson#'creative_shown' as shown,
	optjson#'lob' as lob,
	optjson#'currentplacement' as cp, 
	optjson#'group' as group_opt;

impressionShown = FILTER impressionShown BY served is not null;
impressionShown = FOREACH impressionShown GENERATE vid, type, bsid, epoch, adreqid, adid, prod_id, segment_opt, creative_reason, JsonStringToMap(served) as servedjson, url, shown, lob, cp, group_opt;
impressionShown = FILTER impressionShown BY servedjson is not null;
impressionShown = FOREACH impressionShown GENERATE vid, type, bsid, epoch, adreqid, adid, prod_id, segment_opt, creative_reason, 
	servedjson#'creative_id' as creative_id, 
	servedjson#'segment' as segment_served, 
	servedjson#'creative_name' as segment_creative_name,
	url, shown, lob, cp, group_opt;

-- impressionShown = FILTER impressionShown BY type == 'on-domain' and creative_reason == 'null' and cp == 'AFEATURE' and lob == 'PostPaid';  
impressionShown = FILTER impressionShown BY type == 'on-domain' and creative_reason == 'null';  
impressionShown = DISTINCT impressionShown;

-- impression clicked
impressionClicked = FILTER CompleteData BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760080';
impressionClicked = FILTER impressionClicked BY custom is not null;
impressionClicked = FOREACH impressionClicked GENERATE custom#'EventData' as eData, header.associativeTag as vid, header.channelSessionId as bsid, header.timeEpochMillisUTC as epoch;
impressionClicked = FILTER impressionClicked BY eData is not null;
impressionClicked = FOREACH impressionClicked GENERATE JsonStringToMap(eData) as eDatajson, vid, bsid, epoch;
impressionClicked = FILTER impressionClicked BY eDatajson is not null ;
impressionClicked = FOREACH impressionClicked GENERATE eDatajson#'c'  as c, vid, bsid, epoch;
impressionClicked = FILTER impressionClicked BY c is not null;
impressionClicked = FOREACH impressionClicked GENERATE JsonStringToMap(c) as cjson, vid, bsid, epoch;
impressionClicked = FILTER impressionClicked BY cjson is not null;
impressionClicked = FOREACH impressionClicked GENERATE cjson#'optdata' as optdata, cjson#'type' as type, vid, bsid, epoch, cjson#'adreqid' as adreqid, cjson#'adid' as adid;
impressionClicked = FILTER impressionClicked BY optdata is not null;
impressionClicked = FOREACH impressionClicked GENERATE JsonStringToMap(optdata) as optjson, vid, type, bsid, epoch, adreqid, adid;
impressionClicked = FILTER impressionClicked BY optjson is not null;
impressionClicked = FOREACH impressionClicked GENERATE vid, type, bsid, epoch, adreqid, adid, 
	optjson#'clickurl' as click_url,
	optjson#'currentplacement' as cp, 
	optjson#'currentpageurl' as current_url;
impressionClicked = FILTER impressionClicked BY type == 'on-domain';  
impressionClicked = DISTINCT impressionClicked;

-- (c1799c02-5e78-4c5f-b5a5-59ee5ba6fc89,on-domain,c1799c02-5e78-4c5f-b5a5-59ee5ba6fc89::3,1507267805962,9e5fb299-1f33-43ec-ba63-b3f6d3c2b29f,4230,http://www.optus.com.au/shop/broadband/home-broadband/plans?category=60Plan24M,AFEATURE-BROADBAND,http://www.optus.com.au/shop/broadband)

-- seenClicked = JOIN impressionShown BY adreqid LEFT OUTER, impressionClicked BY adreqid;
-- C = LIMIT seenClicked 100;
-- DUMP C;
-- [vid, type, bsid, epoch, adreqid, adid, prod_id, segment_opt, creative_reason, creative_id, segment_served, segment_creative_name, url, shown, lob, cp, group_opt], [vid, type, bsid, epoch, adreqid, adid, click_url, cp, current_url];

--(00bf6f01-3f71-416c-8c92-8ac18a068d50,on-domain,00bf6f01-3f71-416c-8c92-8ac18a068d50::5,1506922731577,01576d34-741e-4225-9f30-4b3ce62634bf,3196,Cable,Fixed_Suburb,null,1720903,Fixed_Suburb,Cable Suburb,http://www.optus.com.au/shop/broadband,YES,Fixed,AFEATURE-BROADBAND,control,00bf6f01-3f71-416c-8c92-8ac18a068d50,on-domain,00bf6f01-3f71-416c-8c92-8ac18a068d50::6,1506922740297,01576d34-741e-4225-9f30-4b3ce62634bf,3196,http://www.optus.com.au/shop/broadband/home-broadband/plans?SID=con:bb:person:fixed:afeat:suburb:cable,AFEATURE-BROADBAND,http://www.optus.com.au/shop/broadband)
-- (00bf6f01-3f71-416c-8c92-8ac18a068d50,on-domain,00bf6f01-3f71-416c-8c92-8ac18a068d50::6,1506922740146,01576d34-741e-4225-9f30-4b3ce62634bf,3196,Cable,Fixed_Suburb,null,1720903,Fixed_Suburb,Cable Suburb,http://www.optus.com.au/shop/broadband,YES,Fixed,AFEATURE-BROADBAND,control,00bf6f01-3f71-416c-8c92-8ac18a068d50,on-domain,00bf6f01-3f71-416c-8c92-8ac18a068d50::6,1506922740297,01576d34-741e-4225-9f30-4b3ce62634bf,3196,http://www.optus.com.au/shop/broadband/home-broadband/plans?SID=con:bb:person:fixed:afeat:suburb:cable,AFEATURE-BROADBAND,http://www.optus.com.au/shop/broadband)

-- (00963580-516a-4a2c-aaac-761997d92c81,on-domain,00963580-516a-4a2c-aaac-761997d92c81::1,1507201305819,60e3429e-c4e6-496b-9b01-9a1a09ff2268,3512,samsung/galaxy-s8,Switcher,null,1714771,Switcher,SWITCHER_GS8,http://www.optus.com.au/shop/Mobile-Site/Index?CID=short:preinstll:exstg:DFLTmobihm,YES,PostPaid,BFEATURE,testAPI,00963580-516a-4a2c-aaac-761997d92c81,on-domain,00963580-516a-4a2c-aaac-761997d92c81::2,1507201336724,60e3429e-c4e6-496b-9b01-9a1a09ff2268,3512,https://offer.optus.com.au/shop/mobile/phones/samsung/galaxy-s8?plantype=lease?SID=con:mobi:person:switch:afeat:GS8:FOM,BFEATURE,http://www.optus.com.au/shop/Mobile-Site/Index?CID=short:preinstll:exstg:DFLTmobihm)

seenAndClicked = JOIN impressionShown BY adreqid, impressionClicked BY adreqid;
seenAndClicked = DISTINCT seenAndClicked;
--D = LIMIT seenAndClicked 100;
--DUMP D;

-- all counts, 1104
LOGS_GROUP= GROUP seenAndClicked ALL;
LOG_COUNT = FOREACH LOGS_GROUP GENERATE COUNT(seenAndClicked);
DUMP LOG_COUNT;

seenAndClickedSessionMatched = FILTER seenAndClicked BY $2 == $19;
seenAndClickedSessionMatched = DISTINCT seenAndClickedSessionMatched;

-- counts, 829
LOGS_GROUP2= GROUP seenAndClickedSessionMatched ALL;
LOG_COUNT2 = FOREACH LOGS_GROUP2 GENERATE COUNT(seenAndClickedSessionMatched);
DUMP LOG_COUNT2;

-- 06: 120,725, 85058
-- 05, 06, 07: 408,338

-- [vid, type, bsid, epoch, adreqid, adid, prod_id, segment_opt, creative_reason, creative_id, segment_served, segment_creative_name, url, shown, lob, cp, group_opt], [vid, type, bsid, epoch, adreqid, adid, click_url, cp, current_url];
-- seenAndClickedSessionMatched = FOREACH seenAndClickedSessionMatched GENERATE $0, $1, $2, $3, $4, $5, $6, $7
