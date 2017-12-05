--  Group by segment, product & plan    
    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=02/day=19/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    -- FifteenthEvent  = FILTER FifteenthEvent BY header.associativeTag == '9da31b36-be56-406b-a56c-648dab1243f2';

    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'segmentOndomain' as segmentOndomain, mOutput#'productsOndomain' as productsOndomain;

    FifteenthEvent = FILTER FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE JsonStringToMap ( segmentOndomain ) as segmentOndomain, JsonStringToMap ( productsOndomain ) as productsOndomain;
    FifteenthEvent = FILTER  FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE segmentOndomain#'AFEATURE-BUSINESS' as segment, productsOndomain#'AFEATURE-BUSINESS' as products;
    FifteenthEvent = FILTER FifteenthEvent BY segment is not null and products is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE segment, STRSPLIT(products, '"product":"').$1 as products, STRSPLIT(products, '"plan":"').$1 as plan ;
    FifteenthEvent = FILTER FifteenthEvent BY products is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE segment, STRSPLIT( products, '"' ).$0 as products,  STRSPLIT( plan, '"' ).$0 as plan;
    FifteenthEvent = FILTER  FifteenthEvent BY products is not null and plan is not null;

    FifteenthEvent = GROUP FifteenthEvent BY ( segment, products, plan ) ;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE FLATTEN( group ), COUNT (FifteenthEvent);
    
    STORE FifteenthEvent INTO 'Feb19_afeature__segment_products_plan.count';


        
    

--  Hotlead-Conversions
    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    idmData = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2016}/month=11/day=*/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER idmData BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    
    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData, header.associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput, associativeTag;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'segmentOndomain' as segmentOndomain, associativeTag;

    FifteenthEvent = FILTER FifteenthEvent BY segmentOndomain is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE JsonStringToMap ( segmentOndomain ) as segmentOndomain, associativeTag;
    FifteenthEvent = FILTER  FifteenthEvent BY segmentOndomain is not null ;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE segmentOndomain#'AFEATURE' as segment, associativeTag ;
    FifteenthEvent = FILTER FifteenthEvent BY segment is not null ;

    FifteenthEvent = DISTINCT FifteenthEvent;

    FifteenthEvent = GROUP FifteenthEvent BY segment;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT(FifteenthEvent);
    DUMP

    
    FifteenthEvent = FILTER idmData BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    
    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData, header.associativeTag, header.timeEpochMillisUTC;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData, associativeTag, timeEpochMillisUTC;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c, associativeTag, timeEpochMillisUTC;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c, associativeTag, timeEpochMillisUTC;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res, associativeTag, timeEpochMillisUTC;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res, associativeTag, timeEpochMillisUTC;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput, associativeTag, timeEpochMillisUTC;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput, associativeTag, timeEpochMillisUTC;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'segmentOndomain' as segmentOndomain, associativeTag, timeEpochMillisUTC;

    FifteenthEvent = FILTER FifteenthEvent BY segmentOndomain is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE JsonStringToMap ( segmentOndomain ) as segmentOndomain, associativeTag, timeEpochMillisUTC;
    FifteenthEvent = FILTER  FifteenthEvent BY segmentOndomain is not null ;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE segmentOndomain#'AFEATURE' as segment, associativeTag, timeEpochMillisUTC ;
    FifteenthEvent = FILTER FifteenthEvent BY segment is not null ;

    FifteenthEvent = DISTINCT FifteenthEvent;

    

    WebPurchaseEvent = FILTER idmData BY specificEventType == 'WebPurchaseEvent';
      WebPurchaseEvent = FOREACH WebPurchaseEvent GENERATE header.associativeTag, header.timeEpochMillisUTC;

      
      Event11122 = FILTER idmData BY specificEventType == 'WebCustomEvent' and body#'customEventType' == '11122';
      Event11122 = FOREACH Event11122 GENERATE  header.associativeTag, header.timeEpochMillisUTC;

      
      SliderConversions = FILTER idmData BY specificEventType == 'WebCustomEvent' and body#'customEventType' == '400104';
      SliderConversions = FOREACH SliderConversions GENERATE  header.associativeTag, header.timeEpochMillisUTC, custom#'EventData' as eventData;
      SliderConversions = FILTER  SliderConversions BY eventData is not null;
      SliderConversions = FOREACH  SliderConversions GENERATE associativeTag, timeEpochMillisUTC, JsonStringToMap( eventData )#'f' as f;
      SliderConversions = FILTER  SliderConversions BY f is not null;
      SliderConversions = FOREACH  SliderConversions GENERATE associativeTag, timeEpochMillisUTC, JsonStringToMap ( f )#'c' as c;
      SliderConversions = FILTER  SliderConversions BY c is not null;
      SliderConversions = FOREACH  SliderConversions GENERATE associativeTag, timeEpochMillisUTC, JsonStringToMap ( c )#'c15' as c15, JsonStringToMap ( c )#'c18' as c18;
      SliderConversions = FILTER SliderConversions BY c15 is not null and c18 is not null;
      SliderConversions = FILTER  SliderConversions BY c15 MATCHES '.*Accept.*' and c18 MATCHES '.*SL.*';
      SliderConversions = FOREACH SliderConversions GENERATE associativeTag, timeEpochMillisUTC;

      
      -- OfflineConversions = FILTER idmData BY specificEventType == 'WebPageLoadEvent';
      -- OfflineConversions = FILTER OfflineConversions BY ( body#'pageURL' MATCHES '.*TELE.*ORDERCONFIRMATION.*POST.*' or body#'pageURL' MATCHES '.*RET.*ORDERCONFIRMATION.*POST.*' ) ;
      -- OfflineConversions = FOREACH OfflineConversions GENERATE header.associativeTag, header.timeEpochMillisUTC;

      Conversions = UNION WebPurchaseEvent, Event11122, SliderConversions;--, OfflineConversions;
      Conversions = DISTINCT  Conversions;


    iConvData = JOIN FifteenthEvent BY associativeTag, Conversions by associativeTag;
    iConvData = FILTER iConvData BY (Conversions::timeEpochMillisUTC - FifteenthEvent::timeEpochMillisUTC > 0);
    iConvData = FOREACH iConvData GENERATE FifteenthEvent::associativeTag as vid,  FifteenthEvent::segment as seg;

    iConvData = DISTINCT iConvData;
    iConvData = GROUP iConvData BY seg;
    iConvData = FOREACH  iConvData GENERATE group, COUNT(iConvData);


--
    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2016}/month=12/day={0[1-9]}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    
    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData, header.channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput, channelSessionId;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'segmentOndomain' as segmentOndomain, mOutput#'productsOndomain' as productsOndomain, channelSessionId, mOutput#'productsForLogging' as productsForLogging;

    FifteenthEvent = FILTER FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE JsonStringToMap ( segmentOndomain ) as segmentOndomain, JsonStringToMap ( productsOndomain ) as productsOndomain, channelSessionId, productsForLogging;
    FifteenthEvent = FILTER  FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE segmentOndomain#'AFEATURE' as segment, productsOndomain#'AFEATURE' as products, channelSessionId, productsForLogging;
    FifteenthEvent = FILTER FifteenthEvent BY segment is not null and products is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE segment, STRSPLIT(products, '"product":"').$1 as products, STRSPLIT(products, '"plan":"').$1 as plan, channelSessionId, productsForLogging ;
    FifteenthEvent = FILTER FifteenthEvent BY products is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE segment, STRSPLIT( products, '"' ).$0 as products,  STRSPLIT( plan, '"' ).$0 as plan, channelSessionId, productsForLogging;
    -- FifteenthEvent = FILTER  FifteenthEvent BY products is not null and plan is not null;

    FifteenthEvent = FILTER FifteenthEvent BY segment == 'SMBSpecificPhones';

    FifteenthEvent = DISTINCT FifteenthEvent;

    STORE FifteenthEvent INTO 'SMBSpecificPhones_Dec_1_11.tab';
    -- FifteenthEvent = GROUP FifteenthEvent BY ( segment, products, plan ) ;
    -- FifteenthEvent = FOREACH  FifteenthEvent GENERATE FLATTEN( group ), COUNT (FifteenthEvent);
    



    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2016}/month=12/day=20/hour=00/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    
    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData, header.channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput, channelSessionId;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'segmentOndomain' as segmentOndomain, mOutput#'productsOndomain' as productsOndomain, channelSessionId;

    FifteenthEvent = FILTER FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE JsonStringToMap ( segmentOndomain ) as segmentOndomain, JsonStringToMap ( productsOndomain ) as productsOndomain, channelSessionId;
    FifteenthEvent = FILTER  FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE segmentOndomain#'AFEATURE' as segment, productsOndomain#'AFEATURE' as products, channelSessionId;
    FifteenthEvent = FILTER FifteenthEvent BY segment is not null and products is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE segment, STRSPLIT(products, '"product":"').$1 as products, STRSPLIT(products, '"plan":"').$1 as plan, channelSessionId ;
    FifteenthEvent = FILTER FifteenthEvent BY products is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE segment, STRSPLIT( products, '"' ).$0 as products,  STRSPLIT( plan, '"' ).$0 as plan, channelSessionId;
    FifteenthEvent = FILTER  FifteenthEvent BY products is not null and plan is not null;

    FifteenthEvent = FILTER FifteenthEvent BY segment == 'Mobile' or segment == 'Switcher';
    STORE FifteenthEvent INTO 'MobileSwitcher.tab';

    -- FifteenthEvent = FILTER FifteenthEvent BY segment == 'Mobile' or segment == 'Switcher';
    -- FifteenthEvent = FOREACH FifteenthEvent GENERATE channelSessionId;
    -- DUMP 



    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2016}/month=12/day={23,24,25,26}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    
    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'segmentOffdomain' as segmentOffdomain, mOutput#'productsOndomain' as productsOffdomain;

    FifteenthEvent = FILTER FifteenthEvent BY segmentOffdomain is not null and productsOffdomain is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE STRSPLIT(productsOffdomain, '"product":"').$1 as products, STRSPLIT(productsOffdomain, '"plan":"').$1 as plan, segmentOffdomain ;
    FifteenthEvent = FILTER FifteenthEvent BY products is not null and plan is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE STRSPLIT( products, '"' ).$0 as products,  STRSPLIT( plan, '"' ).$0 as plan, segmentOffdomain;
    FifteenthEvent = FILTER  FifteenthEvent BY products is not null and plan is not null;


    FifteenthEvent = GROUP FifteenthEvent BY ( segmentOffdomain, products, plan );
    FifteenthEvent = FOREACH FifteenthEvent GENERATE FLATTEN(group), COUNT(FifteenthEvent);
    DUMP


    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2016}/month=12/day={23,24,25,26}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    
    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'segmentOffdomain' as segmentOffdomain, mOutput#'productsOndomain' as productsOffdomain;

    FifteenthEvent = FILTER FifteenthEvent BY segmentOffdomain is not null and productsOffdomain is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE 
    -- FifteenthEvent = FOREACH FifteenthEvent GENERATE STRSPLIT(productsOffdomain, '"product":"').$1 as products, STRSPLIT(productsOffdomain, '"plan":"').$1 as plan, segmentOffdomain ;
    -- FifteenthEvent = FILTER FifteenthEvent BY products is not null and plan is not null;

    -- FifteenthEvent = FOREACH  FifteenthEvent GENERATE STRSPLIT( products, '"' ).$0 as products,  STRSPLIT( plan, '"' ).$0 as plan, segmentOffdomain;
    -- FifteenthEvent = FILTER  FifteenthEvent BY products is not null and plan is not null;


    -- FifteenthEvent = GROUP FifteenthEvent BY ( segmentOffdomain, products, plan );
    -- FifteenthEvent = FOREACH FifteenthEvent GENERATE FLATTEN(group), COUNT(FifteenthEvent);
    -- DUMP


    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day=*/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent' ;;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE body#'pageURL' as url, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;;
    FifteenthEvent = FILTER FifteenthEvent BY url MATCHES '.*addToCart.*';
    FifteenthEvent = FILTER FifteenthEvent BY url MATCHES '.*externalPackageId=.*';
    FifteenthEvent = FOREACH FifteenthEvent GENERATE STRSPLIT(url, 'externalPackageId=').$1 as plan, CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;;
    FifteenthEvent = FILTER FifteenthEvent BY plan is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE STRSPLIT(plan, '&').$0 as plan, date;

    FifteenthEvent = GROUP FifteenthEvent BY ( date, plan);
    FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT( FifteenthEvent );
    STORE FifteenthEvent into 'byocartplansJan.tab'

    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day={0[1-9],1[0-5]}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    -- FifteenthEvent  = FILTER FifteenthEvent BY header.associativeTag == '9da31b36-be56-406b-a56c-648dab1243f2';

    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData,  CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c, date;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c, date;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res, date;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res, date;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput, date;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput, date;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'segmentOndomain' as segmentOndomain, mOutput#'productsOndomain' as productsOndomain, date;

    FifteenthEvent = FILTER FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE JsonStringToMap ( segmentOndomain ) as segmentOndomain, JsonStringToMap ( productsOndomain ) as productsOndomain, date;
    FifteenthEvent = FILTER  FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE segmentOndomain#'BFEATURE' as segment, productsOndomain#'BFEATURE' as products, date;
    FifteenthEvent = FILTER FifteenthEvent BY segment is not null and products is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE segment, STRSPLIT(products, '"product":"').$1 as products, STRSPLIT(products, '"plan":"').$1 as plan , date;
    FifteenthEvent = FILTER FifteenthEvent BY products is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE segment, STRSPLIT( products, '"' ).$0 as products,  STRSPLIT( plan, '"' ).$0 as plan, date;
    FifteenthEvent = FILTER  FifteenthEvent BY products is not null and plan is not null;

    -- FifteenthEvent = FILTER FifteenthEvent BY segment == 'ByoCart' ;
    
    FifteenthEvent = GROUP FifteenthEvent BY ( date, segment, products, plan ) ;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE FLATTEN( group ), COUNT (FifteenthEvent);
    
    STORE FifteenthEvent INTO 'bfeature_segment_product_plan_jan01_15_2017.count';
    

    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day=*/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent' ;;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE body#'pageURL' as url, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;;
    FifteenthEvent = FILTER FifteenthEvent BY url MATCHES '.*addToCart.*';
    FifteenthEvent = FILTER FifteenthEvent BY url MATCHES '.*externalPackageId=.*';
    FifteenthEvent = FOREACH FifteenthEvent GENERATE STRSPLIT(url, 'externalPackageId=').$1 as plan, CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;;
    FifteenthEvent = FILTER FifteenthEvent BY plan is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE STRSPLIT(plan, '&').$0 as plan, date;

    FifteenthEvent = GROUP FifteenthEvent BY ( date, plan);
    FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT( FifteenthEvent );
    STORE FifteenthEvent into 'byocartplansJan.tab'


    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2016}/month=11/day=15/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent';
    FifteenthEvent = FOREACH FifteenthEvent GENERATE LOWER ( body#'pageURL' ) as url;
    FifteenthEvent = FILTER FifteenthEvent BY url MATCHES '.*addbundletocart.*' and url MATCHES '.*business.*';
    DUMP



    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day=09/hour=05/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    
    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    
    FifteenthEvent = FOREACH FifteenthEvent GENERATE mOutput#'segmentForLotame' as segmentForLotame;
    FifteenthEvent = FILTER FifteenthEvent BY segmentForLotame is not null;
    FifteenthEvent = GROUP FifteenthEvent BY segmentForLotame;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT(FifteenthEvent);
    DUMP 

    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2016}/month=12/day={2[0-9],30,31}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent' ;;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE body#'pageURL' as url, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;;
    FifteenthEvent = FILTER FifteenthEvent BY url MATCHES '.*shop/mobile.*';
    FifteenthEvent = FOREACH FifteenthEvent GENERATE CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;
    FifteenthEvent = GROUP FifteenthEvent by date;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE  group, COUNT(FifteenthEvent);
    DUMP


    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day={0[1-7]}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    -- FifteenthEvent  = FILTER FifteenthEvent BY header.associativeTag == '9da31b36-be56-406b-a56c-648dab1243f2';

    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData,  CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c, date;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c, date;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res, date;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res, date;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput, date;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput, date;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'segmentOndomain' as segmentOndomain, mOutput#'productsOndomain' as productsOndomain, mOutput#'LOBOndomain' as LOBOndomain, date;

    FifteenthEvent = FILTER FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null and LOBOndomain is not null;



    FifteenthEvent = FOREACH  FifteenthEvent GENERATE JsonStringToMap ( segmentOndomain ) as segmentOndomain, JsonStringToMap ( productsOndomain ) as productsOndomain, JsonStringToMap(LOBOndomain) as LOBOndomain, date;
    FifteenthEvent = FILTER  FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE LOBOndomain#'AFEATURE' as lob, segmentOndomain#'AFEATURE' as segment, productsOndomain#'AFEATURE' as products, date;
    FifteenthEvent = FILTER FifteenthEvent BY lob is not null and segment is not null and products is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE lob, segment, STRSPLIT(products, '"product":"').$1 as products, STRSPLIT(products, '"plan":"').$1 as plan , date;
    FifteenthEvent = FILTER FifteenthEvent BY products is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE lob, segment, STRSPLIT( products, '"' ).$0 as products,  STRSPLIT( plan, '"' ).$0 as plan, date;
    FifteenthEvent = FILTER  FifteenthEvent BY products is not null and plan is not null;

    -- FifteenthEvent = FILTER FifteenthEvent BY segment == 'ByoCart' ;
    
    FifteenthEvent = GROUP FifteenthEvent BY ( date, lob, segment, products, plan ) ;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE FLATTEN( group ), COUNT (FifteenthEvent);

    store FifteenthEvent into 'lob_segment_product_plan.tab';


    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day={0[1-7]}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    -- FifteenthEvent  = FILTER FifteenthEvent BY header.associativeTag == '9da31b36-be56-406b-a56c-648dab1243f2';

    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData,  CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c, date;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c, date;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res, date;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res, date;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput, date;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput, date;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'segmentOndomain' as segmentOndomain, mOutput#'productsOndomain' as productsOndomain, mOutput#'LOB' as lob, date;

    FifteenthEvent = FILTER FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null and lob is not null;


    FifteenthEvent = FOREACH  FifteenthEvent GENERATE JsonStringToMap ( segmentOndomain ) as segmentOndomain, JsonStringToMap ( productsOndomain ) as productsOndomain, lob, date;
    FifteenthEvent = FILTER  FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE lob, segmentOndomain#'AFEATURE' as segment, productsOndomain#'AFEATURE' as products, date;
    FifteenthEvent = FILTER FifteenthEvent BY lob is not null and segment is not null and products is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE lob, segment, STRSPLIT(products, '"product":"').$1 as products, STRSPLIT(products, '"plan":"').$1 as plan , date;
    FifteenthEvent = FILTER FifteenthEvent BY products is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE lob, segment, STRSPLIT( products, '"' ).$0 as products,  STRSPLIT( plan, '"' ).$0 as plan, date;
    FifteenthEvent = FILTER  FifteenthEvent BY products is not null and plan is not null;

    -- FifteenthEvent = FILTER FifteenthEvent BY segment == 'ByoCart' ;
    
    FifteenthEvent = GROUP FifteenthEvent BY ( date, lob, segment, products, plan ) ;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE FLATTEN( group ), COUNT (FifteenthEvent);

    store FifteenthEvent into 'lob_segment_product_plan_updated.tab';



    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day=06/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    -- FifteenthEvent  = FILTER FifteenthEvent BY header.associativeTag == '9da31b36-be56-406b-a56c-648dab1243f2';

    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData,  CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c, date;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c, date;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res, date;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res, date;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput, date;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput, date;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'LOB' as lob ;

    FifteenthEvent = GROUP FifteenthEvent BY lob;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT( FifteenthEvent );



    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2016}/month=12/day=*/hour=10/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    -- FifteenthEvent  = FILTER FifteenthEvent BY header.associativeTag == '9da31b36-be56-406b-a56c-648dab1243f2';

    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData,  CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c, date;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c, date;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res, date;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res, date;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput, date;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput, date;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'productsOndomain' as productsOndomain , date;
    FifteenthEvent = FILTER FifteenthEvent BY productsOndomain is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE  date, JsonStringToMap ( productsOndomain ) as productsOndomain ;
    FifteenthEvent = FILTER  FifteenthEvent By productsOndomain is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE productsOndomain#'AFEATURE' as AFEATURE, date;

    FifteenthEvent = GROUP FifteenthEvent BY (date, AFEATURE);
    FifteenthEvent = FOREACH FifteenthEvent GENERATE FLATTEN( group ), COUNT( FifteenthEvent );

    STORE FifteenthEvent into '151.tab';


    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day={1[0-5]}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    -- FifteenthEvent  = FILTER FifteenthEvent BY header.associativeTag == '9da31b36-be56-406b-a56c-648dab1243f2';

    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData, header.associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput, associativeTag;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'segmentOndomain' as segmentOndomain, mOutput#'productsOndomain' as productsOndomain, associativeTag;

    FifteenthEvent = FILTER FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE JsonStringToMap ( segmentOndomain ) as segmentOndomain, JsonStringToMap ( productsOndomain ) as productsOndomain, associativeTag;
    FifteenthEvent = FILTER  FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE segmentOndomain#'AFEATURE' as segment, productsOndomain#'AFEATURE' as products, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY segment is not null and products is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE segment, STRSPLIT(products, '"product":"').$1 as products, STRSPLIT(products, '"plan":"').$1 as plan , associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY products is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE segment, STRSPLIT( products, '"' ).$0 as products,  STRSPLIT( plan, '"' ).$0 as plan, associativeTag;
    FifteenthEvent = FILTER  FifteenthEvent BY products is not null and plan is not null;

    FifteenthEvent = DISTINCT FifteenthEvent;

    FifteenthEvent = GROUP FifteenthEvent BY ( segment, products, plan ) ;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE FLATTEN( group ), COUNT (FifteenthEvent);
    
    STORE FifteenthEvent INTO 'jan_afeature__segment_products_plan_jan10_15.count';


    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day={01,02,03,04,05,06,07}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent' ;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE LOWER ( body#'pageURL' ) as url, header.channelSessionId;    
    FifteenthEvent = FILTER FifteenthEvent BY url MATCHES '.*deal.*';
    FifteenthEvent = FOREACH FifteenthEvent GENERATE channelSessionId;
    FifteenthEvent = DISTINCT FifteenthEvent;
    FifteenthEvent = GROUP FifteenthEvent ALL;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE COUNT(FifteenthEvent);
    DUMP 


    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day={01,02,03,04,05,06,07}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent' ;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE LOWER ( body#'pageURL' ) as url, header.associativeTag;    
    FifteenthEvent = FILTER FifteenthEvent BY url MATCHES '.*deal.*';
    FifteenthEvent = FOREACH FifteenthEvent GENERATE associativeTag;
    FifteenthEvent = DISTINCT FifteenthEvent;
    FifteenthEvent = GROUP FifteenthEvent ALL;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE COUNT(FifteenthEvent);
    DUMP 

    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 

    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day={01,02,03,04,05,06,07}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent' ;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE LOWER ( body#'pageURL' ) as url;
    
    FifteenthEvent = FOREACH FifteenthEvent GENERATE STRSPLIT(url, '\\?').$0 as url;
        
    FifteenthEvent = GROUP FifteenthEvent BY url;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT(FifteenthEvent);
    STORE FifteenthEvent INTO 'dealUrlVolume.tab';

    
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 

    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day={01,02,03,04,05,06,07}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent' ;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE LOWER ( body#'pageURL' ) as url, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;;

    FifteenthEvent = FILTER FifteenthEvent BY url MATCHES '.*keyword_k.*';
    FifteenthEvent = FOREACH FifteenthEvent GENERATE   CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;
    FifteenthEvent = GROUP FifteenthEvent BY date;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT(FifteenthEvent);
    DUMP 


    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day=09/hour=00/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent' ;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE LOWER ( body#'pageURL' ) as url, header.associativeTag;    
    FifteenthEvent = FILTER FifteenthEvent BY url MATCHES '.*business.*' and MATCHES '.*broadband.*';


    FifteenthEvent = GROUP FifteenthEvent ALL;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE COUNT(FifteenthEvent);



    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 

    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day=09/hour=00/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent' ;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE LOWER ( body#'pageURL' ) as url;

    FifteenthEvent = FILTER FifteenthEvent BY url MATCHES '.*ca_source.*';
    
    FifteenthEvent = GROUP FifteenthEvent ALL;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE COUNT(FifteenthEvent);
    DUMP 
    
    -- FifteenthEvent = FOREACH FifteenthEvent GENERATE STRSPLIT(url, '\\?').$0 as url;
    -- FifteenthEvent = GROUP FifteenthEvent BY url;
    -- FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT(FifteenthEvent);
    -- DUMP 




    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day={0[1-9],1[0-5]}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    -- FifteenthEvent  = FILTER FifteenthEvent BY header.associativeTag == '9da31b36-be56-406b-a56c-648dab1243f2';

    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData,  CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c, date;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c, date;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res, date;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res, date;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput, date;

    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput, date;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE mOutput#'segmentOndomain' as segmentOndomain, mOutput#'productsOndomain' as productsOndomain, mOutput#'LOB' as lob, date;

    FifteenthEvent = FILTER FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null and lob is not null;


    FifteenthEvent = FOREACH  FifteenthEvent GENERATE JsonStringToMap ( segmentOndomain ) as segmentOndomain, JsonStringToMap ( productsOndomain ) as productsOndomain, lob, date;
    FifteenthEvent = FILTER  FifteenthEvent BY segmentOndomain is not null and productsOndomain is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE lob, segmentOndomain#'AFEATURE' as segment, productsOndomain#'AFEATURE' as products, date;
    FifteenthEvent = FILTER FifteenthEvent BY lob is not null and segment is not null and products is not null;

    FifteenthEvent = FOREACH FifteenthEvent GENERATE lob, segment, STRSPLIT(products, '"product":"').$1 as products, STRSPLIT(products, '"plan":"').$1 as plan , date;
    FifteenthEvent = FILTER FifteenthEvent BY products is not null;

    FifteenthEvent = FOREACH  FifteenthEvent GENERATE lob, segment, STRSPLIT( products, '"' ).$0 as products,  STRSPLIT( plan, '"' ).$0 as plan, date;
    FifteenthEvent = FILTER  FifteenthEvent BY products is not null and plan is not null;

    -- FifteenthEvent = FILTER FifteenthEvent BY segment == 'ByoCart' ;
    
    FifteenthEvent = GROUP FifteenthEvent BY ( date, lob, segment, products, plan ) ;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE FLATTEN( group ), COUNT (FifteenthEvent);

    store FifteenthEvent into 'lob_segment_product_plan_updated.tab';





    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc'); 

    iData = LOAD '/raw/prod/rtdp/idm/events/optus/year={2017}/month={01}/day=*/hour=*/min=*/*.avro' USING LOAD_IDM ; /**/

    Evaluation = FILTER iData BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760002';

    Evaluation = FOREACH  Evaluation GENERATE header.associativeTag as vid, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;;
    Evaluation = FOREACH Evaluation GENERATE vid, CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;

    
    Evaluation = FOREACH Evaluation GENERATE  (SIZE(vid) == 36 ? 1 : 0) as vid_ind, date;
    Evaluation = GROUP Evaluation BY ( date, vid_ind);
    Evaluation = FOREACH Evaluation GENERATE FLATTEN( group ), COUNT(Evaluation);
    dump; 




    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_current.avsc'); 

    iData = LOAD '/raw/prod/rtdp/idm/events/optus/year={2017}/month={01}/day=*/hour=*/min=*/*.avro' USING LOAD_IDM ; /**/

    Evaluation = FILTER iData BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760002';

    Evaluation = FOREACH  Evaluation GENERATE header.associativeTag as vid, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;;
    Evaluation = FOREACH Evaluation GENERATE vid, CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;
    Evaluation = FOREACH Evaluation GENERATE  vid, (SIZE(vid) == 36 ? 1 : 0) as vid_ind, date;
    Evaluation = DISTINCT Evaluation;
    Evaluation = GROUP Evaluation BY ( date, vid_ind);
    Evaluation = FOREACH Evaluation GENERATE FLATTEN( group ), COUNT(Evaluation);
    dump; 



    --  Group by segment, product & plan    

    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 

    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2016}/month=02/day=15/hour=00/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent by specificEventType == 'WebSessionStartEvent';
    FifteenthEvent = FOREACH FifteenthEvent GENERATE body#'searchTermsExternal' as searchTermsExternal;
    FifteenthEvent = FILTER FifteenthEvent BY searchTermsExternal != 'UNKNOWN';
    FifteenthEvent = LIMIT FifteenthEvent 100;
    DUMP 



     register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=02/day={13,14,15,16,17}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    
    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    
    FifteenthEvent = FOREACH FifteenthEvent GENERATE mOutput#'segmentForLotame' as segmentForLotame;
    FifteenthEvent = FILTER FifteenthEvent BY segmentForLotame is not null;
    FifteenthEvent = GROUP FifteenthEvent BY segmentForLotame;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT(FifteenthEvent);
    DUMP 


    register elephant-bird-pig-3.0.9.jar;
    register json_simple-1.1.jar;
    DEFINE JsonStringToMap com.twitter.elephantbird.pig.piggybank.JsonStringToMap() ;
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=02/day={13,14,15,16,17}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent' and body#'customEventType' MATCHES '760015';
    
    FifteenthEvent = FILTER  FifteenthEvent BY custom is not null;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE  custom#'EventData' as eData, header.associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(eData) as eData, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY eData is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE eData#'c'  as c, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(c) as c, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY c is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE c#'res'  as res, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(res) as res, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY res is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE res#'VisitorProfilerJS_optus_identity_environment_label_production'  as mOutput, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE JsonStringToMap(mOutput) as mOutput, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY mOutput is not null;
    
    FifteenthEvent = FOREACH FifteenthEvent GENERATE mOutput#'segmentForLotame' as segmentForLotame, associativeTag;
    FifteenthEvent = FILTER FifteenthEvent BY segmentForLotame is not null;
    FifteenthEvent = DISTINCT FifteenthEvent;
    FifteenthEvent = GROUP FifteenthEvent BY segmentForLotame;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT(FifteenthEvent);
    DUMP 




    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/siriusxm/year={2017}/month=02/day=21/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FOREACH FifteenthEvent GENERATE specificEventType;
    FifteenthEvent = GROUP FifteenthEvent BY specificEventType;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT(FifteenthEvent);
    DUMP 


    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/medibank/year={2017}/month=02/day=22/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebCustomEvent';
    FifteenthEvent = FOREACH FifteenthEvent GENERATE body#'customEventType' as eType;
    FifteenthEvent = GROUP FifteenthEvent BY eType;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT(FifteenthEvent);
    DUMP 


    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=02/day=21/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FOREACH FifteenthEvent GENERATE header.associativeTag;
    FifteenthEvent = LIMIT FifteenthEvent 100;
    DUMP 


    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc'); 


    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/siriusxm/year={2017}/month=02/day=21/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FOREACH FifteenthEvent GENERATE header.associativeTag;
    FifteenthEvent = LIMIT FifteenthEvent 10;
    DUMP 
    -- FifteenthEvent = FOREACH FifteenthEvent GENERATE specificEventType;
    -- FifteenthEvent = GROUP FifteenthEvent BY specificEventType;
    -- FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT(FifteenthEvent);
    -- DUMP 
    

    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc');

    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/optus/year={2017}/month=01/day=*/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent' ;;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE body#'pageURL' as url, GetMonth(ToDate(header.timeEpochMillisUTC + 28800000 + 39600000)) as month:chararray, GetDay(ToDate(header.timeEpochMillisUTC + 28800000+ 39600000)) as day:chararray,
        GetYear( ToDate(header.timeEpochMillisUTC + 28800000+ 39600000) ) as year:chararray;;
    FifteenthEvent = FILTER FifteenthEvent BY url MATCHES '.*shop/mobile/phones/sim-only.*';    
    FifteenthEvent = FOREACH FifteenthEvent GENERATE  CONCAT(CONCAT(CONCAT(year, '-'), CONCAT(month, '-')), day) as date;;    
    FifteenthEvent = GROUP FifteenthEvent BY date;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE group, COUNT( FifteenthEvent );
    DUMP


    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc');

    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/sirius/year={2017}/month=02/day={26}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent' ;
    FifteenthEvent = GROUP FifteenthEvent ALL;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE COUNT(FifteenthEvent);
    DUMP

    
    DEFINE LOAD_IDM  com.tfs.idm.pig.IDMEventLoadFunc('hdfs:///lib/idm-schemas/eventIdmSchema_v7.avsc');

    FifteenthEvent = LOAD  '/raw/prod/rtdp/idm/events/sirius/year={2017}/month=02/day={26}/hour=*/min=*/*.avro' USING LOAD_IDM;     
    FifteenthEvent = FILTER FifteenthEvent BY specificEventType == 'WebPageLoadEvent' ;
    FifteenthEvent = FOREACH FifteenthEvent GENERATE body#'pageURL' as url;
    FifteenthEvent = GROUP FifteenthEvent BY url;
    FifteenthEvent = FOREACH  FifteenthEvent GENERATE group, COUNT(FifteenthEvent);
    STORE FifteenthEvent INTO 'sirisusXmUrlVol.tab';