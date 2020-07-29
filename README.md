# covid-hate-speech

See https://github.com/VeenaArv/covid-hate-speech for all code.

All Java code is written by Veena Arvind to process tweets and Python code is written by Yusu Qian to process weibo

For processing tweets: 
1. `data_ingest.md` provides info on how to process the input 
2. ETL code under src/us/
    writable: contains custom writable objects to store information about tweets
    utils: contains utils used by Mappers and Reducers and well as pipeline counters 
    main class: PipelineMain.java consists of 3 jobs (extractTweets, annotateTweets, writeToParquet) 
    check schema.jsonl for parquet schema
3. Impala code under output.md

** CovidGovernmentResponseWritable.java and CSVToCovidGovernmentResponse.java is not used to produce any output nor joined with tweets. 

Data Sources:
Kerchner, Daniel; Wrubel, Laura, 2020, "Coronavirus Tweet Ids", https://doi.org/10.7910/DVN/LW0BTB, Harvard Dataverse, V7
Hale, Thomas, Sam Webster, Anna Petherick, Toby Phillips, and Beatriz Kira (2020). Oxford COVID-19 Government Response Tracker, Blavatnik School of Government. 
