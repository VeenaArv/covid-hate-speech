# Ingesting Data 
## Dataset 
```
Kerchner, Daniel; Wrubel, Laura, 2020, "Coronavirus Tweet Ids", https://doi.org/10.7910/DVN/LW0BTB, Harvard Dataverse, V7
```

## Hydrating Tweets 
Twitter policy only allows tweet ids to be shared for acedemic purposes. In order to get the full tweet, we must hydrate the tweet ids. 
We used the library [twarc](https://github.com/DocNow/twarc) to do this.
```
twarc hydrate data/coronavirus-through-09-June-2020-00.txt > json_data/coronavirus-through-09-June-2020-00
```
Each file contains 100M tweets. We sart by hydrating 20M tweets with file size ~14GB

## Load into HDFS

1. Copy to dumbo 
```
scp  json_data/coronavirus-through-09-June-2020-00 vla240@dumbo.es.its.nyu.edu:
```


