# es-index-s3-archive
Archive Elasticsearch indices into S3 compatible storage like Ceph  

#### Index name pattern
only archive index name in the pattern of 
 ```
 index-pattern-YYYY.MM.DD
 ```
 
 #### Runtime
Tested on  
Elaticsearch 7.7.1  

#### ENV needed
MONGOSERVICE  
ARCHIVE_NAME  
ES_HOSTS  
S3AK  
S3SK  
S3HOST  

#### Elasticsearch S3 keystore setting
```shell
bin/elasticsearch-keystore add s3.client.default.access_key
bin/elasticsearch-keystore add s3.client.default.secret_key
```

then
```POST _nodes/reload_secure_settings```