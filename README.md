# EsToSql
A console app to scrape ElasticSearch indices and upload to an MS Sql Server

## Setup
Do the following to create a test environment
```bash
docker compose up -d
# wait for a few seconds for es and mssql to start
dotnet restore
dotnet run --populate
```

Once that is done you should have 32 different indices named junoslogs-2023.01.DD-HH created in ElasticSearch, each containing 100 documents (NOTE: the actual production instance contains ~9M documents per index)

Confirm the size of the indexes by running
```bash
curl http://elastic:changeme@localhost:9200/_cat/indices
```

You can also see the number of documents in an index by e.g. running (see hits.total)
```bash
curl "http://elastic:changeme@localhost:9200/junoslogs-2023.01.01-05/_search" -d '{"size": 1, "query": {"match_all":{}}}'
```

## Local run
After completing the setup, you can test the program locally with
```bash
dotnet run --testrun
```
This will do all the work (scan indexes and populate the database) so it's not exactly idempotent

To connect to the dockerized ms sql server use
```bash
mssql-cli -S localhost -d tempdb -U sa -P P@ssword -C
```

This is the what the query being sent to Elastic when scraping the indexes looks like
```json
{"from":0,"size":1,"query":{"bool":{"should":[{"bool":{"must":[{"match":{"event_category":"antivirus"}},{"match":{"source_zone":"business-Wired"}}]}},{"bool":{"must":[{"match":{"event_category":"apptrack"}},{"match":{"application":"BITTORRENT"}},{"match":{"event_type":"APPTRACK_SESSION_CLOSE"}},{"match":{"reason":"Closed by junos-dynapp"}}]}},{"bool":{"must":[{"match":{"event_category":"webfilter"}},{"match":{"category":"Enhanced_Personals_and_Dating"}},{"match":{"event_type":"WEBIFLTER_URL_BLOCKED"}},{"match":{"source_zone":"crew-Wired"}},{"match":{"url":"edge.microsoft.com"}}]}},{"bool":{"must":[{"match":{"event_category":"webfilter"}},{"match":{"category":"TELEGRAM"}},{"match":{"event_type":"WEBIFLTER_URL_PERMITTED"}},{"match":{"source_zone":"crew-Wired"}},{"match":{"url":"tools.dvdvideosoft.com/stat.jso"}}]}},{"bool":{"must":[{"match":{"event_category":"firewall"}},{"match":{"reason":"Closed by junos-dynapp"}},{"match":{"event_type":"RT_FLOW_SESSION_CLOSE"}},{"match":{"source_zone":"crew-Wired"}}]}},{"bool":{"must":[{"match":{"event_category":"ips"}},{"match":{"event_type":"IDP_ATTACK_LOG_EVENT"}},{"match":{"source_zone":"crew-Wired"}},{"match":{"threat_severity":"HIGH"}}]}}]}}}
```

You can use it to debug with `curl`

## Publishing
Run `./publish.sh` (Mac only) to publish a win-x64 release. The ouput file is placed under `publish/es_to_sql.zip`
