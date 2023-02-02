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

Once that is done you should have 48 different indices named junoslogs-2023.01.DD-HH created in ElasticSearch, each containing 100 documents (NOTE: the actual production instance contains ~9M documents per index)

Confirm the size of the indexes by running
```bash
curl http://elastic:changeme@localhost:9200/_cat/indices
```

You can also see the number of documents in an index by e.g. running (see hits.total)
```bash
curl "http://elastic:changeme@localhost:9200/junoslogs-2023.01.01-05/_search" -d '{"query": {"match_all":{}}}'
```

## Local run
After completing the setup, you can test the program locally with
```bash
dotnet run --testrun
```
This will do all the work (scan indexes and populate the database) so it's not exactly idempotent
