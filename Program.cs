using Microsoft.Extensions.Configuration;
using System.Dynamic;
using System.Text;
using Newtonsoft.Json.Linq;
using log4net;

internal class Program
{
  static IConfigurationRoot config = CreateConfig();
  static DateTime now = DateTime.UtcNow;
  private static readonly ILog log = LogManager.GetLogger(typeof(Program));

  static Program()
  {
    Log4NetSetup.Setup();
  }

  static object[] matches = new object[]{
    new {event_category = "antivirus", source_zone = "business-Wired"},
    new {event_category = "apptrack", application = "BITTORRENT", event_type = "APPTRACK_SESSION_CLOSE", reason = "Closed by junos-dynapp"},
    new {event_category = "webfilter", category = "Enhanced_Personals_and_Dating", event_type = "WEBIFLTER_URL_BLOCKED", source_zone = "crew-Wired", url = "edge.microsoft.com"},
    new {event_category = "webfilter", category = "TELEGRAM", event_type = "WEBIFLTER_URL_PERMITTED", source_zone = "crew-Wired", url = "tools.dvdvideosoft.com/stat.jso"},
    new {event_category = "firewall", reason = "Closed by junos-dynapp", event_type = "RT_FLOW_SESSION_CLOSE", source_zone = "crew-Wired"},
    new {event_category = "ips", event_type = "IDP_ATTACK_LOG_EVENT", source_zone = "crew-Wired", threat_severity = "HIGH"},
  };

  private static async Task Main(string[] args)
  {
    try
    {
      await Execute(args);
    }
    catch (Exception ex)
    {
      log.Error("Program failed.", ex);
    }
  }

  private static async Task Execute(string[] args)
  {
    var start = now;
    log.Info($"-------------------------------- Start program at {start} --------------------------------");
    var elasticClient = Elastic.CreateElasticSearchClient(config);
    var connection = Database.CreateSqlConnection(config);

    if (args.Length > 0)
    {
      switch (args[0])
      {
        case "--populate":
          EnsureLocal(config);
          var dummyIndexes = await DocumentGenerator.CreateIndexes(elasticClient);
          await DocumentGenerator.PopulateIndexes(elasticClient, dummyIndexes);
          return;
        case "--testrun":
          EnsureLocal(config);
          await Database.CreateLogTableIfNotExists(connection);
          await Database.CreateDocumentsTableIfNotExists(connection);
          // use a fixed timestamp to make sure we'll scan among the existing local indexes
          now = DateTime.Parse("2023-01-02T09:00:00Z").ToUniversalTime();
          break;
      }
    }

    // Get all the indexes available in ElasticSearch
    log.Info("Getting all available indexes from Elastic...");
    var indexes = await Elastic.ListIndexesOrdered(elasticClient);
    log.Info($"Finished getting all available indexes. Indexes found = [{string.Join(", ", indexes)}], count={indexes.Count()}");

    // Calculate the last index to scan. Indexes are named like junoslogs-2023.01.01-13
    // ie they have a date+hour timestamp in their name. The last index will be the time_now_utc - 2h
    // index just to make sure there are no issues with clock skew between the servers
    var maxIndexTime = now.AddHours(-2);
    var maxIndex = $"junoslogs-{maxIndexTime.Year}.{maxIndexTime.Month.ToString("D2")}.{maxIndexTime.Day.ToString("D2")}-{maxIndexTime.Hour.ToString("D2")}";
    log.Info($"Based on current time = {now}, the max index to scan will be {maxIndex}");

    // Filter out all the indexes that are greater than the last index calculated above
    // because writing may not have finished there
    var candidateIndexes = indexes.TakeWhile(idx => string.Compare(idx, maxIndex) < 1).ToHashSet();
    log.Info($"Candidate indexes to scan: count={candidateIndexes.Count}, which=[{string.Join(",", candidateIndexes)}]");

    // Now scan the database for the indexes completed. For every match we're going to skip that index
    // because it would have been scanned in a previous run

    log.Info("Begin querying database for previously completed indexes scraped based on available indexes in Elastic...");

    var previouslyCompletedIndexes = (await Database.ExecuteSql(connection, "SELECT index_name FROM log_entries WHERE index_name IN (@indexes)", new { indexes = candidateIndexes }))
      .SelectMany(x => x.Values)
      .Cast<string>()
      .ToList();
    log.Info($"Finished querying database. Matched indexes previously completed: count={previouslyCompletedIndexes.Count}, which=[{string.Join(", ", previouslyCompletedIndexes)}]");

    // Skip the indexes that have already been completed
    var indexesToScan = candidateIndexes.Except(previouslyCompletedIndexes).ToList();
    log.Info($"Remaining indexes to scrape now: count={indexesToScan.Count}, which=[{string.Join(", ", indexesToScan)}]");

    // Time to fetch the documents from Elastic
    var documentsScrapedTotal = 0;
    log.Info("Begin scraping Elastic indexes...");
    var q = Elastic.GenerateJunosQuery(matches);
    int batchSize = int.Parse(config["ES_BATCH_SIZE"]), scrollTimeoutSeconds = int.Parse(config["ES_SCROLL_TIMEOUT_SECONDS"]);
    await Parallel.ForEachAsync(indexesToScan, async (index, _ct) =>
    {
      try
      {
        var documentsScrapedFromIndex = 0;
        log.Info($"Beging scraping index {index}....");
        await foreach (var page in Elastic.EnumerateAllDocumentsInIndex(elasticClient, index, batchSize, scrollTimeoutSeconds, q))
        {
          log.Info($"Fetched document batch {page.Name}");
          if (page.Documents.Count == 0)
          {
            log.Info($"Document batch {page.Name} is empty, index is Done");
            break;
          }
          // Now write the entire page of documents (1000) in one go to avoid
          // being too chatty
          log.Info($"Begin inserting document batch {page.Name}...");
          (var cmd, var cmdparams) = BuildCommand(page);
          await Database.ExecuteSql(connection, cmd, cmdparams);
          log.Info($"Finished inserting document batch {page.Name}...");
          Interlocked.Add(ref documentsScrapedTotal, page.Documents.Count);
          Interlocked.Add(ref documentsScrapedFromIndex, page.Documents.Count);
        }
        log.Info($"Finished scraping index {index} from Elastic. Documents scraped={documentsScrapedFromIndex}");

        // Let's log that we're done with that particular index
        log.Info($"Begin flagging index {index} as done in database...");
        await Database.ExecuteSql(connection, "INSERT INTO log_entries (index_name, inserted_at) VALUES (@index_name, @inserted_at)", new { index_name = index, inserted_at = DateTime.UtcNow });
        log.Info($"Finished flagging index {index} as done in database");
      }
      catch (Exception ex)
      {
        log.Error($"Failed while scraping index {index}. Will continue with rest indexes...", ex);
      }
    });

    var end = DateTime.UtcNow;
    log.Info($"----------- End program at {end}. Total docs scraped = {documentsScrapedTotal}, time ellapsed = {(end - start).TotalSeconds.ToString("F1")} secs --------------\n");
  }

  private static (string, object) BuildCommand(Elastic.Page page)
  {
    (var builder, var cmdparams) =
    page.Documents
   .Select((doc, i) =>
   {
     var _source = doc["_source"] as JObject;
     var cmdparams = new Dictionary<string, object>{
      {$"document_id{i}", doc["_id"].Raw()},
      { $"index_name{i}", page.IndexName},
      { $"event_category{i}", _source["event_category"].Raw()},
      { $"event_timestamp{i}", _source["event_timestamp"].Raw()},
      { $"event_type{i}", _source["event_type"].Raw()},
      { $"host{i}", _source["host"].Raw()},
      { $"syslog_hostname{i}", _source["syslog_hostname"].Raw()},
      { $"source_zone{i}", _source["source_zone"].Raw()},
      { $"application{i}", _source["application"].Raw()},
      { $"reason{i}", _source["reason"].Raw()},
      { $"category{i}", _source["category"].Raw()},
      { $"url{i}", _source["url"].Raw()},
      // WARN: the ES field attack-name is spelled with a dash -
      { $"attack_name{i}", _source["attack-name"].Raw()},
      { $"threat_severity{i}", _source["threat_severity"].Raw()},
      { $"inserted_at{i}", DateTime.UtcNow},
     };

     // Make sure to only insert rows that don't exist. Entries in Elastic are assumed
     // immutable. Reference statement from https://michaeljswart.com/2017/07/sql-server-upsert-patterns-and-antipatterns/
     var query = @$"
      INSERT documents (document_id, index_name, event_category, event_timestamp, event_type,
                        host, syslog_hostname, source_zone, application, reason, category, url,
                        attack_name, threat_severity, inserted_at)
      SELECT @document_id{i}, @index_name{i}, @event_category{i}, @event_timestamp{i}, @event_type{i}, 
              @host{i}, @syslog_hostname{i}, @source_zone{i}, @application{i}, @reason{i}, @category{i}, @url{i},
              @attack_name{i}, @threat_severity{i}, @inserted_at{i}
      WHERE NOT EXISTS (
        SELECT *
          FROM documents WITH (UPDLOCK, SERIALIZABLE)
          WHERE document_id=@document_id{i} AND index_name=@index_name{i}
      )
     ";

     return (query, cmdparams);
   })
   .Aggregate((new StringBuilder(), new ExpandoObject() as IDictionary<string, object>), (acc, cmdopts) =>
     (acc.Item1.Append(cmdopts.Item1), acc.Item2.Merge(cmdopts.Item2))
   );

    return (builder.ToString(), cmdparams);
  }

  public static IConfigurationRoot CreateConfig()
  {
    return new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", true)
        .AddEnvironmentVariables()
        .Build();
  }

  public static void EnsureLocal(IConfiguration config)
  {
    if (!config["ES_BASE_URL"].Contains("localhost"))
    {
      throw new Exception("ElastiSearch url does not indicate test environment. Aborting!");
    }
    if (!config["DATABASE_CONNECTION_STRING"].Contains("localhost"))
    {
      throw new Exception("MSSQL Server connection string does not indicate test environment. Aborting!");
    }
  }
}
