using Microsoft.Extensions.Configuration;
using System.Dynamic;
using System.Text;
using Newtonsoft.Json.Linq;

internal class Program
{
  static IConfigurationRoot config = CreateConfig();

  static object[] matches = new object[]{
    new {event_category = "antivirus", source_zone = "business-Wired"},
    new {event_category = "apptrack", application = "BITTORRENT", event_type = "APPTRACK_SESSION_CLOSE", reason = "Closed by junos-dynapp"},
    new {event_category = "webfilter", category = "Enhanced_Personals_and_Dating", event_type = "WEBIFLTER_URL_BLOCKED", source_zone = "crew-Wired", url = "edge.microsoft.com"},
    new {event_category = "webfilter", category = "TELEGRAM", event_type = "WEBIFLTER_URL_PERMITTED", source_zone = "crew-Wired", url = "tools.dvdvideosoft.com/stat.jso"},
    new {event_category = "firewall", reason = "Closed by junos-dynapp", event_type = "RT_FLOW_SESSION_CLOSE", source_zone = "crew-Wired"},
    new {event_category = "ips", event_type = "IDP_ATTACK_LOG_EVENT", source_zone = "crew-Wired", threat_severity = "HIGH"},
  };

  static DateTime now = DateTime.UtcNow;

  private static async Task Main(string[] args)
  {
    var elasticClient = Elastic.CreateElasticSearchClient(config);
    var connection = Database.CreateSqlConnection(config);

    if (args.Length > 0)
    {
      switch (args[0])
      {
        case "--populate":
          var dummyIndexes = await DocumentGenerator.CreateIndexes(elasticClient);
          await DocumentGenerator.PopulateIndexes(elasticClient, dummyIndexes);
          return;
        case "--testrun":
          await Database.CreateLogTableIfNotExists(connection);
          await Database.CreateDocumentsTableIfNotExists(connection);
          // use a fixed timestamp to make sure we'll scan among the existing local indexes
          now = DateTime.Parse("2023-01-02T09:00:00Z").ToUniversalTime();
          break;
      }
    }


    // Get all the indexes available in ElasticSearch
    var indexes = await Elastic.ListIndexesOrdered(elasticClient);

    // Calculate the last index to scan. Indexes are named like junoslogs-2023.01.01-13
    // ie they have a date+hour timestamp in their name. The last index will be the time_now_utc - 2h
    // index just to make sure there are no issues with clock skew between the servers
    var lastIndexTime = now.AddHours(-2);
    var lastIndex = $"junoslogs-{lastIndexTime.Year}.{lastIndexTime.Month.ToString("D2")}.{lastIndexTime.Day.ToString("D2")}-{lastIndexTime.Hour.ToString("D2")}";
    Console.WriteLine($"last index= {lastIndex} Now ={now}, LastIndexTime={lastIndexTime}");

    // Filter out all the indexes that are greater than the last index calculated above
    // because writing may not have finished there
    var candidateIndexes = indexes.TakeWhile(idx => string.Compare(idx, lastIndex) < 1).ToHashSet();
    Console.WriteLine($"Indexes to scan = {string.Join(",", candidateIndexes)}, count={candidateIndexes.Count()}");

    // Now scan the database for the indexes completed. For every match we're going to skip that index
    // because it would have been scanned in a previous run

    var previouslyCompletedIndexes = (await Database.ExecuteSql(connection, "SELECT index_name FROM log_entries WHERE index_name IN (@indexes)", new { indexes = candidateIndexes }))
      .SelectMany(x => x.Values)
      .Cast<string>()
      .ToList();
    Console.WriteLine("Indexes completed=" + string.Join(", ", previouslyCompletedIndexes));

    // Skip the indexes that have already been completed
    var indexesToScan = candidateIndexes.Except(previouslyCompletedIndexes).ToList();

    // Time to fetch the documents from Elastic
    var q = Elastic.GenerateJunosQuery(matches);
    await Parallel.ForEachAsync(indexesToScan, async (index, _ct) =>
    {
      Console.WriteLine($"Scraping index {index}....");
      await foreach (var page in Elastic.EnumerateAllDocumentsInIndex(elasticClient, index, q))
      {
        (var cmd, var cmdparams) = BuildCommand(page);
        await Database.ExecuteSql(connection, cmd, cmdparams);
      }
      // Let's log that we're done with that particular index
      await Database.ExecuteSql(connection, "INSERT INTO log_entries (index_name, inserted_at) VALUES (@index_name, @inserted_at)", new { index_name = index, inserted_at = DateTime.UtcNow });
    });

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
          WHERE document_id=@document_id{i} AND index_name=@index_name{1}
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
}
