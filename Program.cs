using Microsoft.Extensions.Configuration;

internal class Program
{
  static IConfigurationRoot config = CreateConfig();

  static object[] matches = new object[]{
    new {event_category="antivirus", source_zone="business-Wired"},
    new {event_category = "apptrack", application="BITTORRENT", event_type="APPTRACK_SESSION_CLOSE", reason="Closed by junos-dynapp"},
    new {event_category="webfilter", category="Enhanced_Personals_and_Dating", event_type="WEBIFLTER_URL_BLOCKED", source_zone="crew-Wired", url="edge.microsoft.com"},
    new {event_category="webfilter", category="TELEGRAM", event_type="WEBIFLTER_URL_PERMITTED", source_zone="crew-Wired", url="tools.dvdvideosoft.com/stat.jso"},
    new {event_category="firewall", reason="Closed by junos-dynapp", event_type="RT_FLOW_SESSION_CLOSE", source_zone="crew-Wired"},
    new {event_category="ips", event_type="IDP_ATTACK_LOG_EVENT", source_zone="crew-Wired", threat_severity="HIGH"},
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
          // use a fixed timestamp to make sure we'll scan among the existing local indexes
          now = DateTime.Parse("2023-01-02T09:00:00Z").ToUniversalTime();
          break;
      }
    }

    Database.CreateLogTableIfNotExists(connection);

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
    var indexesToScan = indexes.TakeWhile(idx => string.Compare(idx, lastIndex) < 1);
    Console.WriteLine($"Indexes to scan = {string.Join(",", indexesToScan)}, count={indexesToScan.Count()}");

    // Now scan the database for the indexes completed. For every match we're going to skip that index
    // because it would have been scanned in a previous run

    var previouslyCompletedIndexes = Database.ExecuteSql(connection, "SELECT index_name FROM log_entries WHERE index_name IN (@indexes)", new { indexes = indexesToScan })
    .SelectMany(x => x.Values)
    .Cast<string>()
    .ToList();
    Console.WriteLine("Indexes completed=" + string.Join(", ", previouslyCompletedIndexes));

    // Calculate the last index to scan
    // var indexesToScrape = Database.DetermineIndexesToFetchDataFrom(indexes, connection);
    // Console.WriteLine(string.Join(", ", indexesToScrape));

    //var q = Elastic.GenerateJunosQuery(matches);
    //Console.WriteLine(JsonConvert.SerializeObject(q));

    // await Parallel.ForEachAsync(indexesToScrape, async (index, _ct) =>
    // {
    //   Console.WriteLine($"Scraping index {index}....");
    //   await foreach (var page in Elastic.EnumerateAllDocumentsInIndex(elasticClient, index))
    //   {
    //     Console.WriteLine($"got results for index {index}: {page.From} to {page.From + page.Documents.Count}, total = {page.Total}");
    //   }
    // });

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
