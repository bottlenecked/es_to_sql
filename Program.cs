using System.Reflection;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;

internal class Program
{
  static IConfigurationRoot config = CreateConfig();


  private static async Task Main(string[] args)
  {
    var elasticClient = Elastic.CreateElasticSearchClient(config);
    var indexes = await Elastic.ListIndexes(elasticClient);
    Console.WriteLine(string.Join(", ", indexes));
    var connection = Database.CreateSqlConnection(config);
    Database.CreateLogTableIfNotExists(connection);
    var indexesToScrape = Database.DetermineIndexesToFetchDataFrom(indexes, connection);
    Console.WriteLine(string.Join(", ", indexesToScrape));

    await Parallel.ForEachAsync(indexesToScrape, async (index, _ct) =>
    {
      Console.WriteLine($"Scraping index {index}....");
      await foreach (var page in Elastic.EnumerateAllDocumentsInIndex(elasticClient, index))
      {
        Console.WriteLine($"got results for index {index}: {page.From} to {page.From + page.Documents.Count}, total = {page.Total}");
      }
    });

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
