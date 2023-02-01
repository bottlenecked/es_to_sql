using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;

public class Elastic
{
  public static HttpClient CreateElasticSearchClient(IConfigurationRoot config)
  {
    var authStr = Convert.ToBase64String(System.Text.ASCIIEncoding.UTF8.GetBytes($"{config["ES_USER_NAME"]}:{config["ES_PASSWORD"]}"));
    return new HttpClient
    {
      BaseAddress = new Uri(config["ES_BASE_URL"]),
      DefaultRequestHeaders = {
        { "Authorization", "Basic " + authStr },
        {"Accept", "application/json"}
      }
    };
  }

  public static async Task<IEnumerable<string>> ListIndexes(HttpClient client)
  {
    var json = await client.GetStringAsync("/_cat/indices");
    return JArray.Parse(json)
            .Select(x => (string)x["index"])
            .ToList();
  }
}
