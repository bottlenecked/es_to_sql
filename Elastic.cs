using System.Text;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
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
    // according to the docs https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-indices.html
    // _cat/indices is intended for human consumption so we need to use the index api
    var json = await client.GetStringAsync("/*?expand_wildcards=open");
    return JObject.Parse(json)
    .Cast<KeyValuePair<string, JToken>>()
    .Select(x => x.Key)
    .ToList();
  }

  public static async IAsyncEnumerable<Page> EnumerateAllDocumentsInIndex(HttpClient client, string indexName)
  {
    int from = 0, size = 1000;
    while (true)
    {
      var query = JsonConvert.SerializeObject(new
      {
        from = from,
        size = size,
        query = new
        {
          match_all = new { }
        }
      });
      var request = new StringContent(query.ToString(), Encoding.UTF8, "application/json");
      var response = await client.PostAsync($"/{indexName}/_search", request);
      var responseBody = await response.Content.ReadAsStringAsync();
      var json = JObject.Parse(responseBody);
      var docs = ((JArray)json["hits"]["hits"]).Cast<JObject>().ToList();
      var page = new Page
      {
        From = from,
        Total = (int)json["hits"]["total"],
        Documents = docs
      };
      yield return page;
      from += size;
      if (docs.Count < size)
      {
        yield break;
      }
    }
  }

  public class Page
  {
    public int From { get; set; }
    public int Total { get; set; }
    public IList<JObject> Documents { get; set; }
  }
}
