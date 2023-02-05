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

  public static async Task<IEnumerable<string>> ListIndexesOrdered(HttpClient client)
  {
    // according to the docs https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-indices.html
    // _cat/indices is intended for human consumption so we need to use the index api
    var json = await client.GetStringAsync("/junoslogs*?expand_wildcards=open");
    return JObject.Parse(json)
    .Cast<KeyValuePair<string, JToken>>()
    .Select(x => x.Key)
    .Order()
    .ToList();
  }
  public static object GenerateJunosQuery(params object[] matches)
  {
    var orQueries = matches
    .Select(JObject.FromObject)
    .Select(AndFields)
    .ToList();

    return new { @bool = new { should = orQueries } };
  }

  private static object AndFields(JObject fields)
  {
    var matches = fields
    .Cast<KeyValuePair<string, JToken>>()
    .Select(field => new { match = new Dictionary<string, object> { { field.Key, field.Value } } })
    .ToList();
    return new { @bool = new { must = matches } };
  }

  public static async IAsyncEnumerable<Page> EnumerateAllDocumentsInIndex(HttpClient client, string indexName, object query)
  {
    int from = 0, size = 1000;
    while (true)
    {
      var paged_query = new { query = query, from = from, size = size };
      var queryStr = JsonConvert.SerializeObject(paged_query, Formatting.None);
      var request = new StringContent(queryStr, Encoding.UTF8, "application/json");
      var response = await client.PostAsync($"/{indexName}/_search", request);
      var responseBody = await response.Content.ReadAsStringAsync();
      var json = JObject.Parse(responseBody);
      var docs = ((JArray)json["hits"]["hits"]).Cast<JObject>().ToList();
      var page = new Page
      {
        IndexName = indexName,
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
    public string IndexName { get; set; }
    public int From { get; set; }
    public int Total { get; set; }

    public string Name { get { return $"{IndexName}:({From}-{Documents.Count - 1})"; } }
    public IList<JObject> Documents { get; set; }
  }
}
