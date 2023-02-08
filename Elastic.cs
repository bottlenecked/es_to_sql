using System.Net;
using System.Text;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

public class Elastic
{

  public static HttpClient CreateElasticSearchClient(IConfigurationRoot config)
  {
    System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
    var handler = new HttpClientHandler()
    {
      ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
    };
    var authStr = Convert.ToBase64String(System.Text.ASCIIEncoding.UTF8.GetBytes($"{config["ES_USER_NAME"]}:{config["ES_PASSWORD"]}"));
    return new HttpClient(handler)
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

  public static async IAsyncEnumerable<Page> EnumerateAllDocumentsInIndex(HttpClient client, string indexName, int batchSize, object query)
  {
    int from = 0;
    string? scrollId = null;
    while (true)
    {
      var (url, jsonParams) = PrepareRequest(query, indexName, batchSize, 10, scrollId);
      var request = new StringContent(jsonParams, Encoding.UTF8, "application/json");
      var response = await client.PostAsync(url, request);
      var responseBody = await response.Content.ReadAsStringAsync();
      var json = JObject.Parse(responseBody);
      if (json["error"] != null)
      {
        throw new Exception(@$"
          Received error from elastic while browsing index={indexName}, from={from}, to={from + batchSize - 1}
          Status code: {response.StatusCode}
          Headers: {String.Join("; ", response.Headers.Select(h => $"{h.Key}={h.Value}"))}
          Body: {await response.Content.ReadAsStringAsync()}
          ");
      }
      scrollId = (string?)json["_scroll_id"];
      var docs = ((JArray)json["hits"]["hits"]).Cast<JObject>().ToList();
      var page = new Page
      {
        IndexName = indexName,
        From = from,
        Total = (int)json["hits"]["total"],
        Documents = docs
      };
      yield return page;
      from += batchSize;
      if (docs.Count < batchSize)
      {
        yield break;
      }
    }
  }

  private static (string url, string body) PrepareRequest(object query, string indexName, int size, int scrollContextTimeoutSeconds, string? scrollId)
  {
    if (scrollId == null)
    {
      var queryParams = new { query = query, size = size };
      var queryStr = JsonConvert.SerializeObject(queryParams, Formatting.None);
      return ($"/{indexName}/_search?scroll={scrollContextTimeoutSeconds}s", queryStr);
    }
    var scrollParams = new { scroll = $"{scrollContextTimeoutSeconds}s", scroll_id = scrollId };
    var scrollStr = JsonConvert.SerializeObject(scrollParams, Formatting.None);
    return ("/_search/scroll", scrollStr);
  }

  public class Page
  {
    public string IndexName { get; set; }
    public int From { get; set; }
    public int Total { get; set; }

    public string Name { get { return $"{IndexName}:({From}-{Math.Max(From, From + Documents.Count - 1)})"; } }
    public IList<JObject> Documents { get; set; }
  }
}
