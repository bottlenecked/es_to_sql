using Newtonsoft.Json.Linq;

public static class Extensions
{

  public static IDictionary<T1, T2> Merge<T1, T2>(this IDictionary<T1, T2> dict1, IDictionary<T1, T2> dict2)
  {
    foreach (var kvp in dict2)
    {
      dict1[kvp.Key] = kvp.Value;
    }
    return dict1;
  }

  public static object? Raw(this JToken? token)
  {
    if (token == null)
    {
      return null;
    }
    return ((JValue)token).Value;
  }
}
