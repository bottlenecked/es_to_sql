[assembly: log4net.Config.XmlConfigurator(Watch = true, ConfigFile = "log4net.config.xml")]


class Log4NetSetup
{
  private static DateTime start = DateTime.UtcNow;
  public static void Setup()
  {
    log4net.GlobalContext.Properties["ellapsed"] = new Log4NetSetup();
  }

  public override string ToString() => $"+{(DateTime.UtcNow - start).TotalSeconds.ToString("F1")}";
}
