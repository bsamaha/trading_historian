using KafkaToInfluxDB.Models;

public interface ICandleDataParser
{
    CandleData Parse(string message);
}

public class CandleDataParser : ICandleDataParser
{
    public CandleData Parse(string message)
    {
        return CandleData.ParseCandleData(message);
    }
}