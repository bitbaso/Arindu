namespace Arindu.Features.TestData.Interfaces;

public interface ITestDataManager
{
    Task InsertRandomRowsAsync(int numberOfRows, string connectionString);
}
