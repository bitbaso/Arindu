using Arindu.Features.Historify.Entities;

namespace Arindu.Features.Historify.Interfaces;

public interface IHistorifyManager
{
    Task<bool> ExecuteHistorify(List<TableConfiguration> tablesToArchive);
}
