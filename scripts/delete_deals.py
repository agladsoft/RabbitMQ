from abc import ABC
from scripts.receive import DataCoreClient


class AllDeletedTables(DataCoreClient, ABC):
    def __init__(self):
        super().__init__()

    def delete_table_logs(self) -> None:
        AllDeletedTables.table = "rmq_log"
        self.delete_old_deals(cond="toDate(datetime) <= date_sub(DAY, 7, today())")


if __name__ == "__main__":
    AllDeletedTables().delete_table_logs()
