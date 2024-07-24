from abc import ABC
from receive import DataCoreClient
from __init__ import TABLE_NAMES, LOG_TABLE


class AllDeletedTables(DataCoreClient, ABC):
    def __init__(self):
        super().__init__()

    @property
    def deal(self):
        return None

    def delete_table_deals(self) -> None:
        for table_name in [LOG_TABLE] + list(TABLE_NAMES.values()):
            AllDeletedTables.table = table_name
            if LOG_TABLE == table_name:
                self.delete_old_deals(cond="toDate(datetime) <= date_sub(DAY, 7, today())")
                continue
            self.delete_old_deals()


AllDeletedTables().delete_table_deals()
