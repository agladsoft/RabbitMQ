from abc import ABC
from __init__ import TABLE_NAMES
from receive import DataCoreClient


class AllDeletedTables(DataCoreClient, ABC):
    def __init__(self):
        super().__init__()

    @property
    def deal(self):
        return None

    def delete_table_deals(self) -> None:
        for table_name in list(TABLE_NAMES.values()):
            AllDeletedTables.table = table_name
            self.delete_old_deals()


AllDeletedTables().delete_table_deals()
