from abc import ABC
from receive import DataCoreClient, CLASSES
from __init__ import TABLE_NAMES, LOG_TABLE


class AllDeletedTables(DataCoreClient, ABC):
    def __init__(self):
        super().__init__()

    @property
    def deal(self):
        return None

    @property
    def database(self):
        return None

    @database.setter
    def database(self, value):
        self.database = value

    def delete_table_deals(self) -> None:
        for table_name, class_name in zip(list(TABLE_NAMES.values()), CLASSES):
            obj_table = class_name()
            AllDeletedTables.database = obj_table.database
            AllDeletedTables.table = table_name
            if LOG_TABLE == table_name:
                self.delete_old_deals(cond="toDate(datetime) <= date_sub(DAY, 7, today())")
                continue
            self.delete_old_deals()


AllDeletedTables().delete_table_deals()
