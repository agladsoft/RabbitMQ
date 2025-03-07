from scripts.receive import DataCoreClient, Receive


class AllDeletedTables(DataCoreClient):
    def __init__(self, receive: "Receive"):
        super().__init__(receive=receive)

    def delete_table_logs(self) -> None:
        AllDeletedTables.table = "rmq_log"
        self.delete_old_deals(cond="toDate(datetime) <= date_sub(DAY, 7, today())")


if __name__ == "__main__":
    AllDeletedTables(Receive()).delete_table_logs()
