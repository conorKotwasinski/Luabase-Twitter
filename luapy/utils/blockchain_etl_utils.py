# Utilities that are modified functions from blockhain-etl repos
import logging

from blockchainetl_common.jobs.exporters.in_memory_item_exporter import (
    InMemoryItemExporter,
)
from polygonetl.enumeration.entity_type import EntityType
from polygonetl.jobs.export_blocks_job import ExportBlocksJob
from polygonetl.jobs.export_geth_traces_job import ExportGethTracesJob
from polygonetl.jobs.export_receipts_job import ExportReceiptsJob
from polygonetl.jobs.extract_contracts_job import ExtractContractsJob
from polygonetl.jobs.extract_geth_traces_job import ExtractGethTracesJob
from polygonetl.jobs.extract_token_transfers_job import ExtractTokenTransfersJob
from polygonetl.jobs.extract_tokens_job import ExtractTokensJob
from polygonetl.thread_local_proxy import ThreadLocalProxy
from polygonetl.streaming.enrich import (
    enrich_transactions,
    enrich_logs,
    enrich_token_transfers,
    enrich_traces,
    enrich_contracts,
    enrich_tokens,
)
from polygonetl.streaming.eth_item_id_calculator import EthItemIdCalculator
from polygonetl.streaming.eth_item_timestamp_calculator import (
    EthItemTimestampCalculator,
)
from web3 import Web3
from web3.middleware import geth_poa_middleware

# class for exporting output from polygon-etl and ethereum-etl in memory
# most repo does not have an in memory exporter
class InMemoryObjectExporter:
    def __init__(
        self, blockchain_streamer_adapter=None, lag=0, start_block=None, end_block=None,
    ):
        self.blockchain_streamer_adapter = blockchain_streamer_adapter
        self.lag = lag
        self.start_block = start_block
        self.end_block = end_block

    def export(self):
        try:
            self.blockchain_streamer_adapter.open()
            result = self._do_export()
        finally:
            self.blockchain_streamer_adapter.close()
            return result

    def _do_export(self):
        result = self.blockchain_streamer_adapter.export_all(
            self.start_block, self.end_block
        )
        return result


# adaptor class for extracting data from eth/polygon data into memory
class EthInMemoryAdapter:
    def __init__(
        self,
        batch_web3_provider,
        item_exporter=InMemoryItemExporter(
            item_types=[
                "block",
                "transaction",
                "log",
                "token_transfer",
                "trace",
                "contract",
                "token",
            ]
        ),
        batch_size=100,
        max_workers=5,
        entity_types=",".join(
            [
                "block",
                "transaction",
                "log",
                "token_transfer",
                "trace",
                "contract",
                "token",
            ]
        ),
        job_id=-1,
    ):
        self.batch_web3_provider = batch_web3_provider
        self.item_exporter = item_exporter
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.entity_types = entity_types
        self.item_id_calculator = EthItemIdCalculator()
        self.item_timestamp_calculator = EthItemTimestampCalculator()
        self.web3 = Web3(self.batch_web3_provider)
        self.web3.middleware_stack.inject(geth_poa_middleware, layer=0)
        self.job_id = job_id

    def open(self):
        self.item_exporter.open()

    def get_current_block_number(self):
        return int(self.web3.eth.getBlock("latest").number)

    def export_all(self, start_block, end_block):
        logging.getLogger("ethereum_dasm.evmdasm").setLevel(logging.CRITICAL)
        # Export blocks and transactions
        blocks, transactions = [], []
        if self._should_export(EntityType.BLOCK) or self._should_export(
            EntityType.TRANSACTION
        ):
            blocks, transactions = self._export_blocks_and_transactions(
                start_block, end_block
            )

        # Export receipts and logs
        receipts, logs = [], []
        if self._should_export(EntityType.RECEIPT) or self._should_export(
            EntityType.LOG
        ):
            receipts, logs = self._export_receipts_and_logs(transactions)

        # Extract token transfers
        token_transfers = []
        if self._should_export(EntityType.TOKEN_TRANSFER):
            token_transfers = self._extract_token_transfers(logs)

        # Export traces
        traces = []
        if self._should_export(EntityType.TRACE):
            raw_traces = self._export_traces(start_block, end_block)
            traces = self._extract_traces(raw_traces)

        # Export contracts
        contracts = []
        if self._should_export(EntityType.CONTRACT):
            contracts = self._export_contracts(traces)

        # Export tokens
        tokens = []
        if self._should_export(EntityType.TOKEN):
            tokens = self._extract_tokens(contracts)

        enriched_blocks = blocks if EntityType.BLOCK in self.entity_types else []
        enriched_transactions = (
            enrich_transactions(transactions, receipts)
            if EntityType.TRANSACTION in self.entity_types
            else []
        )
        enriched_logs = (
            enrich_logs(blocks, logs) if EntityType.LOG in self.entity_types else []
        )
        enriched_token_transfers = (
            enrich_token_transfers(blocks, token_transfers)
            if EntityType.TOKEN_TRANSFER in self.entity_types
            else []
        )
        enriched_traces = (
            enrich_traces(blocks, traces, transactions)
            if EntityType.TRACE in self.entity_types
            else []
        )
        enriched_contracts = (
            enrich_contracts(blocks, contracts)
            if EntityType.CONTRACT in self.entity_types
            else []
        )
        enriched_tokens = (
            enrich_tokens(blocks, tokens)
            if EntityType.TOKEN in self.entity_types
            else []
        )

        logging.info("Exporting with " + type(self.item_exporter).__name__)

        all_items = (
            enriched_blocks
            + enriched_transactions
            + enriched_logs
            + enriched_token_transfers
            + enriched_traces
            + enriched_contracts
            + enriched_tokens
        )

        # self.calculate_item_ids(all_items)
        # self.calculate_item_timestamps(all_items)

        all_items = {
            "blocks": enriched_blocks,
            "transactions": enriched_transactions,
            "logs": enriched_logs,
            "token_transfers": enriched_token_transfers,
            "traces": enriched_traces,
            "contracts": enriched_contracts,
            "tokens": enriched_tokens,
        }
        return all_items

    def _export_blocks_and_transactions(self, start_block, end_block):
        blocks_and_transactions_item_exporter = InMemoryItemExporter(
            item_types=["block", "transaction"]
        )
        blocks_and_transactions_job = ExportBlocksJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=blocks_and_transactions_item_exporter,
            export_blocks=self._should_export(EntityType.BLOCK),
            export_transactions=self._should_export(EntityType.TRANSACTION),
            job_id=self.job_id,
        )
        blocks_and_transactions_job.run()
        blocks = blocks_and_transactions_item_exporter.get_items("block")
        transactions = blocks_and_transactions_item_exporter.get_items("transaction")
        return blocks, transactions

    def _export_receipts_and_logs(self, transactions):
        exporter = InMemoryItemExporter(item_types=["receipt", "log"])
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(
                transaction["hash"] for transaction in transactions
            ),
            batch_size=self.batch_size,
            batch_web3_provider=self.batch_web3_provider,
            max_workers=self.max_workers,
            item_exporter=exporter,
            export_receipts=self._should_export(EntityType.RECEIPT),
            export_logs=self._should_export(EntityType.LOG),
            job_id=self.job_id,
        )
        job.run()
        receipts = exporter.get_items("receipt")
        logs = exporter.get_items("log")
        return receipts, logs

    def _extract_token_transfers(self, logs):
        exporter = InMemoryItemExporter(item_types=["token_transfer"])
        job = ExtractTokenTransfersJob(
            logs_iterable=logs,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
            job_id=self.job_id,
        )
        job.run()
        token_transfers = exporter.get_items("token_transfer")
        return token_transfers

    def _export_traces(self, start_block, end_block):
        exporter = InMemoryItemExporter(item_types=["geth_trace"])
        job = ExportGethTracesJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            batch_web3_provider=ThreadLocalProxy(lambda: self.batch_web3_provider),
            max_workers=self.max_workers,
            item_exporter=exporter,
            job_id=self.job_id,
        )
        job.run()
        traces = exporter.get_items("geth_trace")
        return traces

    def _extract_traces(self, raw_traces):
        exporter = InMemoryItemExporter(item_types=["trace"])
        job = ExtractGethTracesJob(
            traces_iterable=raw_traces,
            # batch_size=self.batch_size, doesn't work for some reason, works in offical repo version
            max_workers=self.max_workers,
            item_exporter=exporter,
            job_id=self.job_id,
        )
        job.run()
        traces = exporter.get_items("trace")
        return traces

    def _export_contracts(self, traces):
        exporter = InMemoryItemExporter(item_types=["contract"])
        job = ExtractContractsJob(
            traces_iterable=traces,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            item_exporter=exporter,
            job_id=self.job_id,
        )
        job.run()
        contracts = exporter.get_items("contract")
        return contracts

    def _extract_tokens(self, contracts):
        exporter = InMemoryItemExporter(item_types=["token"])
        job = ExtractTokensJob(
            contracts_iterable=contracts,
            web3=ThreadLocalProxy(lambda: Web3(self.batch_web3_provider)),
            max_workers=self.max_workers,
            item_exporter=exporter,
            job_id=self.job_id,
        )
        job.run()
        tokens = exporter.get_items("token")
        return tokens

    def _should_export(self, entity_type):
        if entity_type == EntityType.BLOCK:
            return True

        if entity_type == EntityType.TRANSACTION:
            return (
                EntityType.TRANSACTION in self.entity_types
                or self._should_export(EntityType.LOG)
                or self._should_export(EntityType.TRACE)
            )

        if entity_type == EntityType.RECEIPT:
            return EntityType.TRANSACTION in self.entity_types or self._should_export(
                EntityType.TOKEN_TRANSFER
            )

        if entity_type == EntityType.LOG:
            return EntityType.LOG in self.entity_types or self._should_export(
                EntityType.TOKEN_TRANSFER
            )

        if entity_type == EntityType.TOKEN_TRANSFER:
            return EntityType.TOKEN_TRANSFER in self.entity_types

        if entity_type == EntityType.TRACE:
            return EntityType.TRACE in self.entity_types or self._should_export(
                EntityType.CONTRACT
            )

        if entity_type == EntityType.CONTRACT:
            return EntityType.CONTRACT in self.entity_types or self._should_export(
                EntityType.TOKEN
            )

        if entity_type == EntityType.TOKEN:
            return EntityType.TOKEN in self.entity_types

        raise ValueError("Unexpected entity type " + entity_type)

    def calculate_item_ids(self, items):
        for item in items:
            item["item_id"] = self.item_id_calculator.calculate(item)

    def calculate_item_timestamps(self, items):
        for item in items:
            item["item_timestamp"] = self.item_timestamp_calculator.calculate(item)

    def close(self):
        self.item_exporter.close()
