#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):

    def __init__(self) -> None:
        self.storage: list[tuple[int, str]] = []
        self.rank_counter = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return self.storage.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        data = data if isinstance(data, list) else [data]

        for d in data:
            if not isinstance(d, (int, float)):
                return False
        return True

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        data = data if isinstance(data, list) else [data]
        for d in data:
            self.storage.append((self.rank_counter, str(d)))
            self.rank_counter += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        data = data if isinstance(data, list) else [data]

        for d in data:
            if not isinstance(d, str):
                return False
        return True

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        data = data if isinstance(data, list) else [data]
        for d in data:
            self.storage.append((self.rank_counter, str(d)))
            self.rank_counter += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        data = data if isinstance(data, list) else [data]

        for d in data:
            if not isinstance(d, dict):
                return False
        return True

    def ingest(self, data: dict[Any, Any] | list[dict[Any, Any]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        data = data if isinstance(data, list) else [data]
        for d in data:
            formatted = ": ".join(str(v) for v in d.values())
            self.storage.append((self.rank_counter, formatted))
            self.rank_counter += 1


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            processed = False
            for proc in self._processors:
                try:
                    proc.ingest(item)
                except ValueError:
                    pass
                else:
                    processed = True

            if not processed:
                print(
                    "DataStream error - Can't process element in stream:", item
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")

        if not self._processors:
            print("No processor found, no data")

        for proc in self._processors:
            print(
                f"{proc.__class__.__name__}:",
                f"total {proc.rank_counter} items processed,",
                f"remaining {len(proc.storage)} on processor",
            )


def main() -> None:
    print("=== Code Nexus - Data Stream ===")
    print()

    proc_numeric = NumericProcessor()
    proc_text = TextProcessor()
    proc_log = LogProcessor()

    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()
    print()

    print("Registering Numeric Processor")
    stream.register_processor(proc_numeric)
    print()

    first_batch = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {"log_level": "INFO", "log_message": "User wil isconnected"},
        ],
        42,
        ["Hi", "five"],
    ]
    print("Send first batch of data on stream:", first_batch)
    stream.process_stream(first_batch)
    stream.print_processors_stats()
    print()

    print("Registering other data processors")
    stream.register_processor(proc_text)
    stream.register_processor(proc_log)
    print("Send the same batch again")
    stream.process_stream(first_batch)
    stream.print_processors_stats()
    print()

    print(
        "Consume some elements from the data processors:",
        "Numeric 3,",
        "Text 2,",
        "Log 1",
    )
    for _ in range(3):
        proc_numeric.output()
    for _ in range(2):
        proc_text.output()
    for _ in range(1):
        proc_log.output()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
