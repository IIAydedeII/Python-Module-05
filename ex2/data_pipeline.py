#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, Protocol


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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            output = []
            for _ in range(nb):
                try:
                    output.append(proc.output())
                except IndexError:
                    pass
            plugin.process_output(output)


class CSV:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        length = len(data)
        for i, (c, d) in enumerate(data):
            print(d, end="," if i < length - 1 else "\n")


class JSON:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        print("{", end="")
        length = len(data)
        for i, (c, d) in enumerate(data):
            print(f'"item_{c}": "{d}"', end=", " if i < length - 1 else "")
        print("}")


def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")
    print()

    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()
    print()

    print("Registering Processors")
    for proc in [NumericProcessor(), TextProcessor(), LogProcessor()]:
        stream.register_processor(proc)
    print()

    first_batch = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]
    print("Send first batch of data on stream:", first_batch)
    stream.process_stream(first_batch)
    stream.print_processors_stats()
    print()

    print("Send 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, CSV())
    print()

    stream.print_processors_stats()
    print()

    second_batch = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    print("Send another batch of data:", second_batch)
    stream.process_stream(second_batch)
    print()

    stream.print_processors_stats()
    print()

    print("Send 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, JSON())
    print()

    stream.print_processors_stats()


if __name__ == "__main__":
    main()
