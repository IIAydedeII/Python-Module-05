#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):

    def __init__(self) -> None:
        self._storage: list[tuple[int, str]] = []
        self._rank_counter = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return self._storage.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        data = data if type(data) is list else [data]

        for d in data:
            if type(d) is not int and type(d) is not float:
                return False
        return True

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        data = data if type(data) is list else [data]
        for d in data:
            self._storage.append((self._rank_counter, str(d)))
            self._rank_counter += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        data = data if type(data) is list else [data]

        for d in data:
            if type(d) is not str:
                return False
        return True

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        data = data if type(data) is list else [data]
        for d in data:
            self._storage.append((self._rank_counter, str(d)))
            self._rank_counter += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        data = data if type(data) is list else [data]

        for d in data:
            if type(d) is not dict:
                return False
        return True

    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        data = data if type(data) is list else [data]
        for d in data:
            formatted = ": ".join(str(v) for v in d.values())
            self._storage.append((self._rank_counter, formatted))
            self._rank_counter += 1


def main() -> None:
    print("=== Code Nexus - Data Processor ===")
    print()

    processor: DataProcessor
    data: Any

    print("Testing Numeric Processor...")
    processor = NumericProcessor()
    print(" Trying to validate input '42':", processor.validate(42))
    print(" Trying to validate input 'Hello':", processor.validate("Hello"))
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        processor.ingest("foo")
    except ValueError as e:
        print(" Got exception:", e)
    data = [1, 2, 3, 4, 5]
    print(" Processing data:", data)
    processor.ingest(data)
    print(" Extracting 3 values...")
    for _ in range(3):
        output = processor.output()
        print(f" Numeric value {output[0]}:", output[1])
    print()

    print("Testing Text Processor...")
    processor = TextProcessor()
    print(" Trying to validate input '42':", processor.validate(42))
    print(" Test invalid ingestion of number '123' without prior validation:")
    try:
        processor.ingest(123)
    except ValueError as e:
        print(" Got exception:", e)
    data = ["Hello", "Nexus", "World"]
    print(" Processing data:", data)
    processor.ingest(data)
    print(" Extracting 1 values...")
    for _ in range(1):
        output = processor.output()
        print(f" Text value {output[0]}:", output[1])
    print()

    print("Testing Log Processor...")
    processor = LogProcessor()
    print(" Trying to validate input 'Hello':", processor.validate("Hello"))
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        processor.ingest("foo")
    except ValueError as e:
        print(" Got exception:", e)
    data = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]
    print(" Processing data:", data)
    processor.ingest(data)
    print(" Extracting 2 values...")
    for _ in range(2):
        output = processor.output()
        print(f" Log entry {output[0]}:", output[1])
    print()


if __name__ == "__main__":
    main()
