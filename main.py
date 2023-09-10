#!/bin/env python3

import argparse
import os
import sys
from abc import ABC, abstractmethod
from base64 import b64decode

import requests
import yaml


class Database(ABC):
    """
    Abstract class for database
    """

    @abstractmethod
    def test_connection(self, *args, **kwargs):
        ...

    @abstractmethod
    def query(self, *args, **kwargs):
        ...

    @abstractmethod
    def get(self, *args, **kwargs):
        ...


class Datastore(Database):
    DATASTORE_DATASET = os.getenv("DATASTORE_DATASET", "test")
    DATASTORE_HOST = os.getenv("DATASTORE_HOST", "http://localhost:8081")
    DATASTORE_EMULATOR_HOST = os.getenv("DATASTORE_EMULATOR_HOST", "localhost:8081")
    DATASTORE_EMULATOR_HOST_PATH = os.getenv(
        "DATASTORE_EMULATOR_HOST_PATH", "localhost:8081/datastore"
    )
    DATASTORE_PROJECT_ID = os.getenv("DATASTORE_PROJECT_ID", "test")

    def __init__(
        self,
        DATASTORE_DATASET=None,
        DATASTORE_HOST=None,
        DATASTORE_EMULATOR_HOST=None,
        DATASTORE_EMULATOR_HOST_PATH=None,
        DATASTORE_PROJECT_ID=None,
    ):
        self.DATASTORE_DATASET = DATASTORE_DATASET or self.DATASTORE_DATASET
        self.DATASTORE_HOST = DATASTORE_HOST or self.DATASTORE_HOST
        self.DATASTORE_EMULATOR_HOST = (
            DATASTORE_EMULATOR_HOST or self.DATASTORE_EMULATOR_HOST
        )
        self.DATASTORE_EMULATOR_HOST_PATH = (
            DATASTORE_EMULATOR_HOST_PATH or self.DATASTORE_EMULATOR_HOST_PATH
        )
        self.DATASTORE_PROJECT_ID = DATASTORE_PROJECT_ID or self.DATASTORE_PROJECT_ID

    def get_scheme(self, kind):
        response = requests.post(
            self.DATASTORE_HOST + f"/v1/projects/{self.DATASTORE_PROJECT_ID}:runQuery",
            json={
                "gqlQuery": {
                    "queryString": f"SELECT * FROM {kind} LIMIT 100",
                    "allowLiterals": True,
                }
            },
        )
        if response.status_code == 200:
            return self.generate_scheme(response.json())
        else:
            raise Exception("Error:", response.status_code, response.text)

    def generate_scheme(self, data):
        scheme = {}
        c = 0
        for _entity in data.get("batch", {}).get("entityResults", []):
            c += 1

            entity = _entity.get("entity", {})

            kind = entity["key"]["path"][0]["kind"]
            if scheme.get(kind) is None:
                scheme[kind] = {}

            for property, opt in entity.get("properties", {}).items():
                if scheme[kind].get(property) is None:
                    scheme[kind][property] = set()
                for value in opt:
                    if (
                        value.replace("Value", "") != "null"
                        and value != "excludeFromIndexes"
                    ):
                        scheme[kind][property].add(value.replace("Value", ""))

        for kind, properties in scheme.items():
            for property, values in properties.items():
                if len(values) >= 1:
                    scheme[kind][property] = values.pop()
                else:
                    scheme[kind][property] = None

        return scheme

    def _parse_properties(self, properties):
        for property, opt in properties.items():
            properties[property] = None
            for k, v in opt.items():
                if "Value" in k:
                    if k == "blobValue":
                        properties[property] = b64decode(v).decode("utf-8")
                        break
                    else:
                        properties[property] = v
                        break
        return properties

    def format_response(
        self, response: requests.Response, format="yaml", style="scheme"
    ):
        if response.status_code == 200:
            data = response.json()
            output_data = {
                "scheme": self.generate_scheme(data),
                "entities": [],
            }

            if style == "scheme":
                for _entity in data.get("batch", {}).get("entityResults", []):
                    entity = _entity.get("entity", {})
                    kind = entity["key"]["path"][0]["kind"]
                    entity_to_append = {
                        "key": {
                            "kind": kind,
                            "id": int(entity["key"]["path"][0]["id"]),
                        },
                        "properties": entity.get("properties", {}),
                    }
                    entity_to_append["properties"] = self._parse_properties(
                        entity_to_append["properties"]
                    )
                    output_data["entities"].append(entity_to_append)

            if format == "yaml":
                # print(yaml.dump(data, sort_keys=False))
                print(yaml.dump(output_data))
            elif format == "json":
                print(data)
            else:
                print(response.text)

    def get(self, kind, id):
        response = requests.post(
            self.DATASTORE_HOST + f"/v1/projects/{self.DATASTORE_PROJECT_ID}:runQuery",
            json={
                "gqlQuery": {
                    "queryString": f"SELECT * FROM {kind} WHERE __key__ HAS ANCESTOR KEY({kind}, {id})",
                    "allowLiterals": True,
                }
            },
        )
        self.format_response(response)

    def list(self, kind, limit=100):
        response = requests.post(
            self.DATASTORE_HOST + f"/v1/projects/{self.DATASTORE_PROJECT_ID}:runQuery",
            json={
                "gqlQuery": {
                    "queryString": f"SELECT * FROM {kind} LIMIT {limit}",
                    "allowLiterals": True,
                }
            },
        )
        self.format_response(response)

    def _clean_query(self, text):
        opts = {}
        lines = []
        for line in text.split("\n"):
            if line.startswith("--"):
                pass
            else:
                lines.append(line)
        text = "\n".join(lines)
        return text, opts

    def query(self, text, **kwargs):
        queryString, opts = self._clean_query(text)
        response = requests.post(
            self.DATASTORE_HOST + f"/v1/projects/{self.DATASTORE_PROJECT_ID}:runQuery",
            json={
                "gqlQuery": {
                    "queryString": queryString,
                    "allowLiterals": True,
                }
            },
        )
        self.format_response(response, **kwargs)

    def getKinds(self):
        response = requests.post(
            self.DATASTORE_HOST + f"/v1/projects/{self.DATASTORE_PROJECT_ID}:runQuery",
            json={
                "query": {
                    "kind": [
                        {"name": "__kind__"},
                    ],
                }
            },
        )
        if response.status_code == 200:
            print(response.json())

    def test_connection(self):
        return requests.get(self.DATASTORE_HOST).text.strip() == "Ok"

    @classmethod
    def config(cls) -> dict:
        return {
            "DATASTORE_DATASET": cls.DATASTORE_DATASET,
            "DATASTORE_HOST": cls.DATASTORE_HOST,
            "DATASTORE_EMULATOR_HOST": cls.DATASTORE_EMULATOR_HOST,
            "DATASTORE_EMULATOR_HOST_PATH": cls.DATASTORE_EMULATOR_HOST_PATH,
            "DATASTORE_PROJECT_ID": cls.DATASTORE_PROJECT_ID,
        }


def get_args():
    parser = argparse.ArgumentParser(description="CLI for datastore")

    parser.add_argument(
        "action",
        help="Action to perform",
        choices=["query", "get", "list", "put", "delete"],
    )

    parser.add_argument(
        "content",
        help="Content to perform the action on",
        nargs="?",
    )

    parser.add_argument(
        "subcontent",
        help="Subcontent to perform the action on",
        nargs="?",
    )

    parser.add_argument(
        "--datastore-dataset",
        help="Dataset of the datastore",
        default=Datastore.DATASTORE_DATASET,
        required=False,
    )
    parser.add_argument(
        "--datastore-host",
        help="Host of the datastore",
        default=Datastore.DATASTORE_HOST,
        required=False,
    )
    parser.add_argument(
        "--datastore-emulator-host",
        help="Host of the emulator",
        default=Datastore.DATASTORE_EMULATOR_HOST,
        required=False,
    )
    parser.add_argument(
        "--datastore-emulator-host-path",
        help="Path of the emulator",
        default=Datastore.DATASTORE_EMULATOR_HOST_PATH,
        required=False,
    )
    parser.add_argument(
        "--datastore-project-id",
        help="Project ID",
        default=Datastore.DATASTORE_PROJECT_ID,
        required=False,
    )

    parser.add_argument(
        "--limit",
        help="Limit of the query",
        default=100,
        required=False,
    )
    parser.add_argument(
        "--format",
        help="Format of the output",
        action="store_true",
        default="yaml",
        required=False,
    )
    parser.add_argument(
        "--style",
        help="Style of the output",
        action="store_true",
        default="scheme",
        required=False,
    )

    args = parser.parse_args()
    return args


def main():
    args = get_args()
    client = Datastore()

    if args.action == "query":
        if args.content == "-" or args.content is None:
            client.query(sys.stdin.read(), format=args.format, style=args.style)
        else:
            client.query(args.content, format=args.format, style=args.style)
    elif args.action == "get":
        kind = args.content
        id = args.subcontent
        if id == "scheme":
            client.get_scheme(kind)
        else:
            client.get(kind, id)
    elif args.action == "list":
        if args.content == "kinds":
            client.getKinds()
        else:
            client.list(args.content, args.limit)


if __name__ == "__main__":
    main()
