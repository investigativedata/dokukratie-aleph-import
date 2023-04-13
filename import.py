import os
import sys
from functools import cache

import requests
from alephclient.api import AlephAPI
from alephclient.errors import AlephException
from alephclient.util import backoff
from banal import clean_dict, ensure_dict, ensure_list
from mmmeta import mmmeta
from nomenklatura.dataset.catalog import DataCatalog
from nomenklatura.dataset.dataset import DS, Dataset
from normality import slugify
from servicelayer.archive import init_archive
from structlog import get_logger

log = get_logger("dokukratie")

CATALOG_URL = "https://s3.investigativedata.org/dokukratie/catalog.json"
ARCHIVE_MIME = "application/json+archive"
STATES = (
    "bb",
    "be",
    "bw",
    "by",
    "hb",
    "he",
    "hh",
    "mv",
    "ni",
    "nw",
    "rp",
    "sh",
    "sl",
    "sn",
    "st",
    "th",
)


def get_document_type(data: dict) -> str:
    dtype = data.get("document_type", "")
    if not dtype:
        return "Anfrage"
    if "minor" in dtype:
        return "Kleine Anfrage"
    if "major" in dtype:
        return "Große Anfrage"
    return "Anfrage"


def ensure_str(value: str | None = None) -> str | None:
    if not value:
        return
    return str(value)


def create_meta_object(data: dict, ensure_ref_in_name=False) -> dict:
    source_url = data.get("source_url", data.get("url"))
    foreign_id = data.get(
        "foreign_id", data.get("reference", data.get("request_id", source_url))
    )

    name = data.get("title") or data.get("file_name")
    if name is None and source_url is not None:
        name = source_url.split("/")[-1]

    if name is not None:
        if ensure_ref_in_name:
            ref = data.get("reference")
            if ref is not None and ref.lower() not in name.lower():
                name = f"{ref} - {name}"

    return clean_dict(
        {
            "crawler": "dokukratie",
            "foreign_id": foreign_id,
            "source_url": source_url,
            "title": data.get("title"),
            "name": name,
            "file_name": name,
            "author": data.get("author"),
            "publisher": data.get("publisher:name"),
            "publisher_url": data.get("publisher:url"),
            "retrieved_at": ensure_str(data.get("retrieved_at")),
            "modified_at": ensure_str(data.get("modified_at")),
            "published_at": ensure_str(data.get("published_at")),
            "headers": ensure_dict(data.get("headers")),
            "keywords": ensure_list(data.get("keywords")),
            "languages": data.get("languages"),
            "countries": data.get("countries"),
            "mime_type": data.get("mime_type"),
            "parent": data.get("parent"),
        }
    )


def aleph_emit_document(
    api: AlephAPI, collection_id: int, data: dict, fp: str, dataset: str
):
    label = data.get("file_name", data.get("source_url"))
    log.info("Upload: %s", label, dataset=dataset)

    for try_number in range(api.retries):
        try:
            res = api.ingest_upload(collection_id, fp, data)
            document_id = res.get("id")
            log.info("Aleph document ID: %s", document_id, dataset=dataset)
            return
        except AlephException as exc:
            if try_number > api.retries or not exc.transient:
                log.error("Error: %s" % exc)
                return
            backoff(exc, try_number)


@cache
def make_folder(
    api: AlephAPI, collection_id: int, name: str, parent: tuple[int, str] | None = None
) -> str:
    foreign_id = slugify(name)
    if foreign_id is None:
        log.warning("No folder foreign ID!")
        return

    if parent:
        foreign_id = f"{parent[1]}/{foreign_id}"

    log.info("Make folder: %s", name, foreign_id=foreign_id)
    for try_number in range(api.retries):
        try:
            res = api.ingest_upload(
                collection_id,
                metadata=clean_dict(
                    {
                        "file_name": name,
                        "foreign_id": foreign_id,
                        "parent": {"id": parent[0]} if parent else None,
                    }
                ),
                sync=True,
            )
            document_id = res.get("id")
            log.info("Aleph folder entity ID: %s", document_id)
            return document_id, foreign_id
        except AlephException as ae:
            if try_number > api.retries or not ae.transient:
                log.error("Error: %s" % ae)
                return
            backoff(ae, try_number)


@cache
def make_folders(api: AlephAPI, collection_id: int, *paths: list[str]) -> int:
    parent = None
    folder = None
    for path in paths:
        folder = make_folder(api, collection_id, path, parent)
        parent = folder
    return folder


def ensure_collection(api: AlephAPI, dataset: DS, frequency="weekly") -> str:
    foreign_id = dataset.name
    if foreign_id.startswith("de_"):
        foreign_id = foreign_id[3:]
    if not foreign_id.startswith("de_dokukratie_"):
        foreign_id = f"de_dokukratie_{foreign_id}"
    aleph_collection = api.get_collection_by_foreign_id(foreign_id)
    dataset = dataset.to_dict()
    data = {
        "label": dataset["title"],
        "summary": (
            dataset.get("description", "") + "\n\n" + dataset.get("summary", "")
        ).strip(),
        "publisher": dataset.get("publisher", {}).get("name"),
        "publisher_url": dataset.get("publisher", {}).get("url"),
        "countries": ensure_list(dataset.get("publisher", {}).get("country")),
        "data_url": dataset.get("data", {}).get("url"),
        "category": dataset.get("category", "library"),
    }
    if "frequency" in dataset or frequency is not None:
        data["frequency"] = dataset.get("frequency", frequency)

    if aleph_collection is not None:
        log.info("[%s] Updating collection metadata ..." % foreign_id)
        # don't overwrite existing (probably user changed) category:
        data.pop("category", None)
        aleph_collection = api.update_collection(
            aleph_collection["collection_id"], data
        )
    else:
        log.info("[%s] Creating collection ..." % foreign_id)
        aleph_collection = api.create_collection({**data, **{"foreign_id": foreign_id}})

    return aleph_collection["collection_id"]


if __name__ == "__main__":
    res = requests.get(CATALOG_URL)
    if not res.ok:
        raise requests.HTTPError(f"Fetch catalog failed: {res.status_code}")

    catalog = DataCatalog(Dataset, res.json())
    log.info("Loaded catalog.", url=CATALOG_URL)
    api = AlephAPI()

    include = sys.argv[1]

    for dataset in catalog.datasets:
        if include and dataset.name != include:
            log.info("Skipping dataset.", dataset=dataset.name)
            continue

        log.info("Lodaded dataset.", dataset=dataset.name)
        storage = None
        for resource in dataset.resources:
            if resource.mime_type == ARCHIVE_MIME:
                res = requests.get(resource.url)
                if not res.ok:
                    raise requests.HTTPError(
                        f"Fetch resource failed: {res.status_code}"
                    )
                archive_manifest = res.json()
                storage = init_archive(**archive_manifest)

        if storage is None:
            log.warn("No archive storage configured for dataset", dataset=dataset.name)
            continue

        collection_id = ensure_collection(api, dataset)
        log.info("[%s] Loading mmmeta ..." % dataset.name)
        m = mmmeta()
        m.update()

        log.info("mmmeta: %d files" % len(m.files))

        for file in m.files:
            if file["imported"]:
                log.info(
                    "Skipping already imported file", foreign_id=file["foreign_id"]
                )
                continue

            if (
                file["publisher:type"] == "parliament"
                and not file["legislative_term"]  # noqa
            ):
                log.warn("No legislative term", file=file.serialize())
                continue

            key = storage._locate_key(file["content_hash"])
            if key and key.endswith(".json"):
                log.warn("key: %s" % key, dataset=dataset.name)
                continue
            fp = storage.load_file(file["content_hash"])
            if fp is None:
                log.warn(
                    "No file found for content hash: %s" % file["content_hash"],
                    dataset=dataset.name,
                )
                continue

            if not file["foreign_id"]:
                file["foreign_id"] = file["reference"]

            if dataset.name in ("sehrgutachten", "de_vsberichte"):
                data = clean_dict(create_meta_object(file.serialize()))
                if data.get("publisher"):
                    parent, _ = make_folders(api, collection_id, data["publisher"])
                    data["parent"] = {"id": parent}

                if dataset.name == "de_vsberichte":
                    if data["publisher"] not in data["file_name"]:
                        data["file_name"] = f'{data["publisher"]} - {data["file_name"]}'
            elif dataset.name in STATES:
                data = clean_dict(create_meta_object(file.serialize(), True))
                parent, _ = make_folders(
                    api,
                    collection_id,
                    f"{file['legislative_term']}. Wahlperiode",
                    "Drucksache",
                    get_document_type(file.serialize()),
                )
                data["parent"] = {"id": parent}
            else:
                log.error("Unsupported dataset: %s" % dataset.name)
                continue

            aleph_emit_document(api, collection_id, data, fp, dataset.name)
            file["imported"] = True
            file.save()
            os.remove(fp)
