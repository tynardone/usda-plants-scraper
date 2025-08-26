from __future__ import annotations

import asyncio
import csv
import os
import random
import re
from collections.abc import Iterable
from typing import Any

import httpx
import pandas as pd
from tqdm import tqdm

PLANT_PROFILE_URL = "https://plantsservices.sc.egov.usda.gov/api/PlantProfile"
PLANT_CHAR_URL = "https://plantsservices.sc.egov.usda.gov/api/PlantCharacteristics"


PLANT_KEYS = [
    "Id",
    "Symbol",
    "ScientificName",
    "CommonName",
    "Group",
    "RankId",
    "Rank",
    "HasCharacteristics",
    "HasDistributionData",
    "HasImages",
    "HasRelatedLinks",
    "NumImages",
    "ProfileImageFilename",
    "PlantNotes",
    "Durations",
    "GrowthHabits",
    "HasLegalStatuses",
    "LegalStatuses",
    "HasNoxiousStatuses",
    "NoxiousStatuses",
]

TAG_RE = re.compile(r"<.*?>")


def strip_html(s: str | None) -> str | None:
    return TAG_RE.sub("", s) if isinstance(s, str) else s


async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    params: dict | None = None,
    max_retries: int = 4,
) -> Any | None:
    delay = 0.5

    for _ in range(max_retries):
        try:
            r = await client.get(url, params=params)
            if r.status_code == 200:
                return r.json()

            if r.status_code in (429, 500, 502, 503, 504):
                await asyncio.sleep(delay + random.uniform(0, 0.2))
                delay *= 2
                continue

            return None

        except httpx.RequestError:
            await asyncio.sleep(delay + random.uniform(0, 0.2))
            delay *= 2

    return None


async def fetch_profile(client: httpx.AsyncClient, symbol: str) -> dict | None:
    return await fetch_json(client, PLANT_PROFILE_URL, params={"symbol": symbol})


async def fetch_characteristics(
    client: httpx.AsyncClient, plant_id: int
) -> list | None:
    url = f"{PLANT_CHAR_URL}/{plant_id}"
    data = await fetch_json(client, url)
    return data if isinstance(data, list) else None


def normalize_record_to_rows(record: dict) -> tuple[dict, list[dict], list[dict]]:
    """
    Returns:
        plant_row: dict (one row)
        native_rows: list[dict] (zero or more)
        acnester_rows: list[dict] (zero or more)
    """
    plant_row = {k: record.get(k) for k in PLANT_KEYS}
    plant_row["ScientificName"] = strip_html(plant_row["ScientificName"])

    native_rows = [
        {"PlantID": record.get("Id"), **ns} for ns in record.get("NativeStatuses", [])
    ]

    ancestor_rows = [
        {
            "PlantID": record.get("Id"),
            "Id": anc.get("Id"),
            "Symbol": anc.get("Symbol"),
            "ScientificName": strip_html(anc.get("ScientificName")),
            "CommonName": anc.get("CommonName"),
            "RankId": anc.get("RankId"),
            "Rank": anc.get("Rank"),
        }
        for anc in record.get("Ancestors", [])
    ]

    return plant_row, native_rows, ancestor_rows


def normalize_characteristics_to_row(plant_id: int, items: list[dict]) -> list[dict]:
    return [
        {
            "PlantID": plant_id,
            "PlantCharacteristicName": char.get("PlantCharacteristicName"),
            "PlantCharacteristicValue": char.get("PlantCharacteristicValue"),
            "PlantCharacteristicCategory": char.get("PlantCharacteristicCategory"),
            "CultivarName": char.get("CultivarName"),
            "SynonymName": char.get("SynonymName"),
        }
        for char in (items or [])
    ]


async def build_dataframes(
    symbols: Iterable[str], concurrency: int = 10
) -> dict[str, pd.DataFrame]:
    limits = httpx.Limits(
        max_keepalive_connections=concurrency, max_connections=concurrency
    )
    timeout = httpx.Timeout(30.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
        sem = asyncio.Semaphore(concurrency)

        async def process_symbol(
            symbol: str,
        ) -> tuple[dict, list[dict], list[dict], list[dict]] | None:
            async with sem:
                profile = await fetch_profile(client, symbol)
            if not profile:
                return None

            plant_row, native_rows, ancestor_rows = normalize_record_to_rows(profile)

            char_rows: list[dict] = []

            if profile.get("HasCharacteristics") and profile.get("Id") is not None:
                async with sem:
                    chars = await fetch_characteristics(client, int(profile["Id"]))

                if chars:
                    char_rows = normalize_characteristics_to_row(
                        int(profile["Id"]), chars
                    )

            return plant_row, native_rows, ancestor_rows, char_rows

        tasks = [asyncio.create_task(process_symbol(sym)) for sym in symbols]

        results: list[tuple | None] = []
        for fut in tqdm(
            asyncio.as_completed(tasks),
            total=len(tasks),
            desc="Fetching plants data...",
        ):
            res = await fut
            results.append(res)

        # collect rows
        plant_rows: list[dict] = []
        native_rows: list[dict] = []
        ancestor_rows: list[dict] = []
        char_rows: list[dict] = []

        for res in results:
            if not res:
                continue

            plant_row, n_rows, a_rows, c_rows = res

            if plant_row:
                plant_rows.append(plant_row)

            native_rows.extend(n_rows or [])
            ancestor_rows.extend(a_rows or [])
            char_rows.extend(c_rows or [])

    plants_df = (
        pd.DataFrame(plant_rows).drop_duplicates(subset=["Id"]).reset_index(drop=True)
    )
    native_status_df = (
        pd.DataFrame(native_rows)
        if native_rows
        else pd.DataFrame(columns=["PlantID", "Region", "Status", "Type"])
    )

    ancestors_df = (
        pd.DataFrame(ancestor_rows)
        if ancestor_rows
        else pd.DataFrame(
            columns=[
                "PlantID",
                "AncestorId",
                "Symbol",
                "ScientificName",
                "CommonName",
                "RankId",
                "Rank",
            ]
        )
    )

    characteristics_df = (
        pd.DataFrame(char_rows)
        if char_rows
        else pd.DataFrame(
            columns=[
                "PlantID",
                "PlantCharacteristicName",
                "PlantCharacteristicValue",
                "PlantCharacteristicCategory",
                "CultivarName",
                "SynonymName",
            ]
        )
    )
    return {
        "plants_df": plants_df,
        "native_status_df": native_status_df,
        "ancestors_df": ancestors_df,
        "characteristics_df": characteristics_df,
    }


def load_symbols(file: str) -> list[str]:
    out: list[str] = []
    with open(file, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            out.append(row["Symbol"])
    return out


if __name__ == "__main__":
    symbols = load_symbols("input.csv")

    dfs = asyncio.run(build_dataframes(symbols, concurrency=8))

    os.makedirs("data", exist_ok=True)

    dfs["plants_df"].to_csv("data/plants.csv", index=False)
    dfs["native_status_df"].to_csv("data/native_status.csv", index=False)
    dfs["ancestors_df"].to_csv("data/ancestors.csv", index=False)
    # Only non-empty characteristics will have columns
    dfs["characteristics_df"].to_csv("data/characteristics.csv", index=False)

    # Quick peek
    print("plants_df\n", dfs["plants_df"].head())
    print("native_status_df\n", dfs["native_status_df"].head())
    print("ancestors_df\n", dfs["ancestors_df"].head())
    print("characteristics_df\n", dfs["characteristics_df"].head())
