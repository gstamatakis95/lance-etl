"""Schema-evolution test asserting the inverted index lands on the right field.

After a column drop and a column add, the Arrow positional index of the new column diverges from its Lance field id.
Committing an index keyed by the positional index would attach it to the wrong field. This exercises the
``lance_field_id`` fix end-to-end through the distributed INVERTED path.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import lance
import pyarrow as pa
import pytest
from lance.dataset import Index

from lance_etl.indexing import lance_field_id, split_evenly


@pytest.fixture
def evolved_uri(tmp_path: Path) -> str:
    """Write a dataset, drop a column, and add a text column via SQL.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        The URI of the evolved dataset.
    """
    uri: str = str(tmp_path / "evolved.lance")
    rows: int = 1000
    table: pa.Table = pa.table(
        {
            "id": pa.array(range(rows), pa.int64()),
            "legacy": pa.array([f"old{i}" for i in range(rows)]),
            "category": pa.array([f"cat{i % 4}" for i in range(rows)]),
        }
    )
    lance.write_dataset(table, uri, max_rows_per_file=250)
    dataset: lance.LanceDataset = lance.dataset(uri)
    dataset.drop_columns(["legacy"])
    dataset.add_columns({"text": "concat('word', CAST(id % 10 AS STRING), ' common')"})
    return uri


def test_field_id_diverges_from_positional_index(evolved_uri: str) -> None:
    """The Lance field id of the evolved column differs from its Arrow position."""
    dataset: lance.LanceDataset = lance.dataset(evolved_uri)
    positional: int = dataset.schema.get_field_index("text")
    field_id: int = lance_field_id(dataset, "text")
    assert positional == 2
    assert field_id != positional


def test_inverted_index_lands_on_evolved_field(evolved_uri: str) -> None:
    """The INVERTED segment path commits against the correct Lance field id."""
    dataset: lance.LanceDataset = lance.dataset(evolved_uri)
    fragment_ids: list[int] = [fragment.fragment_id for fragment in dataset.get_fragments()]
    version: int = dataset.version
    index_uuid: str = str(uuid.uuid4())
    for group in split_evenly(fragment_ids, 2):
        shard_dataset: lance.LanceDataset = lance.dataset(evolved_uri, version=version)
        for fragment_id in group:
            shard_dataset.create_scalar_index(
                column="text",
                index_type="INVERTED",
                name="text_fts_idx",
                replace=False,
                index_uuid=index_uuid,
                fragment_ids=[fragment_id],
                with_position=False,
            )
    dataset.merge_index_metadata(index_uuid, index_type="INVERTED")
    current: lance.LanceDataset = lance.dataset(evolved_uri)
    field_id: int = lance_field_id(current, "text")
    index: Index = Index(
        uuid=index_uuid,
        name="text_fts_idx",
        fields=[field_id],
        dataset_version=current.version,
        fragment_ids=set(fragment_ids),
        index_version=0,
    )
    operation = lance.LanceOperation.CreateIndex(new_indices=[index], removed_indices=[])
    lance.LanceDataset.commit(evolved_uri, operation, read_version=current.version)

    committed: lance.LanceDataset = lance.dataset(evolved_uri)
    listed: list[dict[str, object]] = [item for item in committed.list_indices() if item["name"] == "text_fts_idx"]
    assert len(listed) == 1
    assert listed[0]["fields"] == ["text"]
    result = committed.to_table(full_text_query="word4")
    assert result.num_rows == 100
    assert set(value % 10 for value in result["id"].to_pylist()) == {4}
