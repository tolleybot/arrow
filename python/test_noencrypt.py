import sys

sys.path.append("/Users/dtolley/Documents/Projects/arrow/python")
import base64
import numpy as np
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.parquet.encryption as pqe


class NoOpKmsClient(pqe.KmsClient):
    def __init__(self):
        super().__init__()

    def wrap_key(self, key_bytes: bytes, _: str) -> bytes:
        b = base64.b64encode(key_bytes)
        return b

    def unwrap_key(self, wrapped_key: bytes, _: str) -> bytes:
        b = base64.b64decode(wrapped_key)
        return b


row_count = pow(2, 15) + 1
table = pa.Table.from_arrays(
    [pa.array(np.random.rand(row_count), type=pa.float32())], names=["foo"]
)

scan_options = ds.ParquetFragmentScanOptions()
file_format = ds.ParquetFileFormat(default_fragment_scan_options=scan_options)
write_options = file_format.make_write_options()


with tempfile.TemporaryDirectory() as tempdir:
    path = tempdir + "/test-dataset"
    ds.write_dataset(table, path, format=file_format, file_options=write_options)

    file_path = path + "/part-0.parquet"
    new_table = pq.ParquetFile(file_path).read()
    assert table == new_table

    dataset = ds.dataset(path, format=file_format)
    new_table = dataset.to_table()
    assert table == new_table
