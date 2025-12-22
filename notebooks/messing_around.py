# %%
from polars.selectors import categorical
from polars.testing import assert_frame_equal
import polars as pl
import duckdb
import os
import sys
import numpy as np
NOTEBOOK_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(NOTEBOOK_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
sys.path.append(PROJECT_ROOT)
# %%
lf = pl.scan_parquet(f'{DATA_DIR}/raw/arcos.pq')
lf.head().collect()


# %%
con = duckdb.connect()
con.execute('SET memory_limit = "2GB"')
con.execute('SET threads TO 4')

lf =con.sql(f'SELECT * FROM read_parquet("{DATA_DIR}/raw/arcos2.pq")').pl(lazy=True)

lf.explode("readings", "readings2").filter((pl.col("readings").is_between(7.5, 25)) | (pl.col("readings").is_between(33.43,66.89))).with_columns(year = pl.col("transaction_date").dt.year()).group_by('year', 'buyer_state').agg(avg_drugz = (pl.col("mme_conversion_factor")*pl.col("dos_str")*pl.col("quantity")*pl.col("dosage_unit")).mean()).with_columns(buyer_state = pl.col("buyer_state").cast(pl.Categorical)).sink_parquet('out1.pq')
# %%
lf = pl.scan_parquet(f"{DATA_DIR}/raw/arcos2.pq")
lf.explode("readings", "readings2").filter((pl.col("readings").is_between(7.5, 25)) | (pl.col("readings").is_between(33.43,66.89))).with_columns(year = pl.col("transaction_date").dt.year()).group_by('year', 'buyer_state').agg(avg_drugz = (pl.col("mme_conversion_factor")*pl.col("dos_str")*pl.col("quantity")*pl.col("dosage_unit")).mean()).sink_parquet('out2.pq')



# %%
df1 = pl.read_parquet('out1.pq').sort('year', 'buyer_state')
df2  = pl.read_parquet('out2.pq').sort('year', 'buyer_state')

from polars.testing import assert_frame_equal
assert_frame_equal(df1, df2)

# %%
df1

# %%
df2
# %%
n_rows = lf.select(pl.len()).collect().item()
n_rows

# %%
readings = pl.Series(np.random.uniform(1, 30, 50*n_rows).reshape(n_rows, 50))
readings2 = pl.Series(np.random.uniform(50, 100, 50*n_rows).reshape(n_rows, 50))

lf = lf.with_columns(
    readings = readings,
    readings2 = readings2
)
lf.sink_parquet(f'{DATA_DIR}/raw/arcos2.pq')