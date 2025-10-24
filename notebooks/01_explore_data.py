# %%
import os
NOTEBOOK_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(NOTEBOOK_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
import polars as pl
# %%

lf = pl.scan_parquet(f'{DATA_DIR}/raw/arcos.pq')
lf.head().collect()
# %%
lf.select(pl.col("transaction_date")).describe()
# %%
