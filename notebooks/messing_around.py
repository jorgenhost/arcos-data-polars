# %%
import os
import sys
os.environ['POLARS_MAX_THREADS'] = '8'
import polars.selectors as cs
import duckdb
import numpy as np
import polars as pl
from polars.testing import assert_frame_equal
NOTEBOOK_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(NOTEBOOK_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
sys.path.append(PROJECT_ROOT)

# %%
con = duckdb.connect()
con.execute('SET memory_limit = "8GB"') 
con.execute('SET threads TO 8')

SCHEMA = {
    "reporter_bus_act": pl.Categorical,
        "reporter_name": pl.Categorical,
        "reporter_state": pl.Categorical,
        'reporter_county': pl.Categorical,
        "buyer_bus_act": pl.Categorical,
        "buyer_state": pl.Categorical,
        "drug_name": pl.Categorical,
        "drug_code": pl.Categorical,
        "measure": pl.Categorical,
        "revised_company_name": pl.Categorical,
        "ingredient_name": pl.Categorical,
        "unit": pl.Categorical,
        "reporter_family": pl.Categorical,
        "combined_labeler_name": pl.Categorical,
        "order_form_no": pl.String,
        "dosage_unit": pl.Float64,
        'buyer_city': pl.Categorical,
        "transaction_code": pl.Categorical,
        "buyer_zip": pl.Int32,
        "reporter_zip": pl.Int32,
        "quantity": pl.Int32,
        "action_indicator": pl.Categorical,
        "correction_no": pl.Float64,
        'transaction_id': pl.Int64
}

#  %%
lf = (
    con.sql(
        f"""
        SELECT * FROM read_csv('{DATA_DIR}/raw/arcos-full.csv', 
        header=True, 
        quote = '"',
        escape = '"',
        strict_mode=false,
        sample_size = 2000000
        )
        """
    )
    .pl(lazy=True, batch_size=10_000_000)
    .with_columns(
     reporter_dea_no = pl.col("reporter_dea_no").hash(),
     buyer_dea_no = pl.col("buyer_dea_no").hash(),
     ndc_no = pl.col("ndc_no").hash()
    )
    .cast(SCHEMA)
)

lf.sink_parquet(f'{DATA_DIR}/raw/arcos.pq')
# %%
lf.select(pl.col("buyer_zip")).collect().to_series()

#%%
lf = pl.scan_parquet(f'{DATA_DIR}/raw/arcos.pq')
lf.head().collect()
# %%
lf = pl.scan_csv(f'{DATA_DIR}/raw/arcos-full.csv', ignore_errors=True).with_row_index().filter(pl.col("index")==500_000)
lf.collect()
# %%
stringz = lf.select(cs.by_dtype(pl.String)).collect_schema().names()

for col in stringz:
    try:
        s = lf.select(pl.col(col)).collect().to_series().cast(pl.Int64)
        s = s.shrink_dtype()
        lf = lf.with_columns(
            col = s
        )
        print(f'{col}')
    except Exception:
        pass
        print(f'error @ {col}')

lf.head().collect()

# %%
lf.select(pl.col("reporter_zip")).collect().to_series().cast(pl.Int64)

# %%
lf = pl.scan_parquet(f'{DATA_DIR}/raw/arcos.pq')
lf.head().collect()

# %%
con.execute(f"""
    CREATE TABLE data AS 
    SELECT * FROM read_csv('{DATA_DIR}/raw/arcos-full.csv', 
        ignore_errors=True, 
        rejects_table='errors')
        
""")

# Look at what failed:
bad_rows = con.sql("SELECT * FROM errors LIMIT 5").pl()
print(bad_rows)

# %%
con = duckdb.connect()
con.execute('SET memory_limit = "2GB"') 
con.execute('SET threads TO 4')

lf =con.sql(f'SELECT * FROM read_parquet("{DATA_DIR}/raw/arcos2.pq")').pl(lazy=True)

lf.explode("readings", "readings2").filter((pl.col("readings").is_between(7.5, 25)) | (pl.col("readings").is_between(33.43,66.89))).with_columns(year = pl.col("transaction_date").dt.year()).group_by('year', 'buyer_state').agg(avg_drugz = (pl.col("mme_conversion_factor")*pl.col("dos_str")*pl.col("quantity")*pl.col("dosage_unit")).mean()).sort('buyer_state', 'year').with_columns(buyer_state = pl.col("buyer_state").cast(pl.Categorical)).sink_parquet('out1.pq')
# %%
lf = pl.scan_parquet(f"{DATA_DIR}/raw/arcos2.pq")
lf.explode("readings", "readings2").filter((pl.col("readings").is_between(7.5, 25)) | (pl.col("readings").is_between(33.43,66.89))).with_columns(year = pl.col("transaction_date").dt.year()).group_by('year', 'buyer_state').agg(avg_drugz = (pl.col("mme_conversion_factor")*pl.col("dos_str")*pl.col("quantity")*pl.col("dosage_unit")).mean()).sort('buyer_state', 'year').sink_parquet('out2.pq')



# %%
df1 = pl.read_parquet('out1.pq').sort('year', 'buyer_state')
df2  = pl.read_parquet('out2.pq').sort('year', 'buyer_state')


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