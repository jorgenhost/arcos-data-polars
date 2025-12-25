# %%
import os
import sys
os.environ['POLARS_MAX_THREADS'] = '8'
import polars.selectors as cs
import duckdb
import polars as pl
from polars.testing import assert_frame_equal
NOTEBOOK_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(NOTEBOOK_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
sys.path.append(PROJECT_ROOT)

# %%
con = duckdb.connect(f'{DATA_DIR}/raw/databaz.db')
con.execute('SET memory_limit = "8GB"') 
con.execute('SET threads TO 8')

# %%
SCHEMA = {
    "reporter_bus_act": pl.Categorical,
        "reporter_name": pl.Categorical,
        "reporter_state": pl.Categorical,
        'reporter_county': pl.Categorical,
        "buyer_bus_act": pl.Categorical,
        "buyer_state": pl.Categorical,
        "buyer_county": pl.Categorical,
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
        "dos_str": pl.Float64,
        "mme_conversion_factor": pl.Float64,
        "strength": pl.Int16,
        "dosage_unit": pl.Float64,
        "calc_base_wt_in_gm": pl.Float64,
        "transaction_id": pl.Float64
}

#  %%
table = (
    con.sql(
        f"""
        SELECT * FROM read_csv('{DATA_DIR}/raw/arcos-full.csv', 
            strict_mode = false,
            null_padding = true,
            store_rejects = true,
            quote = '"',
            all_varchar = true,
            escape = '"'
        )
        """
    )
)
table.write_parquet(f'{DATA_DIR}/raw/temp.pq')


# %%

lf = (pl.scan_parquet(f'{DATA_DIR}/raw/temp.pq')
        .cast(SCHEMA)
        .with_columns(
            reporter_dea_no = pl.when(pl.col("reporter_dea_no").is_not_null()).then(pl.col("reporter_dea_no").hash()),
            buyer_dea_no = pl.when(pl.col("buyer_dea_no").is_not_null()).then(pl.col("buyer_dea_no").hash()),
            order_form_no = pl.when(pl.col("order_form_no").is_not_null()).then(pl.col("order_form_no").hash()),
            ndc_no = pl.when(pl.col("ndc_no").is_not_null()).then(pl.col("ndc_no").hash()),
            correction_no = pl.when(pl.col("correction_no").is_not_null()).then(pl.col("correction_no").hash()),
            transaction_date = pl.col("transaction_date").str.to_date(format="%m/%d/%Y"),
            transaction_id = pl.col("transaction_id").cast(pl.Int64)
        )

)
lf.sink_parquet(f'{DATA_DIR}/raw/arcos.pq')
# %%
lf = pl.scan_parquet(f'{DATA_DIR}/raw/arcos.pq')
lf.head().collect()
# %%
