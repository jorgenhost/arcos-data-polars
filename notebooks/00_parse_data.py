# %%
import os
import sys
NOTEBOOK_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(NOTEBOOK_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
sys.path.append(PROJECT_ROOT)
import polars as pl
import polars.selectors as cs
from src import utils



# %%
lf = pl.scan_csv(f'{DATA_DIR}/raw/arcos-full.csv', 
schema_overrides={
        "reporter_bus_act": pl.Categorical,
        "report_name": pl.Categorical,
        "reporter_state": pl.Categorical,
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
        "transaction_code": pl.Categorical,
        "buyer_zip": pl.Int32,
        "reporter_zip": pl.Int32,
        "action_indicator": pl.Categorical,
        "correction_no": pl.Int32,
        "ndc_no": pl.String,
        # "transaction_date": pl.Date
    },
    null_values=[""])
lf.head(10000000).collect()

# %%

gzip_tsv_file = f'{DATA_DIR}/raw/arcos-full.csv'
tsv_file = f'{DATA_DIR}/raw/arcos_all_washpost.tsv'

utils.get_file_size(gzip_tsv_file)
# %%


lf_temp = pl.scan_csv(
    tsv_file,
    separator='\t',
    low_memory=True,
    null_values=['null'],
schema_overrides={
        "REPORTER_BUS_ACT": pl.Categorical,
        "REPORTER_NAME": pl.Categorical,
        "REPORTER_STATE": pl.Categorical,
        "BUYER_BUS_ACT": pl.Categorical,
        "BUYER_STATE": pl.Categorical,
        "DRUG_NAME": pl.Categorical,
        "DRUG_CODE": pl.Int16,
        "Measure": pl.Categorical,
        "Revised_Company_Name": pl.Categorical,
        "Ingredient_Name": pl.Categorical,
        "UNIT": pl.Categorical,
        "Reporter_family": pl.Categorical,
        "Combined_Labeler_Name": pl.Categorical,
        "TRANSACTION_CODE": pl.Categorical,
        "BUYER_ZIP": pl.Int32,
        "REPORTER_ZIP": pl.Int32,
        "ACTION_INDICATOR": pl.Categorical,
        "CORRECTION_NO": pl.Int32,
        "NDC_NO": pl.String,
    }
).select(
    pl.all().name.to_lowercase()
)

out_temp = f'{DATA_DIR}/raw/arcos_temp.pq'
lf_temp.sink_parquet(out_temp, engine='streaming')


# %%
lf = (pl.scan_parquet(f'{DATA_DIR}/raw/arcos_temp.pq', low_memory=True)
.with_columns(
     transaction_date = pl.col("transaction_date").cast(pl.String).str.zfill(8)
       .str.to_date("%m%d%Y")
       .alias("date_column")
).with_columns(
     reporter_dea_no = pl.col("reporter_dea_no").hash(),
     buyer_dea_no = pl.col("buyer_dea_no").hash(),
     ndc_no = pl.col("ndc_no").str.strip_chars("**").cast(pl.Int64),
     drug_code = pl.col("drug_code").cast(pl.Int16)
)
)
lf.sink_parquet(f'{DATA_DIR}/raw/arcos.pq', engine='streaming')

remove_files = [gzip_tsv_file, tsv_file, out_temp]

for file in remove_files:
    os.remove(out_temp)