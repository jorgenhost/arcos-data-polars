import os
SRC_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
import polars as pl
import utils

csv_file = f'{DATA_DIR}/raw/arcos-full.csv'
initial_file_size = utils.get_file_size(csv_file)

lf_temp = pl.scan_csv(f'{DATA_DIR}/raw/arcos-full.csv', 
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
        "quantity": pl.Int32,
        "action_indicator": pl.Categorical,
        "correction_no": pl.Float64,
        "ndc_no": pl.String,
    },
    null_values=[""])

out_temp = f'{DATA_DIR}/raw/arcos_temp.pq'
lf_temp.sink_parquet(out_temp, engine='streaming')


lf = (pl.scan_parquet(f'{DATA_DIR}/raw/arcos_temp.pq', low_memory=True)
.with_columns(
     transaction_date = pl.col("transaction_date").str.to_date(format="%m/%d/%Y"),
     reporter_dea_no = pl.col("reporter_dea_no").hash(),
     buyer_dea_no = pl.col("buyer_dea_no").hash())
)
outfile = f'{DATA_DIR}/raw/arcos.pq'

lf.sink_parquet(outfile, engine='streaming')

gzip_csv_file = f'{DATA_DIR}/raw/arcos-full.csv.gz'
remove_files = [gzip_csv_file, csv_file, out_temp]

for file in remove_files:
    os.remove(file)
    print(f'Removed {file}')

final_file_size = utils.get_file_size(outfile)

print('Optimized datatypes and converted ARCOS to parquet.')
print(f'Initial file size: {initial_file_size} GB. File size after optimizing: {final_file_size} GB')