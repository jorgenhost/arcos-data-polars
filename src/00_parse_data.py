import os
import duckdb
import polars as pl
import utils
SRC_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

# Schema
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


csv_file = f'{DATA_DIR}/raw/arcos-full.csv'
gzip_csv_file = f'{DATA_DIR}/raw/arcos-full.csv.gz'
initial_file_size = utils.get_file_size(csv_file)


con = duckdb.connect(f'{DATA_DIR}/raw/databaz.db')

con.execute('SET memory_limit = "8GB"') 
con.execute('SET threads TO 8')

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
temp_file = f'{DATA_DIR}/raw/temp.pq'
table.write_parquet(temp_file)

lf = (pl.scan_parquet(temp_file)
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
outfile = f'{DATA_DIR}/raw/arcos.pq'

lf.sink_parquet(outfile, engine='streaming')

remove_files = [gzip_csv_file, csv_file, temp_file]

for file in remove_files:
    os.remove(file)
    print(f'Removed {file}')

final_file_size = utils.get_file_size(outfile)

print('Optimized datatypes and converted ARCOS to parquet.')
print(f'Initial file size: {initial_file_size} GB. File size after optimizing: {final_file_size} GB')