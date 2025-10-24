# Converting (almost) billion scale datasets on your own laptop

This project serves as an example of how to convert "big" datasets (+750 million rows) on your own laptop by optimizing data types. [Polars](https://docs.pola.rs/api/python/stable/reference/) does the heavy lifting by way of [streaming](https://docs.pola.rs/user-guide/concepts/streaming/) results to disk and [lazy](https://docs.pola.rs/user-guide/concepts/lazy-api/) evaluation. 


As an example, I use the Automation of Reports and Consolidated Orders System (ARCOS) dataset from the Drug Enforcement Agency (DEA). This dataset contains info on shipments of synthetic opioids from manufacturers to buyers (pharmacies/practitioners) from 2006-2019. The dataset contains 758,814,112 rows. Code is run on a Linux Mint 22.2 AMD 5900HX machine with 16GB RAM.

Results below:

```bash
Initial file size: 344.73 GB. File size after optimizing: 25.09 GB
```

Install uv and run
```bash
uv sync
```