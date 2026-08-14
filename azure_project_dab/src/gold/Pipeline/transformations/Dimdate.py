import dlt

@dlt.table
def dimdatestg():
    df=spark.readStream.table("spotify_catalog.silver.dimdate")
    return df

dlt.create_streaming_table("dimdate")


dlt.create_auto_cdc_flow(
    target="dimdate",
    source="dimdatestg",
    keys=["date_key"],
    sequence_by="date",
    stored_as_scd_type=2
)