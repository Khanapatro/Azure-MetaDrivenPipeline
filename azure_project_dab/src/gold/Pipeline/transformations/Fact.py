import dlt

@dlt.table
def factstreamstg():
    df=spark.readStream.table("spotify_catalog.silver.factstream")
    return df

dlt.create_streaming_table("factstream")


dlt.create_auto_cdc_flow(
    target="factstream",
    source="factstreamstg",
    keys=["stream_id"],
    sequence_by="stream_timestamp",
    stored_as_scd_type=1
)