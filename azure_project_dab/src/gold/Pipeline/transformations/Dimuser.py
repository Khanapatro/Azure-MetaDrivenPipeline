import dlt

@dlt.table
def dimuserstg():
    df=spark.readStream.table("spotify_catalog.silver.dimuser")
    return df

dlt.create_streaming_table("dimuser")


dlt.create_auto_cdc_flow(
    target="dimuser",
    source="dimuserstg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)