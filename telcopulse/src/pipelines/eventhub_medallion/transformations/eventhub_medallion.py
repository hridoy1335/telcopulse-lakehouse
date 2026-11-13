# TelcoPulse - Event Hub medallion pipeline (bronze / silver / gold)
#
# Placeholder so Git tracks this folder: Git does not commit empty
# directories, and the bundle (resource/pipeline/telcopulse_event_hub_bronze.yml)
# references src/pipelines/eventhub_medallion as root_path, so CI validate
# fails with "stat src/pipelines/eventhub_medallion: no such file or directory"
# when the folder is missing.
#
# TODO: implement the Event Hub (Kafka endpoint) streaming bronze, the
# stream-static silver join with the Postgres bronze dims, and gold aggregates.
