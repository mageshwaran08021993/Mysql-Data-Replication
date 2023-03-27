**MYSQL binlog transaction data replication to Redshift**

This Module used to replicate the MYSQL table change data using Binary logs to any replication databases like redshift, postgres.

We used Python library - pymysqlreplication as the main component to read the binary logs and capture the ROW events.

Configs are maintained separately under the yaml file under resource folder. #TODO - Need to get params from Key Vault or Redis

The entry point for the module is start.py module. It requires mandatory params like server_id, also we can provide additional params like - blocking, stream_resume and other PYMYSQLREPLICATION module accepts.

example: python start.py --server_id 100

Deployment:
    We can deploy it using dockerfile, attached the sample docker file.

Next Release:
    -- Capture the DDL change to the replication server.