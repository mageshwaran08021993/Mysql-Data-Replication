from src.mysql_bin_log_replicate import start_stream
import argparse


parser = argparse.ArgumentParser(description="Replica function params")

# Add the arguments
# Mandatory Params
parser.add_argument('-S', '--server-id',
                    required=True,
                    dest='server_id',
                    help='Server_id is required field',
                    type=int
                    )
# Optional Params
parser.add_argument('-C', '--ctl_connection_ettings')
parser.add_argument('-R', '--resume_stream')
parser.add_argument('-B', '--blocking')
parser.add_argument('-OE', '--only_events')
parser.add_argument('-LF', '--log_file')
parser.add_argument('-LP', '--log_pos')
parser.add_argument('-ELP', '--end_log_pos')
parser.add_argument('-F', '--filter_non_implemented_events')
parser.add_argument('-I', '--ignored_events')
parser.add_argument('-AP', '--auto_position')
parser.add_argument('-OT', '--only_tables')
parser.add_argument('-IT', '--ignored_tables')
parser.add_argument('-OS', '--only_schemas')
parser.add_argument('-IS', '--ignored_schemas')
parser.add_argument('-FS', '--freeze_schema')
parser.add_argument('-STT', '--skip_to_timestamp')
args, unknown = parser.parse_known_args()

# Parse the arguments
# args = parser.parse_args()

param_dict = {}
for param in ["server_id", "resume_stream", "blocking", "only_events", "log_file", "log_pos", "end_log_pos", "filter_non_implemented_events", "ignored_events",
"auto_position", "only_tables", "ignored_tables", "only_schemas", "ignored_schemas", "freeze_schema", "skip_to_timestamp",
"report_slave", "slave_uuid", "pymysql_wrapper", "fail_on_table_metadata_unavailable", "slave_heartbeat",
"is_mariadb", "ignore_decode_errors"]:
    if getattr(args, param, None):
        param_dict[param] = getattr(args, param)

# server_id=100, log_pos=847,end_log_pos=None, is_blocking=False, is_resume_stream=False
start_stream(**param_dict)

