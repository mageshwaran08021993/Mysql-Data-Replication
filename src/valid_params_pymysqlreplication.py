
def replicate_input_params_validate(**provided_params):
    """
        Function to validate the replicate function params
    :param provided_params:
    :return:
    """
    valid_params = {
                "connection_settings": "",
                "server_id" : "",
                "ctl_connection_ettings": None,
                "resume_stream": False,
                "blocking" : False,
                "only_events" : None,
                "log_file" : None,
                "log_pos" : None,
                "end_log_pos" : None,
                "filter_non_implemented_events" : True,
                "ignored_events" : None,
                "auto_position" : None,
                "only_tables" : None,
                "ignored_tables" : None,
                "only_schemas" : None,
                "ignored_schemas" : None,
                "freeze_schema" : False,
                "skip_to_timestamp" : None,
                "report_slave" : None,
                "slave_uuid" : None,
                "pymysql_wrapper" : None,
                "fail_on_table_metadata_unavailable" : False,
                "slave_heartbeat" : None,
                "is_mariadb" : False,
                "ignore_decode_errors" : False
    }

    not_valid_params = set(provided_params.keys()) - set(valid_params.keys())
    for key in not_valid_params:
        del provided_params[key]
    return provided_params
