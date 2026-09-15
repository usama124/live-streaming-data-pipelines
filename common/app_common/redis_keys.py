PIPELINE_STATE_EVENTS_CHANNEL = "pipeline_state_events"


def pipeline_config_key(pipeline_id: str) -> str:
    return f"pipeline:{pipeline_id}:config"


def pipeline_state_key(pipeline_id: str) -> str:
    return f"pipeline:{pipeline_id}:state"
