import faust

class pipeline_message(faust.Record):
    processing_flow: str  # processing flow type
    run_id: str           # unique identifier for the run
    processing_step: int  # processing step id (where are we in the pipeline)
