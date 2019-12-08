#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata, metrics, bookmarks
from datetime import datetime, timedelta, date


from .nikabot_client import NikabotClient

REQUIRED_CONFIG_KEYS = ["team", "token"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        stream_key_properties = []

        # create and add catalog entry
        catalog_entry = {"stream": schema_name, "tap_stream_id": schema_name, "schema": schema, "metadata": [], "key_properties": []}
        streams.append(catalog_entry)

    return {"streams": streams}


def get_selected_streams(catalog):
    """
    Gets selected streams.  Checks schema's 'selected' first (legacy)
    and then checks metadata (current), looking for an empty breadcrumb
    and mdata with a 'selected' entry
    """
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = metadata.to_map(stream.metadata)

        if stream.schema.selected:
            selected_streams.append(stream.tap_stream_id)

        # stream metadata will have an empty breadcrumb
        if metadata.get(stream_metadata, (), "selected"):
            selected_streams.append(stream.tap_stream_id)

    return selected_streams


def sync(config, state, catalog):
    LOGGER.info(f"sync: (config: {config})")
    LOGGER.info(f"sync: (state): {state}")
    LOGGER.info(f"sync: (catalog): {catalog}")

    selected_stream_ids = get_selected_streams(catalog)

    # Loop over streams in catalog
    for stream in catalog.streams:
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        if stream_id in selected_stream_ids:
            # TODO: sync code for stream goes here...
            LOGGER.info("Syncing stream:" + stream_id)
            if stream_id == "timesheets":
                get_timesheets(config, stream)
            else:
                get_entities(config, stream)

    return


def get_timesheets(config, stream):
    client = NikabotClient(config["team"], config["token"])
    stream_id = stream.tap_stream_id
    LOGGER.info(f"get_timesheets")
    write_stream_schema(stream)


    history_days = 180
    request_interval = 30

    start = date.today() - timedelta(days=history_days)
    current = start

    # We need to increment the start date by the minimum step
    # so that we aren't requesting the enddate twice (once as the end, and once as the start)
    request_increment_delta = timedelta(days=1)
    request_interval_delta = timedelta(days=request_interval)
    
    with metrics.record_counter(stream_id) as counter:
        while current < date.today():
            params =  {
                "dateStart":(current+request_increment_delta).strftime("%Y%m%d"),
                "dateEnd": (current+request_interval_delta).strftime("%Y%m%d")
            }
            extraction_time = singer.utils.now()

            timesheets = client.get_paged("records", params)
            for rec in timesheets:
                singer.write_record(stream_id, rec, time_extracted=extraction_time)
                # singer.write_bookmark(state, "users", "commits", {"since": singer.utils.strftime(extraction_time)})
                counter.increment()

            current = current + request_interval_delta


def get_entities(config, stream):
    client = NikabotClient(config["team"], config["token"])
    stream_id = stream.tap_stream_id
    
    LOGGER.info(f"get_entities: {stream_id}")
    write_stream_schema(stream)

    extraction_time = singer.utils.now()

    users = client.get_paged(stream_id, {})
    with metrics.record_counter(stream_id) as counter:
        for rec in users:
            singer.write_record(stream_id, rec, time_extracted=extraction_time)
            # singer.write_bookmark(state, "users", "commits", {"since": singer.utils.strftime(extraction_time)})
            counter.increment()

def write_stream_schema(stream):
    stream_id = stream.tap_stream_id
    stream_schema = stream.schema
    stream_metadata = stream.metadata
    
    LOGGER.info(f"get_entities: {stream_id}")
    singer.write_schema(stream_id, f"{stream_schema}", stream.key_properties)



@utils.handle_top_exception(LOGGER)
def main():

    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()

        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
