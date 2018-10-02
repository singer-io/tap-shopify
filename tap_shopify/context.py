from singer import metadata

class Context():
    config = {}
    state = {}
    catalog = {}
    tap_start = None
    stream_map = {}
    streams = {}
    stream_objects = {}

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s["tap_stream_id"]: s for s in cls.catalog['streams']}
        return cls.stream_map[stream_name]

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        stream_metadata = metadata.to_map(stream['metadata'])
        return metadata.get(stream_metadata, (), 'selected')

    @classmethod
    def has_selected_child(cls, stream_name):
        for sub_stream_name in cls.streams.get(stream_name, []):
            if cls.is_selected(sub_stream_name):
                return True
        return False

    @classmethod
    def get_schema(cls, stream_name):
        stream = [s for s in cls.catalog["streams"] if s["tap_stream_id"] == stream_name][0]
        return stream["schema"]
