from singer import metadata

class Context():
    config = {}
    state = {}
    catalog = {}
    stream_map = {}
    stream_objects = {}
    counts = {}

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s["tap_stream_id"]: s for s in cls.catalog['streams']}

        return cls.stream_map[stream_name] if stream_name in cls.stream_map else None

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        if(stream is None): 
            return False

        stream_metadata = metadata.to_map(stream['metadata'])
        return metadata.get(stream_metadata, (), 'selected')
