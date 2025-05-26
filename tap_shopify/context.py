import singer
from singer import metadata

LOGGER = singer.get_logger()

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
        return cls.stream_map[stream_name]

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        stream_metadata = metadata.to_map(stream['metadata'])
        return metadata.get(stream_metadata, (), 'selected')

    @classmethod
    def get_unselected_fields(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        stream_metadata = metadata.to_map(stream['metadata'])
        # All fields defined in the schema
        all_fields = set(stream["schema"]["properties"].keys())

        # Selected fields from metadata
        selected_fields = set()
        for breadcrumb, data in stream_metadata.items():
            if len(breadcrumb) == 2:
                if data.get('selected') or data.get('inclusion') == 'automatic':
                    selected_fields.add(breadcrumb[1])

        return list(all_fields - selected_fields)

    @classmethod
    def get_results_per_page(cls, default_results_per_page):
        results_per_page = default_results_per_page
        try:
            results_per_page = int(cls.config.get("results_per_page"))
        except TypeError:
            # None value or no key
            pass
        except ValueError:
            # non-int value
            log_msg = ('Failed to parse results_per_page value of "%s" ' +
                       'as an integer, falling back to default of %d')
            LOGGER.info(log_msg,
                        Context.config['results_per_page'],
                        default_results_per_page)
        return results_per_page
