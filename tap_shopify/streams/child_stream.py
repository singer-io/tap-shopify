from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      shopify_error_handling,
                                      OutOfOrderIdsError)

from tap_shopify.context import Context

from abc import abstractmethod


class ChildStream(Stream):

    @shopify_error_handling
    def get_children(self, value, since_id, include_since_id=True):
        params = {
            "limit": RESULTS_PER_PAGE,
            "order": 'id asc',
            self.get_parent_field_name(): value.id
        }
        if include_since_id:
            params["since_id"] = since_id
        return self.replication_object.find(**params)

    def get_objects(self):
        selected_parent = Context.stream_objects[self.get_parent_name()]()
        selected_parent.name = self.name

        # Page through all `orders`, bookmarking at `child_orders`
        for parent_object in selected_parent.get_objects():
            since_id = self.get_since_id() or 1
            while True:
                children = self.get_children(parent_object, since_id)
                for child in children:
                    if child.id < since_id:
                        raise OutOfOrderIdsError("child.id < since_id: {} < {}".format(
                            child.id, since_id))
                    yield child
                if len(children) < RESULTS_PER_PAGE:
                    break
                if children[-1].id != max([o.id for o in children]):
                    raise OutOfOrderIdsError("{} is not the max id in children ({})".format(
                        children[-1].id, max([o.id for o in children])))
                since_id = children[-1].id
                self.push_id(since_id)

    @abstractmethod
    def get_parent_name(self):
        pass

    @abstractmethod
    def get_parent_field_name(self):
        pass
