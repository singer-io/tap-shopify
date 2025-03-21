from tap_shopify.context import Context
from tap_shopify.streams.graphql import ShopifyGqlStream


class Collections(ShopifyGqlStream):
    """Stream class for Shopify collections."""
    name = "collections"
    data_key = "collections"
    replication_key = "updatedAt"

    # pylint: disable=arguments-differ
    def get_query_params(self, updated_at_min, updated_at_max, cursor=None):
        """
        Returns query and params for filtering and pagination.

        Args:
            updated_at_min (str): Minimum updated_at timestamp.
            updated_at_max (str): Maximum updated_at timestamp.
            cursor (str, optional): Pagination cursor.

        Returns:
            dict: Query parameters.
        """
        filter_key = "updated_at"
        params = {
            "query": f"{filter_key}:>='{updated_at_min}' AND {filter_key}:<'{updated_at_max}'",
            "first": self.results_per_page,
        }
        if cursor:
            params["after"] = cursor
        return params

    def transform_products(self, data):
        """
        Transforms the products data by extracting product IDs and handling pagination.

        Args:
            data (dict): Product data.

        Returns:
            list: List of product IDs.
        """
        product_ids = []

        # Extract product IDs from the first page
        product_ids.extend(
            node["id"]
            for item in data["products"]["edges"]
            if (node := item.get("node")) and "id" in node
        )

        # Handle pagination
        page_info = data["products"].get("pageInfo", {})
        while page_info.get("hasNextPage"):
            params = {
                "first": self.results_per_page,
                "query": f"id:{data['id'].split('/')[-1]}",
                "childafter": page_info.get("endCursor"),
            }

            # Fetch the next page of data
            response = self.call_api(params)
            products_data = response.get("node", {}).get("products", {})
            product_ids.extend(
                node["id"]
                for item in products_data.get("edges", [])
                if (node := item.get("node")) and "id" in node
            )
            page_info = products_data.get("pageInfo", {})

        return product_ids

    def transform_object(self, obj):
        """
        Transforms a collection object.

        Args:
            obj (dict): Collection object.

        Returns:
            dict: Transformed collection object.
        """
        obj["type"] = "SMART" if obj.get("ruleSet") else "MANUAL"
        if obj.get("products"):
            obj["products"] = self.transform_products(obj)
        return obj

    def get_query(self):
        """
        Returns the GraphQL query for fetching collections.

        Returns:
            str: GraphQL query string.
        """
        return """
        query Collections($first: Int!, $after: String, $query: String, $childafter: String) {
            collections(first: $first, after: $after, query: $query, sortKey: UPDATED_AT) {
                edges {
                    node {
                        id
                        title
                        handle
                        updatedAt
                        productsCount {
                            count
                            precision
                        }
                        sortOrder
                        ruleSet {
                            appliedDisjunctively
                            rules {
                                column
                                condition
                                relation
                            }
                        }
                        seo {
                            description
                            title
                        }
                        feedback {
                            summary
                        }
                        products(first: 250, sortKey: ID, after: $childafter) {
                            edges {
                                node {
                                    id
                                }
                            }
                            pageInfo {
                                endCursor
                                hasNextPage
                            }
                        }
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """


Context.stream_objects["collections"] = Collections
