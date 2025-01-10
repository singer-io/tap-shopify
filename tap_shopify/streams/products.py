from tap_shopify.streams.graphql_base import GraphQLStream
from tap_shopify.context import Context

class Products(GraphQLStream):
    name = 'products'
    
    def get_graphql_query(self):
        return """
        query($first: Int!, $after: String, $query: String) {
          products(first: $first, after: $after, query: $query) {
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              node {
                id
                title
                handle
                productType
                createdAt
                updatedAt
                publishedAt
                description
                descriptionHtml
                vendor
                tags
                variants(first: 100) {
                  edges {
                    node {
                      id
                      title
                      sku
                      price
                      compareAtPrice
                      inventoryQuantity
                      selectedOptions {
                        name
                        value
                      }
                    }
                  }
                }
                images(first: 100) {
                  edges {
                    node {
                      id
                      src
                      altText
                    }
                  }
                }
                options {
                  id
                  name
                  values
                }
              }
            }
          }
        }
        """

    def get_connection_from_data(self, data):
        return data['products']

    def process_node(self, node):
        # Transform the nested GraphQL response into a flatter structure
        processed = {
            'id': node['id'],
            'title': node['title'],
            'handle': node['handle'],
            'product_type': node['productType'],
            'created_at': node['createdAt'],
            'updated_at': node['updatedAt'],
            'published_at': node['publishedAt'],
            'description': node['description'],
            'description_html': node['descriptionHtml'],
            'vendor': node['vendor'],
            'tags': node['tags'],
            'options': node['options'],
            'variants': [
                {
                    'id': variant['node']['id'],
                    'title': variant['node']['title'],
                    'sku': variant['node']['sku'],
                    'price': variant['node']['price'],
                    'compare_at_price': variant['node']['compareAtPrice'],
                    'inventory_quantity': variant['node']['inventoryQuantity'],
                    'selected_options': variant['node']['selectedOptions']
                }
                for variant in node['variants']['edges']
            ],
            'images': [
                {
                    'id': image['node']['id'],
                    'src': image['node']['src'],
                    'alt_text': image['node']['altText']
                }
                for image in node['images']['edges']
            ]
        }
        return processed

Context.stream_objects['products'] = Products
