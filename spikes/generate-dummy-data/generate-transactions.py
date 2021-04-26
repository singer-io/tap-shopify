import os
import requests
import shopify

# This is the same API_KEY that goes into the tap config
API_KEY = os.getenv('SHOPIFY_API_KEY')
shop = 'talenddatawearhouse'

BASE_URL = f'https://{shop}.myshopify.com'

headers = {
    "X-Shopify-Access-Token" : f"{API_KEY}"
}

def makeCollection(i):
    """
    You can create a collection in the test store and capture the POST it makes in the DevTools' Network tab.
    """

    headers = {
        'authority': 'FIXME',
        'pragma': 'FIXME',
        'cache-control': 'FIXME',
        'sec-ch-ua': 'FIXME',
        'accept': 'FIXME',
        'content-type': 'FIXME',
        'x-csrf-token': 'FIXME',
        'sec-ch-ua-mobile': 'FIXME',
        'user-agent': 'FIXME',
        'x-shopify-web-force-proxy': 'FIXME',
        'origin': 'FIXME',
        'sec-fetch-site': 'FIXME',
        'sec-fetch-mode': 'FIXME',
        'sec-fetch-dest': 'FIXME',
        'accept-language': 'FIXME',
        'cookie': 'FIXME',
    }
    title_base = 'FIXME'
    title = title_base + '_' + str(i)
    raw_input = '{"operationName":"CreateCollection","variables":{"collection":{"title":"' + title + '","descriptionHtml":"" ,"seo":{"title":"","description":""},"handle":"","templateSuffix":"","image":null}},"query":"mutation CreateCollection($collection: CollectionInput\u0021) {\\n  collectionCreate(input: $collection) {\\n    collection {\\n      id\\n      title\\n descriptionHtml\\n      sortOrder\\n      handle\\n      templateSuffix\\n      ruleSet {\\n        appliedDisjunctively\\n rules {\\n          column\\n          relation\\n          condition\\n          __typename\\n        }\\n        __typename\\n      }\\n      ...SEOCardCollection\\n      ...CollectionImageCard\\n      ...ChannelAvailabilityCollection\\n      __typename\\n    }\\n    userErrors {\\n      field\\n      message\\n      __typename\\n    }\\n    __typename\\n  }\\n}\\n\\nfragment SEOCardCollection on Collection {\\n  seo {\\n    title\\n    description\\n    __typename\\n  }\\n  __typename\\n}\\n \\nfragment CollectionImageCard on Collection {\\n  image {\\n    id\\n    altText\\n    src\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment ChannelAvailabilityCollection on Collection {\\n  id\\n  resourcePublications(first: 250, onlyPublished : false) {\\n    edges {\\n      node {\\n        publishDate\\n        isPublished\\n        publication {\\n          id\\n ...PublicationVisibilityPublication\\n          __typename\\n        }\\n        __typename\\n      }\\n      __typename\\n    }\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment PublicationVisibilityPublication on Publication {\\n  id\\n  name\\n  supportsFuturePublishing\\n  app {\\n    id\\n    title\\n    handle\\n    feedback {\\n      messages {\\n message\\n        __typename\\n      }\\n      link {\\n        url\\n        label\\n        __typename\\n      }\\n      __typename\\n    }\\n    __typename\\n  }\\n  __typename\\n}\\n"}'

    return requests.post(f'{BASE_URL}/admin/internal/web/graphql/core?operation=CreateCollection',
                         data=raw_input,
                         headers=headers)


def addProductToCollection(collection_id):
    product_body = {
        "collect": {
            "product_id": 'FIXME-int of some product id',
            "collection_id": collection_id
        }
    }

    requests.post(f'{BASE_URL}/admin/api/2021-04/collects.json',
                  json=product_body,
                  headers=headers)


def createMetafield(i):
    x = requests.post(f'{BASE_URL}/admin/api/2021-04/metafields.json',
                      json={
                          "metafield": {
                              "namespace": "inventory",
                              "key": f"warehouse{i}",
                              "value": i,
                              "value_type": "integer"
                          }
                      },
                      headers=headers)

def makeProduct(i):
    """
    You can create a product in the test store and capture the POST it makes in the DevTools' Network tab.
    """

    headers = {
        'authority': 'FIXME',
        'pragma': 'FIXME',
        'cache-control': 'FIXME',
        'sec-ch-ua': 'FIXME',
        'accept': 'FIXME',
        'content-type': 'FIXME',
        'x-csrf-token': 'FIXME',
        'sec-ch-ua-mobile': 'FIXME',
        'user-agent': 'FIXME',
        'x-shopify-web-force-proxy': 'FIXME',
        'origin': 'FIXME',
        'sec-fetch-site': 'FIXME',
        'sec-fetch-mode': 'FIXME',
        'sec-fetch-dest': 'FIXME',
        'accept-language': 'FIXME',
        'cookie': 'FIXME',
    }

    title_base = 'FIXME'
    title = title_base + '_' + str(i)

    body = '{"operationName":"CreateProduct","variables":{"media":null,"product":{"title":"' + title + '","descriptionHtml":"","handle":"","seo":{"title":"","description":""},"status":"DRAFT","options":[],"variants":[{"compareAtPrice":null,"price":"1.00","taxable":true,"inventoryItem":{"cost":null,"countryCodeOfOrigin":null,"provinceCodeOfOrigin":null,"harmonizedSystemCode":null,"tracked":true},"requiresShipping":true,"weight":0,"weightUnit":"POUNDS","fulfillmentServiceId":"gid://shopify/FulfillmentService/manual","sku":"","barcode":"","inventoryPolicy":"DENY","showUnitPrice":false,"unitPriceMeasurement":{"quantityValue":0,"quantityUnit":null,"referenceValue":0,"referenceUnit":null},"taxCode":null,"inventoryQuantities":[{"availableQuantity":20,"locationId":"gid://shopify/Location/62030610598"}]}],"images":[],"productType":"","tags":[],"templateSuffix":"","giftCardTemplateSuffix":"","vendor":"","giftCard":false,"collectionsToJoin":[],"workflow":"product-details-create","metafields":[]}},"query":"mutation CreateProduct($product: ProductInput\u0021, $media: [CreateMediaInput\u0021]) {\\n  productCreate(input: $product, media: $media) {\\n    product {\\n      id\\n      title\\n      handle\\n      descriptionHtml\\n      resourceAlerts {\\n        content\\n        dismissed\\n        dismissibleHandle\\n        severity\\n        title\\n        actions {\\n          primary\\n          title\\n          url\\n          __typename\\n        }\\n        __typename\\n      }\\n      firstVariant: variants(first: 1) {\\n        edges {\\n          node {\\n            id\\n            requiresShipping\\n            weight\\n            weightUnit\\n            barcode\\n            sku\\n            inventoryPolicy\\n            fulfillmentService {\\n              id\\n              __typename\\n            }\\n            inventoryItem {\\n              id\\n              unitCost {\\n                amount\\n                __typename\\n              }\\n              countryCodeOfOrigin\\n              provinceCodeOfOrigin\\n              harmonizedSystemCode\\n              tracked\\n              __typename\\n            }\\n            ...PricingCardVariant\\n            __typename\\n          }\\n          __typename\\n        }\\n        __typename\\n      }\\n      ...SEOCardProduct\\n      __typename\\n    }\\n    userErrors {\\n      field\\n      message\\n      __typename\\n    }\\n    __typename\\n  }\\n}\\n\\nfragment PricingCardVariant on ProductVariant {\\n  id\\n  price\\n  compareAtPrice\\n  taxable\\n  taxCode\\n  presentmentPrices(first: 2) {\\n    edges {\\n      node {\\n        price {\\n          amount\\n          __typename\\n        }\\n        __typename\\n      }\\n      __typename\\n    }\\n    __typename\\n  }\\n  showUnitPrice\\n  unitPriceMeasurement {\\n    quantityValue\\n    quantityUnit\\n    referenceValue\\n    referenceUnit\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment SEOCardProduct on Product {\\n  seo {\\n    title\\n    description\\n    __typename\\n  }\\n  __typename\\n}\\n"}'

    return requests.post('https://talenddatawearhouse.myshopify.com/admin/internal/web/graphql/core?operation=CreateProduct',
                         data=body,
                         headers=headers)

#################
# Example Usage #
#################
# for i in range(250):
#     print("Making Collection {}".format(i))
#     x = makeCollection(i)
#     collection_id = int(x.json()['data']['collectionCreate']['collection']['id'].split('Collection/')[-1])
#     addProductToCollection(collection_id)

# for i in range(250):
#     print(f"Creating metafield {i}")
#     createMetafield(i)
