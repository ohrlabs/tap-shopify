from singer import metrics

from tap_shopify.context import Context
from tap_shopify.streams.base import Stream

class Locations(Stream):
    """Stream class for Shopify Locations"""

    name = "locations"
    data_key = "locations"
    # Currently, the replication key is set to 'createdAt' because the Shopify
    # locations graphql endpoint doesn't allow the filter on 'updatedAt' field.
    # It's only nominal: every sync fetches every location (see get_objects).
    replication_key = "createdAt"

    def get_query(self):
        """
        Returns the GraphQL query for fetching locations.

        Returns:
            str: GraphQL query string.
        """
        return """
        query GetLocations($first: Int!, $after: String) {
            # includeLegacy adds the locations fulfillment service apps (3PLs)
            # own; includeInactive, deactivated ones. Both default to false.
            locations(first: $first, after: $after, sortKey: ID,
                      includeLegacy: true, includeInactive: true) {
                edges {
                    node {
                        address {
                            countryCode
                            address1
                            city
                            address2
                            provinceCode
                            zip
                            province
                            phone
                            country
                            formatted
                            latitude
                            longitude
                        }
                        name
                        id
                        updatedAt
                        createdAt
                        isActive
                        addressVerified
                        deactivatable
                        deactivatedAt
                        deletable
                        fulfillsOnlineOrders
                        hasActiveInventory
                        hasUnfulfilledOrders
                        isFulfillmentService
                        legacyResourceId
                        localPickupSettingsV2 {
                            instructions
                            pickupTime
                        }
                        shipsInventory
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                }
            }
        }
        """

    def get_objects(self):
        """
        Yields every location, on every sync. A shop has few, and their
        updatedAt can't be filtered on, so filtering by createdAt (as the base
        class would) would never pick up a later change, such as a rename or
        deactivation.
        """
        query = self.remove_fields_from_query(Context.get_unselected_fields(self.name))
        has_next_page, cursor = True, None
        while has_next_page:
            params = {"first": self.results_per_page}
            if cursor:
                params["after"] = cursor
            with metrics.http_request_timer(self.name):
                data = self.call_api(params, query=query)
            for edge in data.get("edges"):
                yield self.transform_object(edge.get("node"))
            page_info = data.get("pageInfo")
            cursor, has_next_page = page_info.get("endCursor"), page_info.get("hasNextPage")


Context.stream_objects["locations"] = Locations
