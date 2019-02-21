# Version 1

| Issue | Status |
| ----- | ------ |
| #88   | Draft  |

## Data Models

See [Data Models](cart-api-models.md).

## Web API Specification

Please note that:
1. All endpoints are protected by **a bearer token** ([RFC 6750](https://tools.ietf.org/html/rfc6750)).
  * For the initial release,
    * the bearer token can be anything that we use to identify a user *until the web service integrates with the auth service (`https://auth.{DEPLOYMENT_STAGE}.data.humancellatlas.org`, [docs](https://allspark.dev.data.humancellatlas.org/dcp-ops/docs/wikis/Security/Authentication%20and%20Authorization/Setting%20up%20DCP%20Auth))*, and
    * the client will generate a version 4 UUID string as dubbed as session ID (and the backend will treat the session ID as user ID) and the base64-encoded string of that UUID will be a bearer token.

2. All CRUD endpoints take JSON request body on `POST` and `PUT` requests.

#### HTTP Responses

| HTTP Status Code | Condition |
| --- | --- |
| 200 | Everything is ok. |
| 400 | Invalid input parameters |
| 401 | The bearer token is invalid or not present. |
| 404 | Resource (Cart) not found |

API endpoints with specifications can be found in [cart-api-endpoint.md](cart-api-endpoints.md)

#### CRUD APIs for `Cart`

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/resources/carts/` | Create a new (non-default) cart |
| `GET` | `/resources/carts/` | Retrieve a list of carts |
| `GET` | `/resources/carts/{id}` | Retrieve a single cart |
| `PUT` | `/resources/carts/{id}` | Update the cart |
| `DELETE` | `/resources/carts/{id}` | Delete a single cart |

#### CRUD APIs for `CartItem`

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/resources/carts/{cart_id}/items` | Add an item to a cart |
| `GET` | `/resources/carts/{cart_id}/items` | Retrieve all items in the given cart |
| `DELETE` | `/resources/carts/{cart_id}/items/{item_id}` | Delete an item from the cart |
| `POST` | `/resources/carts/{cart_id}/items/batch` | Add all items matching the given filters to a cart |

## Default Cart - *TODO*

The default cart is a special case of a cart where the `cart_id` is `default`.

When the default cart is retrieved, if a cart with id `default` belonging to the user does not exist, 
a cart is created.

Pseudo-code for cart retrieval:
```python
def get_cart(cart_id, user_id):
    cart_item_manager = CartItemManager()
    cart = cart_item_manager.get_cart(user_id, cart_id)
    if cart is None:
        if cart_id == 'default':
            cart = cart_item_manager.create_cart(user_id, name='Default Cart', default=True)
        else:
            raise NotFoundException
    return cart
```

A default cart can only be created by the `get_cart` function.

A default cart can be deleted and a new empty cart will be created when it is retrieved afterwards.

## Export to DSS Collection - *TODO*

Carts can be exported to the DSS Collection via the `POST /resources/carts/{cart_id}/export` endpoint.
This endpoint will make use of `PUT /collections` in the DSS API corresponding to the matching environment 
(dev, staging, integration, prod).

The collection will look like:
```json
{
    "contents": [
        {
          "type": "files",
          "uuid": "ec4b742d-816b-4029-8194-418f714cd05d"
        },
        ...
    ],
    "description": "Exported cart f0734d85-f098-488f-b2cb-6e59dc20a65a",
    "details": {},
    "name": "Cart Name"
}
```

When a cart is exported, an attribute `CollectionId` is added to the cart in DynamoDB.
This attribute is the UUID of the latest exported collection of the cart.

If a cart is exported again, it will create a new collection and the `CollectionId` will be updated.

## Batch cart item write

The batch cart item write is executed by a step function state machine that is triggered by the 
`POST /resources/carts/{cart_id}/items/batch` endpoint.  This is to avoid both the 30 second limit
imposed by API Gateway and the 15 minute limit of a single Lambda execution.  This implementation
also allows for a simple method to monitor the status of ongoing writes (can check the status of an execution).

Filters and an entity type are given to `POST /resources/carts/{cart_id}/items/batch`.
The endpoint will start a state machine execution and immediately return the number
of items that will be written and a URL to send a GET request to in order to check the
status of the write.

The status check endpoint is `GET /resources/carts/status/{token}`.
`token` is a base64-encoded JSON containing the ID of the state machine execution.

If the write is still ongoing, the endpoint will return a flag indicating "not done" and
a URL at which the status can be rechecked.  e.g.:
```
{
    "done": false,
    "statusUrl": "https://status.url/resources/carts/status/{token}"
}
```

If the write is finished (or errored), the endpoint will return a flag indicating "done" 
and a flag indicating if the write was successful.  e.g.: 
```
{
    "done": true,
    "success": true
}
```  

#### State machine

The step function state machine consists of the states

State machine visualization:

![State machine visualization](state_machine.png)

The input of the state machine is:

```json
{
    "filters" : { ... },
    "entity_type" : str,
    "cart_id" : str,
    "item_count" : int,
    "batch_size" : int
}
```

The `WriteBatch` state will write up to `batch_size` items to DynamoDB and output the input, 
as well as pagination information (`search_after`) for Elasticsearch and the number of items 
that were written.

The `NextBatch` state will check the number of items that were written in the previous state.
- If the number is 0, that means all items have been written and the state will transition to
the `SuccessState`.
- If the number is not 0, then the state will transition to `WriteBatch` to continue writing
items starting at the given `search_after` position.

The `SuccessState` will end the execution with a success.

#### Limitations:

Because this implementation uses Elasticsearch search after for retrieving each batch, 
if the underlying data is changed in the middle of a write job (e.g. matching entities 
are added or deleted), then the resulting written cart items may not exactly match
the query results the user sees.

## TODO

- Default cart behaviour is not implemented
- Only one bundle is associated with each cart item but a project or specimen may have multiple bundles
- Export to DSS Collection API
- Authentication and remove IP whitelist
