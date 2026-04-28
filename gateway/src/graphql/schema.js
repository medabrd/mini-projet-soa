const gql = require('graphql-tag');

// Schema GraphQL : expose les memes operations que REST mais permet en plus
// de combiner les donnees des 3 services dans une seule requete grace aux
// resolvers field-level (Order.driver, Order.delivery, Delivery.order, etc.)
const typeDefs = gql`
  type OrderItem {
    product_name: String!
    quantity: Int!
    unit_price: Float!
  }

  type Order {
    id: ID!
    customer_id: String!
    customer_name: String!
    delivery_address: String!
    items: [OrderItem!]!
    total_amount: Float!
    status: String!
    assigned_driver_id: String!
    created_at: String!
    updated_at: String!

    # Joins cross-services
    driver: Driver
    delivery: Delivery
  }

  type Location {
    driver_id: String!
    latitude: Float!
    longitude: Float!
    speed_kmh: Float!
    heading_deg: Float!
    timestamp: String!
  }

  type Driver {
    id: ID!
    name: String!
    phone: String!
    vehicle_type: String!
    status: String!
    current_order_id: String!
    last_location: Location
    created_at: String!
    updated_at: String!

    # Joins cross-services
    current_order: Order
  }

  type DeliveryEvent {
    id: Int!
    delivery_id: String!
    event_type: String!
    event_data_json: String!
    created_at: String!
  }

  type Delivery {
    id: ID!
    order_id: String!
    customer_id: String!
    customer_name: String!
    delivery_address: String!
    driver_id: String!
    driver_name: String!
    status: String!
    created_at: String!
    updated_at: String!

    # Joins cross-services
    order: Order
    driver: Driver
    history: [DeliveryEvent!]!
  }

  type OrdersPage {
    orders: [Order!]!
    total: Int!
  }

  type DriversList {
    drivers: [Driver!]!
    total: Int!
  }

  type DeliveriesPage {
    deliveries: [Delivery!]!
    total: Int!
  }

  type Query {
    # Orders
    order(id: ID!): Order
    orders(customer_id: String, status: String, limit: Int, offset: Int): OrdersPage!

    # Drivers
    driver(id: ID!): Driver
    availableDrivers(limit: Int): DriversList!

    # Deliveries
    delivery(id: ID!): Delivery
    deliveries(status: String, driver_id: String, limit: Int, offset: Int): DeliveriesPage!
    deliveryByOrder(order_id: ID!): Delivery
  }

  input OrderItemInput {
    product_name: String!
    quantity: Int!
    unit_price: Float!
  }

  input CreateOrderInput {
    customer_id: String!
    customer_name: String!
    delivery_address: String!
    items: [OrderItemInput!]!
  }

  input RegisterDriverInput {
    name: String!
    phone: String
    vehicle_type: String
  }

  input UpdateLocationInput {
    driver_id: ID!
    latitude: Float!
    longitude: Float!
    speed_kmh: Float
    heading_deg: Float
  }

  type Mutation {
    createOrder(input: CreateOrderInput!): Order!
    cancelOrder(id: ID!, reason: String): Order!

    registerDriver(input: RegisterDriverInput!): Driver!
    updateDriverLocation(input: UpdateLocationInput!): Location!

    advanceDelivery(delivery_id: ID!, new_status: String!): Delivery!
  }
`;

module.exports = { typeDefs };
