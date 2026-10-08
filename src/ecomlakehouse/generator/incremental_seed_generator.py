from __future__ import annotations
import logging
import os
import random
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from faker import Faker
from sqlalchemy import create_engine, text


# LOGGING


logger = logging.getLogger(__name__)


# CONFIG


postgres_host = os.getenv("POSTGRES_HOST", "localhost")

postgres_port = os.getenv(
    "POSTGRES_PORT",
    "5441" if postgres_host == "localhost" else "5432",
)

postgres_user = os.getenv(
    "POSTGRES_USER",
    "ecom_user",
)

postgres_password = os.getenv(
    "POSTGRES_PASSWORD",
    "ecom_password",
)

postgres_db = os.getenv(
    "POSTGRES_DB",
    "ecommerce",
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql+psycopg://"
    f"{postgres_user}:{postgres_password}"
    f"@{postgres_host}:{postgres_port}/{postgres_db}",
)


NEW_CUSTOMERS_MIN = int(
    os.getenv("NEW_CUSTOMERS_MIN", "5")
)

NEW_CUSTOMERS_MAX = int(
    os.getenv("NEW_CUSTOMERS_MAX", "15")
)

NEW_ORDERS_MIN = int(
    os.getenv("NEW_ORDERS_MIN", "10")
)

NEW_ORDERS_MAX = int(
    os.getenv("NEW_ORDERS_MAX", "25")
)

CUSTOMER_UPDATES_MIN = int(
    os.getenv("CUSTOMER_UPDATES_MIN", "1")
)

CUSTOMER_UPDATES_MAX = int(
    os.getenv("CUSTOMER_UPDATES_MAX", "5")
)

ORDER_UPDATES_MIN = int(
    os.getenv("ORDER_UPDATES_MIN", "2")
)

ORDER_UPDATES_MAX = int(
    os.getenv("ORDER_UPDATES_MAX", "10")
)

MIN_PRODUCTS_PER_ORDER = 1
MAX_PRODUCTS_PER_ORDER = 5

MIN_QUANTITY = 1
MAX_QUANTITY = 4

ORDER_DISCOUNT_PROBABILITY = 0.30

PAYMENT_SUCCESS_PROBABILITY = 0.90


# FAKER


fake = Faker("en_IN")

# Deterministic randomness is useful while developing/testing.
random.seed(42)
Faker.seed(42)


# DATABASE


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)


# MONEY


def money(value: Decimal | float | int) -> Decimal:
    """
    Always keep monetary values at two decimal places.
    """

    return Decimal(str(value)).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )


# CUSTOMERS


def create_customers(
    connection,
    count: int,
) -> int:
    """
    Simulate new customers registering in the application.
    """

    inserted = 0

    for _ in range(count):

        now = datetime.now(timezone.utc)

        connection.execute(
            text(
                """
                INSERT INTO customers (
                    first_name,
                    last_name,
                    email,
                    phone,
                    created_at,
                    updated_at
                )
                VALUES (
                    :first_name,
                    :last_name,
                    :email,
                    :phone,
                    :created_at,
                    :updated_at
                )
                """
            ),
            {
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": f"customer_{uuid.uuid4().hex}@example.com",
                "phone": fake.phone_number(),
                "created_at": now,
                "updated_at": now,
            },
        )

        inserted += 1

    return inserted


# UPDATE CUSTOMERS


def update_customers(
    connection,
    count: int,
) -> int:
    """
    Simulate existing customers changing their information.
    """

    customer_ids = connection.execute(
        text(
            """
            SELECT customer_id
            FROM customers
            ORDER BY RANDOM()
            LIMIT :limit
            """
        ),
        {
            "limit": count,
        },
    ).scalars().all()

    updated = 0

    for customer_id in customer_ids:

        now = datetime.now(timezone.utc)

        result = connection.execute(
            text(
                """
                UPDATE customers
                SET
                    phone = :phone,
                    updated_at = :updated_at
                WHERE customer_id = :customer_id
                """
            ),
            {
                "phone": fake.phone_number(),
                "updated_at": now,
                "customer_id": customer_id,
            },
        )

        updated += result.rowcount

    return updated


# CUSTOMERS


def get_customer_ids(connection) -> list[int]:

    return list(
        connection.execute(
            text(
                """
                SELECT customer_id
                FROM customers
                ORDER BY customer_id
                """
            )
        ).scalars().all()
    )


# PRODUCTS


def get_products(connection) -> list[dict]:
    """
    Load the REAL product catalog.

    The generator never invents a product price.
    """

    rows = connection.execute(
        text(
            """
            SELECT
                product_id,
                price
            FROM products
            WHERE price > 0
            ORDER BY product_id
            """
        )
    ).mappings().all()

    return [
        {
            "product_id": row["product_id"],
            "price": money(row["price"]),
        }
        for row in rows
    ]


# SELECT PRODUCTS FOR ORDER


def select_products_for_order(
    products: list[dict],
) -> list[dict]:
    """
    Select multiple REAL products for an order.
    """

    if not products:
        raise RuntimeError(
            "Cannot create order because product catalog is empty."
        )

    product_count = random.randint(
        MIN_PRODUCTS_PER_ORDER,
        min(
            MAX_PRODUCTS_PER_ORDER,
            len(products),
        ),
    )

    selected_products = random.sample(
        products,
        product_count,
    )

    order_items = []

    for product in selected_products:

        quantity = random.randint(
            MIN_QUANTITY,
            MAX_QUANTITY,
        )

        order_items.append(
            {
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": product["price"],
            }
        )

    return order_items


# CREATE ORDER


def create_order(
    connection,
    customer_id: int,
    order_items: list[dict],
) -> dict:
    """
    Create a realistic order.

    The final amount is calculated from actual order items.
    """

    now = datetime.now(timezone.utc)

    # --------------------------------------------------------
    # Create empty order
    # --------------------------------------------------------

    result = connection.execute(
        text(
            """
            INSERT INTO orders (
                customer_id,
                order_status,
                total_amount,
                created_at,
                updated_at
            )
            VALUES (
                :customer_id,
                :order_status,
                0,
                :created_at,
                :updated_at
            )
            RETURNING order_id
            """
        ),
        {
            "customer_id": customer_id,
            "order_status": random.choice(
                [
                    "PENDING",
                    "PLACED",
                    "CONFIRMED",
                    "SHIPPED",
                    "DELIVERED",
                    "CANCELLED",
                ]
            ),
            "created_at": now,
            "updated_at": now,
        },
    )

    order_id = result.scalar_one()

    # --------------------------------------------------------
    # Create order items
    # --------------------------------------------------------

    subtotal = Decimal("0.00")

    for item in order_items:

        quantity = item["quantity"]

        unit_price = money(
            item["unit_price"]
        )

        line_total = money(
            unit_price * quantity
        )

        connection.execute(
            text(
                """
                INSERT INTO order_items (
                    order_id,
                    product_id,
                    quantity,
                    unit_price,
                    created_at,
                    updated_at
                )
                VALUES (
                    :order_id,
                    :product_id,
                    :quantity,
                    :unit_price,
                    :created_at,
                    :updated_at
                )
                """
            ),
            {
                "order_id": order_id,
                "product_id": item["product_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "created_at": now,
                "updated_at": now,
            },
        )

        subtotal += line_total

    # --------------------------------------------------------
    # Discount
    # --------------------------------------------------------

    discount = Decimal("0.00")

    if random.random() < ORDER_DISCOUNT_PROBABILITY:

        discount_percentage = random.choice(
            [
                Decimal("0.05"),
                Decimal("0.10"),
                Decimal("0.15"),
                Decimal("0.20"),
            ]
        )

        discount = money(
            subtotal * discount_percentage
        )

    # --------------------------------------------------------
    # Tax
    # --------------------------------------------------------

    taxable_amount = subtotal - discount

    tax_rate = Decimal("0.18")

    tax = money(
        taxable_amount * tax_rate
    )

    # --------------------------------------------------------
    # Shipping
    # --------------------------------------------------------

    if taxable_amount >= Decimal("1000"):
        shipping = Decimal("0.00")
    else:
        shipping = Decimal("99.00")

    # --------------------------------------------------------
    # Final order total
    # --------------------------------------------------------

    total = money(
        taxable_amount
        + tax
        + shipping
    )

    # --------------------------------------------------------
    # Update order
    # --------------------------------------------------------

    connection.execute(
        text(
            """
            UPDATE orders
            SET
                total_amount = :total_amount,
                updated_at = :updated_at
            WHERE order_id = :order_id
            """
        ),
        {
            "total_amount": total,
            "updated_at": datetime.now(timezone.utc),
            "order_id": order_id,
        },
    )

    return {
        "order_id": order_id,
        "subtotal": subtotal,
        "discount": discount,
        "tax": tax,
        "shipping": shipping,
        "total": total,
        "item_count": len(order_items),
    }


# PAYMENT METHOD


def get_payment_method_id(connection):

    return connection.execute(
        text(
            """
            SELECT payment_method_id
            FROM payment_methods
            ORDER BY RANDOM()
            LIMIT 1
            """
        )
    ).scalar_one_or_none()


# CREATE PAYMENT


def create_payment(
    connection,
    order_id: int,
    amount: Decimal,
) -> str:
    """
    Create a payment using the ACTUAL order total.
    """

    payment_method_id = get_payment_method_id(
        connection
    )

    if payment_method_id is None:
        raise RuntimeError(
            "No payment methods exist."
        )

    payment_succeeded = (
        random.random()
        < PAYMENT_SUCCESS_PROBABILITY
    )

    if payment_succeeded:

        payment_status = "SUCCESS"
        paid_at = datetime.now(timezone.utc)

    else:

        payment_status = "FAILED"
        paid_at = None

    connection.execute(
        text(
            """
            INSERT INTO payments (
                order_id,
                payment_method_id,
                payment_status,
                amount,
                transaction_reference,
                paid_at
            )
            VALUES (
                :order_id,
                :payment_method_id,
                :payment_status,
                :amount,
                :transaction_reference,
                :paid_at
            )
            """
        ),
        {
            "order_id": order_id,
            "payment_method_id": payment_method_id,
            "payment_status": payment_status,
            "amount": amount,
            "transaction_reference": str(
                uuid.uuid4()
            ),
            "paid_at": paid_at,
        },
    )

    return payment_status


# ORDER STATUS TRANSITIONS

ORDER_STATUS_TRANSITIONS = {
    "PENDING": [
        "CONFIRMED",
        "CANCELLED",
    ],

    "CONFIRMED": [
        "PROCESSING",
        "CANCELLED",
    ],

    "PROCESSING": [
        "SHIPPED",
    ],

    "SHIPPED": [
        "DELIVERED",
    ],
}


# UPDATE ORDERS


def update_orders(
    connection,
    count: int,
) -> int:
    """
    Simulate real order lifecycle changes.
    """

    orders = connection.execute(
        text(
            """
            SELECT
                order_id,
                order_status
            FROM orders
            WHERE order_status NOT IN (
                'DELIVERED',
                'CANCELLED'
            )
            ORDER BY RANDOM()
            LIMIT :limit
            """
        ),
        {
            "limit": count,
        },
    ).mappings().all()

    updated = 0

    for order in orders:

        current_status = order["order_status"]

        possible_statuses = (
            ORDER_STATUS_TRANSITIONS.get(
                current_status,
                [],
            )
        )

        if not possible_statuses:
            continue

        new_status = random.choice(
            possible_statuses
        )

        now = datetime.now(timezone.utc)

        result = connection.execute(
            text(
                """
                UPDATE orders
                SET
                    order_status = :order_status,
                    order_date = :order_date
                WHERE order_id = :order_id
                """
            ),
            {
                "order_status": new_status,
                "order_date": now,
                "order_id": order["order_id"],
            },
        )

        updated += result.rowcount

    return updated


# MAIN


def run_incremental_seed_generation() -> None:
    """
    Generate one batch of realistic source-system activity.

    This function knows NOTHING about Airflow.

    Airflow will call this function.
    """

    logger.info(
        "Incremental seed generation started"
    )

    with engine.begin() as connection:

        # 1. NEW CUSTOMERS
        customer_count = random.randint(
            NEW_CUSTOMERS_MIN,
            NEW_CUSTOMERS_MAX,
        )

        logger.info(
            "\nGenerating new customers | requested=%d",
            customer_count,
        )

        customers_inserted = create_customers(
            connection,
            customer_count,
        )

        logger.info(
            "Customers inserted | count=%d",
            customers_inserted,
        )

        # 2. CUSTOMER UPDATES
        customer_update_count = random.randint(
            CUSTOMER_UPDATES_MIN,
            CUSTOMER_UPDATES_MAX,
        )

        logger.info(
            "\nUpdating existing customers | requested=%d",
            customer_update_count,
        )

        customers_updated = update_customers(
            connection,
            customer_update_count,
        )

        logger.info(
            "Customers updated | count=%d",
            customers_updated,
        )

        # 3. LOAD EXISTING CUSTOMERS
        customer_ids = get_customer_ids(
            connection
        )

        logger.info(
            "\nLoaded existing customers | count=%d",
            len(customer_ids),
        )

        if not customer_ids:
            raise RuntimeError(
                "No customers exist. "
                "Seed customers before generating orders."
            )

        # 4. LOAD ACTUAL PRODUCT CATALOG
        products = get_products(
            connection
        )

        logger.info(
            "\nLoaded product catalog | count=%d",
            len(products),
        )

        if not products:
            raise RuntimeError(
                "No products exist. "
                "Seed products before generating orders."
            )

        # 5. CREATE NEW ORDERS
        order_count = random.randint(
            NEW_ORDERS_MIN,
            NEW_ORDERS_MAX,
        )

        logger.info(
            "\nGenerating new orders | requested=%d",
            order_count,
        )

        orders_inserted = 0
        order_items_inserted = 0

        successful_payments = 0
        failed_payments = 0

        generated_orders = []

        for _ in range(order_count):

            # Select existing customer
            customer_id = random.choice(
                customer_ids
            )

            # Select actual products
            order_items = select_products_for_order(
                products
            )

            # Create order
            order = create_order(
                connection,
                customer_id,
                order_items,
            )

            orders_inserted += 1

            order_items_inserted += len(
                order_items
            )

            generated_orders.append(order)

            # Create payment
            payment_status = create_payment(
                connection,
                order["order_id"],
                order["total"],
            )

            if payment_status == "SUCCESS":
                successful_payments += 1
            else:
                failed_payments += 1

        logger.info(
            "Orders generated | orders=%d | order_items=%d",
            orders_inserted,
            order_items_inserted,
        )

        logger.info(
            "Payments generated | successful=%d | failed=%d",
            successful_payments,
            failed_payments,
        )

        # 6. UPDATE EXISTING ORDERS
        order_update_count = random.randint(
            ORDER_UPDATES_MIN,
            ORDER_UPDATES_MAX,
        )

        logger.info(
            "\nUpdating existing orders | requested=%d",
            order_update_count,
        )

        orders_updated = update_orders(
            connection,
            order_update_count,
        )

        logger.info(
            "Orders updated | count=%d",
            orders_updated,
        )

        # 7. SUMMARY
        logger.info("\n")
        logger.info("=" * 60)
        logger.info("ECOMMERCE INCREMENTAL SEED SUMMARY")
        logger.info("=" * 60)

        logger.info(
            "Customers inserted   : %d",
            customers_inserted,
        )

        logger.info(
            "Customers updated    : %d",
            customers_updated,
        )

        logger.info(
            "Orders inserted      : %d",
            orders_inserted,
        )

        logger.info(
            "Orders updated       : %d",
            orders_updated,
        )

        logger.info(
            "Order items inserted : %d",
            order_items_inserted,
        )

        logger.info(
            "Payments successful  : %d",
            successful_payments,
        )

        logger.info(
            "Payments failed      : %d",
            failed_payments,
        )

        logger.info("=" * 60)

        # 8. SAMPLE ORDERS
        logger.info(
            "\nGenerated order samples | count=%d",
            min(5, len(generated_orders)),
        )

        for order in generated_orders[:5]:

            logger.info(
                "Order generated | "
                "order_id=%s | "
                "items=%d | "
                "subtotal=%s | "
                "discount=%s | "
                "tax=%s | "
                "shipping=%s | "
                "total=%s",
                order["order_id"],
                order["item_count"],
                order["subtotal"],
                order["discount"],
                order["tax"],
                order["shipping"],
                order["total"],
            )

    logger.info(
        "\nIncremental seed generation completed successfully"
    )


# ENTRY POINT
if __name__ == "__main__":
    run_incremental_seed_generation()
