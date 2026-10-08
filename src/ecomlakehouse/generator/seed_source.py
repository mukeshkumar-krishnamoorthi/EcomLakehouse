from datetime import datetime, timedelta
import random

from faker import Faker
from sqlalchemy import create_engine, text

fake = Faker("en_IN")

DATABASE_URL = (
    "postgresql+psycopg://"
    "ecom_user:ecom_password@localhost:5441/ecommerce"
)

engine = create_engine(DATABASE_URL)

random.seed(42)
Faker.seed(42)


def seed_categories(connection):
    categories = [
        "Electronics",
        "Mobiles",
        "Laptops",
        "Fashion",
        "Footwear",
        "Home & Kitchen",
        "Beauty",
        "Sports",
        "Books",
        "Accessories",
    ]

    connection.execute(
        text(
            """
            INSERT INTO categories (category_name)
            VALUES (:category_name)
            """
        ),
        [{"category_name": category} for category in categories],
    )


def seed_stores(connection):
    stores = [
        ("Chennai Central", "Chennai", "Tamil Nadu"),
        ("Bangalore Central", "Bangalore", "Karnataka"),
        ("Hyderabad Central", "Hyderabad", "Telangana"),
        ("Mumbai Central", "Mumbai", "Maharashtra"),
        ("Delhi Central", "Delhi", "Delhi"),
        ("Pune Central", "Pune", "Maharashtra"),
        ("Coimbatore Central", "Coimbatore", "Tamil Nadu"),
        ("Kochi Central", "Kochi", "Kerala"),
        ("Mysore Central", "Mysore", "Karnataka"),
        ("Madurai Central", "Madurai", "Tamil Nadu"),
    ]

    connection.execute(
        text(
            """
            INSERT INTO stores
                (store_name, city, state)
            VALUES
                (:store_name, :city, :state)
            """
        ),
        [
            {
                "store_name": store_name,
                "city": city,
                "state": state,
            }
            for store_name, city, state in stores
        ],
    )


def seed_payment_methods(connection):
    methods = [
        "UPI",
        "Credit Card",
        "Debit Card",
        "Net Banking",
        "Cash on Delivery",
    ]

    connection.execute(
        text(
            """
            INSERT INTO payment_methods (method_name)
            VALUES (:method_name)
            """
        ),
        [{"method_name": method} for method in methods],
    )


def seed_customers(connection, count=1000):
    rows = []

    for _ in range(count):
        rows.append(
            {
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.unique.email(),
                "phone": fake.phone_number(),
                "city": fake.city(),
                "state": fake.state(),
                "country": "India",
            }
        )

    connection.execute(
        text(
            """
            INSERT INTO customers
                (
                    first_name,
                    last_name,
                    email,
                    phone,
                    city,
                    state,
                    country
                )
            VALUES
                (
                    :first_name,
                    :last_name,
                    :email,
                    :phone,
                    :city,
                    :state,
                    :country
                )
            """
        ),
        rows,
    )


def seed_products(connection, count=100):
    category_ids = connection.execute(
        text("SELECT category_id FROM categories")
    ).scalars().all()

    rows = []

    for i in range(1, count + 1):
        rows.append(
            {
                "category_id": random.choice(category_ids),
                "product_name": fake.catch_phrase(),
                "sku": f"SKU-{i:05d}",
                "price": round(random.uniform(100, 100000), 2),
                "stock_quantity": random.randint(0, 500),
                "is_active": True,
            }
        )

    connection.execute(
        text(
            """
            INSERT INTO products
                (
                    category_id,
                    product_name,
                    sku,
                    price,
                    stock_quantity,
                    is_active
                )
            VALUES
                (
                    :category_id,
                    :product_name,
                    :sku,
                    :price,
                    :stock_quantity,
                    :is_active
                )
            """
        ),
        rows,
    )


def seed_orders(connection, count=10000):
    customer_ids = connection.execute(
        text("SELECT customer_id FROM customers")
    ).scalars().all()

    store_ids = connection.execute(
        text("SELECT store_id FROM stores")
    ).scalars().all()

    product_rows = connection.execute(
        text(
            """
            SELECT product_id, price
            FROM products
            """
        )
    ).fetchall()

    product_map = {
        product_id: float(price)
        for product_id, price in product_rows
    }

    order_statuses = [
        "PLACED",
        "CONFIRMED",
        "SHIPPED",
        "DELIVERED",
        "CANCELLED",
    ]

    for _ in range(count):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)

        item_count = random.randint(1, 4)

        selected_products = random.sample(
            list(product_map.keys()),
            item_count,
        )

        order_items = []

        total_amount = 0

        for product_id in selected_products:
            quantity = random.randint(1, 3)
            unit_price = product_map[product_id]

            total_amount += quantity * unit_price

            order_items.append(
                {
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                }
            )

        order_result = connection.execute(
            text(
                """
                INSERT INTO orders
                    (
                        customer_id,
                        store_id,
                        order_status,
                        order_date,
                        total_amount
                    )
                VALUES
                    (
                        :customer_id,
                        :store_id,
                        :order_status,
                        :order_date,
                        :total_amount
                    )
                RETURNING order_id
                """
            ),
            {
                "customer_id": customer_id,
                "store_id": store_id,
                "order_status": random.choice(order_statuses),
                "order_date": datetime.now()
                - timedelta(days=random.randint(0, 365)),
                "total_amount": round(total_amount, 2),
            },
        )

        order_id = order_result.scalar_one()

        for item in order_items:
            connection.execute(
                text(
                    """
                    INSERT INTO order_items
                        (
                            order_id,
                            product_id,
                            quantity,
                            unit_price
                        )
                    VALUES
                        (
                            :order_id,
                            :product_id,
                            :quantity,
                            :unit_price
                        )
                    """
                ),
                {
                    "order_id": order_id,
                    **item,
                },
            )

        payment_method_id = connection.execute(
            text(
                """
                SELECT payment_method_id
                FROM payment_methods
                ORDER BY RANDOM()
                LIMIT 1
                """
            )
        ).scalar_one()

        connection.execute(
            text(
                """
                INSERT INTO payments
                    (
                        order_id,
                        payment_method_id,
                        payment_status,
                        amount,
                        transaction_reference,
                        paid_at
                    )
                VALUES
                    (
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
                "payment_status": "SUCCESS",
                "amount": round(total_amount, 2),
                "transaction_reference": fake.uuid4(),
                "paid_at": datetime.now(),
            },
        )


def main():
    print("Connecting to PostgreSQL...")

    with engine.begin() as connection:
        print("Seeding categories...")
        seed_categories(connection)

        print("Seeding stores...")
        seed_stores(connection)

        print("Seeding payment methods...")
        seed_payment_methods(connection)

        print("Seeding customers...")
        seed_customers(connection)

        print("Seeding products...")
        seed_products(connection)

        print("Seeding orders, order items and payments...")
        seed_orders(connection)

    print("Source system seeded successfully.")


if __name__ == "__main__":
    main()
