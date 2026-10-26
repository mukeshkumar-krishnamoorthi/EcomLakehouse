import random
from datetime import datetime, timedelta

from faker import Faker
from sqlalchemy import create_engine, text

from ecomlakehouse.generator.flipkart_catalog import (
    generate_flipkart_product,
    generate_indian_customer,
    generate_payment_reference,
)

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
            ON CONFLICT (category_name) DO NOTHING
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
            INSERT INTO stores (store_name, city, state)
            VALUES (:store_name, :city, :state)
            ON CONFLICT (store_name) DO NOTHING
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
            ON CONFLICT (method_name) DO NOTHING
            """
        ),
        [{"method_name": method} for method in methods],
    )


def seed_customers(connection, count=1000):
    rows = []

    for _ in range(count):
        cust = generate_indian_customer()
        rows.append(
            {
                "first_name": cust["first_name"],
                "last_name": cust["last_name"],
                "email": cust["email"],
                "phone": cust["phone"],
                "city": cust["city"],
                "state": cust["state"],
                "country": cust["country"],
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
    categories = connection.execute(
        text("SELECT category_id, category_name FROM categories")
    ).all()

    max_product_id = connection.execute(
        text("SELECT COALESCE(MAX(product_id), 0) FROM products")
    ).scalar() or 0

    rows = []

    for i in range(1, count + 1):
        cat_id, cat_name = random.choice(categories)
        sku_num = max_product_id + i
        item = generate_flipkart_product(cat_name, sku_num)
        rows.append(
            {
                "category_id": cat_id,
                "product_name": item["product_name"],
                "sku": item["sku"],
                "price": item["price"],
                "stock_quantity": item["stock_quantity"],
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

    pm_list = connection.execute(
        text("SELECT payment_method_id, method_name FROM payment_methods")
    ).fetchall()

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

        order_status = random.choice(order_statuses)
        order_date = datetime.now() - timedelta(days=random.randint(0, 365))

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
                "order_status": order_status,
                "order_date": order_date,
                "total_amount": round(total_amount, 2),
            },
        )

        order_id = order_result.scalar_one()

        initial_item_status = (
            "DELIVERED"
            if order_status == "DELIVERED"
            else ("CANCELLED" if order_status == "CANCELLED" else "ORDERED")
        )

        for item in order_items:
            connection.execute(
                text(
                    """
                    INSERT INTO order_items
                        (
                            order_id,
                            product_id,
                            quantity,
                            unit_price,
                            item_status
                        )
                    VALUES
                        (
                            :order_id,
                            :product_id,
                            :quantity,
                            :unit_price,
                            :item_status
                        )
                    """
                ),
                {
                    "order_id": order_id,
                    "item_status": initial_item_status,
                    **item,
                },
            )

        pm_id, pm_name = random.choice(pm_list)

        if order_status == "CANCELLED":
            pay_status = "FAILED"
            paid_at = None
        elif pm_name == "Cash on Delivery":
            pay_status = "SUCCESS" if order_status == "DELIVERED" else "PENDING"
            paid_at = order_date if order_status == "DELIVERED" else None
        else:
            pay_status = "SUCCESS"
            paid_at = order_date

        payment_ref = generate_payment_reference(pm_name, order_id)

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
                "payment_method_id": pm_id,
                "payment_status": pay_status,
                "amount": round(total_amount, 2),
                "transaction_reference": payment_ref,
                "paid_at": paid_at,
            },
        )


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Bootstrap or reset the PostgreSQL source database"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Truncate all tables and re-seed from scratch",
    )
    args = parser.parse_args()

    print("Connecting to PostgreSQL...")

    with engine.begin() as connection:
        if args.reset:
            print("Reset flag detected. Truncating all tables...")
            connection.execute(
                text(
                    "TRUNCATE categories, stores, payment_methods, customers, "
                    "products, orders, order_items, payments RESTART IDENTITY CASCADE;"
                )
            )
            print("Tables truncated successfully.")

        print("Seeding categories...")
        seed_categories(connection)

        print("Seeding stores...")
        seed_stores(connection)

        print("Seeding payment methods...")
        seed_payment_methods(connection)

        existing_customers = connection.execute(
            text("SELECT COUNT(*) FROM customers")
        ).scalar() or 0

        if existing_customers == 0 or args.reset:
            print("Seeding customers...")
            seed_customers(connection)

            print("Seeding products...")
            seed_products(connection)

            print("Seeding orders, order items and payments...")
            seed_orders(connection)
        else:
            print(
                f"Database already contains {existing_customers:,} customers. "
                "Master data verified. Use --reset to truncate and re-seed from scratch."
            )

    print("Source system seeded successfully.")


if __name__ == "__main__":
    main()
