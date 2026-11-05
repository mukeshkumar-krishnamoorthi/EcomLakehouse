"""
Incremental Live Activity Seed Generator.

Simulates real-time e-commerce user activity like Flipkart:
- Generates new products each incremental batch
- Generates 200 to 300+ authentic Indian customer registrations
- Generates minimum 1,000 realistic orders with realistic cart items and Indian pricing
- Generates 1,000 accurately linked payments (UPI, Cards, Net Banking, COD)
- Simulates realistic order lifecycle transitions (PLACED -> CONFIRMED -> SHIPPED -> DELIVERED)
- Updates customer details and product inventories
- Runs via Airflow DAG (every minute) or standalone continuous streaming simulator
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import MetaData, create_engine, select, text

from ecomlakehouse.generator.flipkart_catalog import (
    generate_flipkart_product,
    generate_indian_customer,
    generate_payment_reference,
    money,
)


def with_deadlock_retry(func, max_attempts=5, initial_delay=0.3):
    """Execute a database function with exponential backoff if a deadlock or lock timeout occurs."""
    for attempt in range(1, max_attempts + 1):
        try:
            return func()
        except Exception as exc:
            err_msg = str(exc).lower()
            if "deadlock" in err_msg or "lock wait timeout" in err_msg or "sharelock" in err_msg:
                if attempt < max_attempts:
                    sleep_sec = initial_delay * (2 ** (attempt - 1)) + random.uniform(0.1, 0.4)
                    logger.warning(
                        "Deadlock/lock conflict detected on attempt %d/%d; retrying in %.2fs...",
                        attempt,
                        max_attempts,
                        sleep_sec,
                    )
                    time.sleep(sleep_sec)
                    continue
            raise

# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ecomlakehouse.generator")


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

postgres_host = os.getenv("POSTGRES_HOST", "localhost")
postgres_port = os.getenv(
    "POSTGRES_PORT",
    "5441" if postgres_host == "localhost" else "5432",
)
postgres_user = os.getenv("POSTGRES_USER", "ecom_user")
postgres_password = os.getenv("POSTGRES_PASSWORD", "ecom_password")
postgres_db = os.getenv("POSTGRES_DB", "ecommerce")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql+psycopg://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}",
)

# Incremental generation batch volumes
NEW_PRODUCTS_MIN = int(os.getenv("NEW_PRODUCTS_MIN", "15"))
NEW_PRODUCTS_MAX = int(os.getenv("NEW_PRODUCTS_MAX", "30"))

NEW_CUSTOMERS_MIN = int(os.getenv("NEW_CUSTOMERS_MIN", "200"))
NEW_CUSTOMERS_MAX = int(os.getenv("NEW_CUSTOMERS_MAX", "300"))

NEW_ORDERS_MIN = int(os.getenv("NEW_ORDERS_MIN", "1000"))
NEW_ORDERS_MAX = int(os.getenv("NEW_ORDERS_MAX", "1200"))

CUSTOMER_UPDATES_MIN = int(os.getenv("CUSTOMER_UPDATES_MIN", "40"))
CUSTOMER_UPDATES_MAX = int(os.getenv("CUSTOMER_UPDATES_MAX", "80"))

ORDER_UPDATES_MIN = int(os.getenv("ORDER_UPDATES_MIN", "80"))
ORDER_UPDATES_MAX = int(os.getenv("ORDER_UPDATES_MAX", "150"))

PAYMENT_RETRIES_MIN = int(os.getenv("PAYMENT_RETRIES_MIN", "20"))
PAYMENT_RETRIES_MAX = int(os.getenv("PAYMENT_RETRIES_MAX", "50"))

ORDER_RETURNS_MIN = int(os.getenv("ORDER_RETURNS_MIN", "10"))
ORDER_RETURNS_MAX = int(os.getenv("ORDER_RETURNS_MAX", "30"))

PRODUCT_UPDATES_MIN = int(os.getenv("PRODUCT_UPDATES_MIN", "10"))
PRODUCT_UPDATES_MAX = int(os.getenv("PRODUCT_UPDATES_MAX", "25"))

MIN_PRODUCTS_PER_ORDER = 1
MAX_PRODUCTS_PER_ORDER = 4

ORDER_DISCOUNT_PROBABILITY = 0.35
TAX_RATE = Decimal("0.18")  # Standard Indian GST
FREE_SHIPPING_THRESHOLD = Decimal("500.00")  # Flipkart Plus threshold
STANDARD_SHIPPING_FEE = Decimal("40.00")

# Order lifecycle transitions
ORDER_STATUS_TRANSITIONS = {
    "PLACED": ["CONFIRMED", "CANCELLED"],
    "CONFIRMED": ["SHIPPED", "CANCELLED"],
    "SHIPPED": ["DELIVERED"],
}

# ---------------------------------------------------------------------------
# DATABASE INITIALIZATION
# ---------------------------------------------------------------------------

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

metadata = MetaData()
metadata.reflect(bind=engine)

customers_table = metadata.tables["customers"]
products_table = metadata.tables["products"]
orders_table = metadata.tables["orders"]
order_items_table = metadata.tables["order_items"]
payments_table = metadata.tables["payments"]
categories_table = metadata.tables["categories"]
stores_table = metadata.tables["stores"]
payment_methods_table = metadata.tables["payment_methods"]


# ---------------------------------------------------------------------------
# PRODUCT OPERATIONS
# ---------------------------------------------------------------------------

def create_incremental_products(connection, count: int) -> int:
    """
    Generate and insert authentic Flipkart products across available categories.
    """
    categories = connection.execute(
        select(categories_table.c.category_id, categories_table.c.category_name)
    ).all()

    if not categories:
        logger.warning("No categories found; skipping product generation")
        return 0

    max_product_id = connection.execute(
        select(text("COALESCE(MAX(product_id), 0) FROM products"))
    ).scalar() or 0

    now = datetime.now(UTC)
    product_rows = []

    for i in range(1, count + 1):
        cat_id, cat_name = random.choice(categories)
        sku_num = max_product_id + i
        item = generate_flipkart_product(cat_name, sku_num)
        price = item["price"]

        # Anomaly injection: ~2% zero or negative price (violates price > 0.0)
        if random.random() < 0.02:
            price = Decimal("0.00")

        product_rows.append({
            "category_id": cat_id,
            "product_name": item["product_name"],
            "sku": item["sku"],
            "price": price,
            "stock_quantity": item["stock_quantity"],
            "is_active": True,
            "created_at": now,
            "updated_at": now,
        })

    if product_rows:
        connection.execute(products_table.insert(), product_rows)

    return len(product_rows)


def update_products_inventory(connection, count: int) -> int:
    """
    Simulate supplier restocks and promotional price revisions on existing products.
    """
    products = connection.execute(
        text(
            "SELECT product_id, price, stock_quantity "
            "FROM products ORDER BY RANDOM() LIMIT :limit"
        ),
        {"limit": count},
    ).mappings().all()

    # Sort deterministically by product_id to prevent deadlocks across concurrent runs
    products = sorted(products, key=lambda p: p["product_id"])

    updated = 0
    now = datetime.now(UTC)

    for prod in products:
        # 70% chance stock replenishment, 30% chance price offer
        if random.random() < 0.70:
            new_stock = prod["stock_quantity"] + random.randint(20, 150)
            connection.execute(
                text(
                    "UPDATE products SET stock_quantity = :stock, updated_at = :now "
                    "WHERE product_id = :id"
                ),
                {"stock": new_stock, "now": now, "id": prod["product_id"]},
            )
        else:
            # 5% to 15% promotional discount or price adjustment
            discount_pct = Decimal(str(random.choice([0.90, 0.95, 1.05])))
            new_price = money(prod["price"] * discount_pct)
            connection.execute(
                text(
                    "UPDATE products SET price = :price, updated_at = :now "
                    "WHERE product_id = :id"
                ),
                {"price": new_price, "now": now, "id": prod["product_id"]},
            )
        updated += 1

    return updated


# ---------------------------------------------------------------------------
# CUSTOMER OPERATIONS
# ---------------------------------------------------------------------------

def create_incremental_customers(connection, count: int) -> list[int]:
    """
    Simulate new Indian customers registering on Flipkart.
    Returns the list of newly created customer IDs.
    """
    now = datetime.now(UTC)
    customer_rows = []

    for _ in range(count):
        cust = generate_indian_customer()
        email = cust["email"]

        # Anomaly injection: ~2% malformed email (violates email regex check)
        if random.random() < 0.02:
            email = f"user_{uuid.uuid4().hex[:6]}@bad_domain"

        customer_rows.append({
            "first_name": cust["first_name"],
            "last_name": cust["last_name"],
            "email": email,
            "phone": cust["phone"],
            "city": cust["city"],
            "state": cust["state"],
            "country": cust["country"],
            "created_at": now,
            "updated_at": now,
        })

    result = connection.scalars(
        customers_table.insert().returning(customers_table.c.customer_id),
        customer_rows,
    ).all()

    return list(result)


def update_customers_profile(connection, count: int) -> int:
    """
    Simulate existing customers updating contact numbers or moving cities.
    """
    customer_ids = connection.execute(
        text("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT :limit"),
        {"limit": count},
    ).scalars().all()

    # Sort deterministically to prevent cross-process deadlocks
    customer_ids = sorted(customer_ids)

    now = datetime.now(UTC)
    updated = 0

    for customer_id in customer_ids:
        new_cust = generate_indian_customer()
        result = connection.execute(
            text("""
                UPDATE customers
                SET phone = :phone,
                    city = :city,
                    state = :state,
                    updated_at = :now
                WHERE customer_id = :customer_id
            """),
            {
                "phone": new_cust["phone"],
                "city": new_cust["city"],
                "state": new_cust["state"],
                "now": now,
                "customer_id": customer_id,
            },
        )
        updated += result.rowcount

    return updated


# ---------------------------------------------------------------------------
# ORDER & PAYMENT OPERATIONS (BATCH HIGH-PERFORMANCE)
# ---------------------------------------------------------------------------

def generate_orders_and_payments(
    connection,
    order_count: int,
    existing_customer_ids: list[int],
    new_customer_ids: list[int],
    store_ids: list[int],
    products: list[dict],
    payment_methods: list[dict],
) -> dict:
    """
    Generate realistic orders, items, and linked payments in ultra-fast bulk operations.
    """
    if not products:
        raise RuntimeError("Product catalog is empty. Cannot generate orders.")
    if not store_ids:
        raise RuntimeError("No stores found in database.")
    if not payment_methods:
        raise RuntimeError("No payment methods found in database.")

    # Prepare payment method lookup & weights (Flipkart payment method distribution)
    # UPI (~55%), Credit Card (~18%), Debit Card (~12%), COD (~10%), Net Banking (~5%)
    method_weights = []
    for pm in payment_methods:
        name = pm["method_name"]
        if "UPI" in name:
            method_weights.append(55)
        elif "Credit" in name:
            method_weights.append(18)
        elif "Debit" in name:
            method_weights.append(12)
        elif "Cash" in name:
            method_weights.append(10)
        else:
            method_weights.append(5)

    now = datetime.now(UTC)

    # 1. Build all order definitions in memory
    order_insert_rows = []
    pre_order_items = []
    pre_payments = []

    stock_decrements: dict[int, int] = {}
    successful_payments = 0
    failed_payments = 0
    pending_payments = 0

    for _ in range(order_count):
        # 30% chance for newly registered user, 70% returning customer
        if new_customer_ids and random.random() < 0.30:
            customer_id = random.choice(new_customer_ids)
        else:
            customer_id = random.choice(existing_customer_ids)

        store_id = random.choice(store_ids)

        # Initial order status distribution
        status_rand = random.random()
        if status_rand < 0.65:
            initial_status = "PLACED"
        elif status_rand < 0.90:
            initial_status = "CONFIRMED"
        else:
            initial_status = "CANCELLED"

        # Select 1 to 4 distinct products
        item_count = random.choices([1, 2, 3, 4], weights=[60, 25, 10, 5], k=1)[0]
        selected_prods = random.sample(products, min(item_count, len(products)))

        subtotal = Decimal("0.00")
        items_for_this_order = []

        for p in selected_prods:
            qty = random.choices([1, 2, 3], weights=[80, 15, 5], k=1)[0]
            unit_price = p["price"]
            item_status = "ORDERED"

            # Anomaly injection for order items (~2-3% rate)
            item_rand = random.random()
            if item_rand < 0.015:
                qty = 0  # Violates quantity > 0 range check
            elif item_rand < 0.025:
                unit_price = Decimal("-10.00")  # Violates unit_price >= 0.0 range check
            elif item_rand < 0.035:
                item_status = "UNKNOWN_ITEM_STATUS"  # Violates item_status allowed_values

            subtotal += unit_price * qty

            items_for_this_order.append({
                "product_id": p["product_id"],
                "quantity": qty,
                "unit_price": unit_price,
                "item_status": item_status,
                "created_at": now,
                "updated_at": now,
            })
            if qty > 0:
                stock_decrements[p["product_id"]] = stock_decrements.get(p["product_id"], 0) + qty

        # Discount
        discount = Decimal("0.00")
        if random.random() < ORDER_DISCOUNT_PROBABILITY:
            discount_pct = random.choice([Decimal("0.05"), Decimal("0.10"), Decimal("0.15")])
            discount = money(subtotal * discount_pct)

        # Indian GST Tax
        taxable_amount = max(Decimal("0.00"), subtotal - discount)
        tax = money(taxable_amount * TAX_RATE)

        # Flipkart Plus Free Delivery vs Standard Shipping
        if taxable_amount >= FREE_SHIPPING_THRESHOLD:
            shipping = Decimal("0.00")
        else:
            shipping = STANDARD_SHIPPING_FEE

        order_total = money(taxable_amount + tax + shipping)

        # Payment determination
        selected_method = random.choices(payment_methods, weights=method_weights, k=1)[0]
        method_name = selected_method["method_name"]

        if initial_status == "CANCELLED":
            payment_status = "FAILED"
            paid_at = None
            failed_payments += 1
            for itm in items_for_this_order:
                itm["item_status"] = "CANCELLED"
        elif method_name == "Cash on Delivery":
            payment_status = "PENDING"
            paid_at = None
            pending_payments += 1
        else:
            # 92% success rate for initial online payments
            if random.random() < 0.92:
                payment_status = "SUCCESS"
                paid_at = now
                successful_payments += 1
            else:
                # Initial payment failed! Keep order PENDING so customer can retry
                payment_status = "FAILED"
                paid_at = None
                initial_status = "PENDING"
                failed_payments += 1

        # Anomaly injection for orders (~2% rate)
        order_rand = random.random()
        order_date = now
        if order_rand < 0.01:
            # Violates allowed_values
            initial_status = random.choice([
                "UNKNOWN_STATUS", "SUSPENDED", "PENDING_VERIFICATION"
            ])
        elif order_rand < 0.02:
            order_total = Decimal("-49.99")  # Violates total_amount >= 0.0 range check
        elif order_rand < 0.025:
            order_date = now + timedelta(days=2)  # Violates cannot_be_future

        order_insert_rows.append({
            "customer_id": customer_id,
            "store_id": store_id,
            "order_status": initial_status,
            "order_date": order_date,
            "total_amount": order_total,
            "created_at": now,
            "updated_at": now,
        })

        pre_order_items.append(items_for_this_order)

        payment_amount = order_total
        # Anomaly injection for payments (~2% rate)
        pay_rand = random.random()
        if payment_status == "SUCCESS" and pay_rand < 0.015:
            paid_at = None  # Violates conditional rule: SUCCESS => paid_at IS NOT NULL
        elif pay_rand < 0.025:
            payment_amount = Decimal("0.00")  # Violates amount > 0.0 range check
        elif pay_rand < 0.035:
            payment_status = "CHARGEBACK"  # Violates payment_status allowed_values

        pre_payments.append({
            "payment_method_id": selected_method["payment_method_id"],
            "method_name": method_name,
            "payment_status": payment_status,
            "amount": payment_amount,
            "paid_at": paid_at,
            "created_at": now,
            "updated_at": now,
        })

    # 2. Bulk insert all orders and fetch generated order_ids
    order_ids = connection.scalars(
        orders_table.insert().returning(orders_table.c.order_id),
        order_insert_rows,
    ).all()

    # 3. Associate returned order_id with order items and payments
    all_order_items = []
    all_payments = []

    for order_id, items, payment in zip(order_ids, pre_order_items, pre_payments):
        for item in items:
            item["order_id"] = order_id
            all_order_items.append(item)

        payment_ref = generate_payment_reference(payment["method_name"], order_id)
        all_payments.append({
            "order_id": order_id,
            "payment_method_id": payment["payment_method_id"],
            "payment_status": payment["payment_status"],
            "amount": payment["amount"],
            "transaction_reference": payment_ref,
            "paid_at": payment["paid_at"],
            "created_at": payment["created_at"],
            "updated_at": payment["updated_at"],
        })

    # 4. Bulk insert order items and payments
    connection.execute(order_items_table.insert(), all_order_items)
    connection.execute(payments_table.insert(), all_payments)

    # 5. Decrement inventory stock (sorted by prod_id to guarantee deterministic lock order)
    for prod_id in sorted(stock_decrements.keys()):
        qty_bought = stock_decrements[prod_id]
        connection.execute(
            text("""
                UPDATE products
                SET stock_quantity = GREATEST(0, stock_quantity - :qty),
                    updated_at = :now
                WHERE product_id = :id
            """),
            {"qty": qty_bought, "now": now, "id": prod_id},
        )

    return {
        "orders_inserted": len(order_ids),
        "order_items_inserted": len(all_order_items),
        "payments_inserted": len(all_payments),
        "successful_payments": successful_payments,
        "failed_payments": failed_payments,
        "pending_payments": pending_payments,
    }


def update_orders_lifecycle(connection, count: int) -> int:
    """
    Progress existing active orders through real-world e-commerce states:
    PLACED -> CONFIRMED -> SHIPPED -> DELIVERED (or CANCELLED).
    Also updates linked payments (e.g. COD PENDING becomes SUCCESS on delivery).
    """
    active_orders = connection.execute(
        text("""
            SELECT order_id, order_status
            FROM orders
            WHERE order_status IN ('PLACED', 'CONFIRMED', 'SHIPPED')
            ORDER BY RANDOM()
            LIMIT :limit
        """),
        {"limit": count},
    ).mappings().all()

    # Sort deterministically to prevent concurrent update deadlocks
    active_orders = sorted(active_orders, key=lambda o: o["order_id"])

    now = datetime.now(UTC)
    updated = 0

    for ord_row in active_orders:
        order_id = ord_row["order_id"]
        current_status = ord_row["order_status"]
        next_choices = ORDER_STATUS_TRANSITIONS.get(current_status, [])

        if not next_choices:
            continue

        # Weigh positive delivery progression much higher than cancellation
        if "CANCELLED" in next_choices:
            new_status = random.choices(next_choices, weights=[88, 12], k=1)[0]
        else:
            new_status = random.choice(next_choices)

        connection.execute(
            text("""
                UPDATE orders
                SET order_status = :status,
                    updated_at = :now
                WHERE order_id = :order_id
            """),
            {"status": new_status, "now": now, "order_id": order_id},
        )

        # Synchronize order items and payment lifecycle
        if new_status == "DELIVERED":
            # Mark all items as DELIVERED
            connection.execute(
                text("""
                    UPDATE order_items
                    SET item_status = 'DELIVERED',
                        updated_at = :now
                    WHERE order_id = :order_id
                      AND item_status = 'ORDERED'
                """),
                {"now": now, "order_id": order_id},
            )
            # For Cash on Delivery, mark payment as SUCCESS upon delivery
            connection.execute(
                text("""
                    UPDATE payments
                    SET payment_status = 'SUCCESS',
                        paid_at = :now,
                        updated_at = :now
                    WHERE order_id = :order_id
                      AND payment_status = 'PENDING'
                """),
                {"now": now, "order_id": order_id},
            )
        elif new_status == "CANCELLED":
            # Mark items as CANCELLED
            connection.execute(
                text("""
                    UPDATE order_items
                    SET item_status = 'CANCELLED',
                        updated_at = :now
                    WHERE order_id = :order_id
                """),
                {"now": now, "order_id": order_id},
            )
            # If payment was already SUCCESS, mark as REFUNDED
            connection.execute(
                text("""
                    UPDATE payments
                    SET payment_status = 'REFUNDED',
                        updated_at = :now
                    WHERE order_id = :order_id
                      AND payment_status = 'SUCCESS'
                """),
                {"now": now, "order_id": order_id},
            )
            # Restock cancelled products back to inventory
            cancelled_items = connection.execute(
                text("SELECT product_id, quantity FROM order_items WHERE order_id = :order_id"),
                {"order_id": order_id},
            ).mappings().all()
            for itm in cancelled_items:
                connection.execute(
                    text("""
                        UPDATE products
                        SET stock_quantity = stock_quantity + :qty,
                            updated_at = :now
                        WHERE product_id = :prod_id
                    """),
                    {"qty": itm["quantity"], "now": now, "prod_id": itm["product_id"]},
                )

        updated += 1

    return updated


# ---------------------------------------------------------------------------
# PAYMENT RETRY SIMULATION (1:N PAYMENT ATTEMPTS)
# ---------------------------------------------------------------------------

def retry_pending_payments(
    connection,
    payment_methods: list[dict],
    method_weights: list[int],
    max_retries_to_process: int = 50,
) -> dict:
    """
    Simulate real-world customer payment retry behaviors for pending orders.
    Customers with a failed initial payment attempt have ~75% chance to retry:
    - Can retry with same or different payment method (UPI, Card, NetBanking, COD)
    - Successful retries confirm the order
    - Failed retries either leave it pending or cancel after max attempts (3)
    - ~25% abandon the order, causing it to cancel and restock inventory.
    """
    now = datetime.now(UTC)

    pending_orders = connection.execute(
        text("""
            SELECT o.order_id, o.customer_id, o.total_amount,
                   (
                       SELECT COUNT(p.payment_id)
                       FROM payments p
                       WHERE p.order_id = o.order_id
                   ) AS attempt_count
            FROM orders o
            WHERE o.order_status = 'PENDING'
            ORDER BY RANDOM()
            LIMIT :limit
        """),
        {"limit": max_retries_to_process},
    ).mappings().all()

    if not pending_orders:
        return {"retried_success": 0, "retried_failed": 0, "abandoned": 0}

    retried_success = 0
    retried_failed = 0
    abandoned = 0

    for ord_row in pending_orders:
        order_id = ord_row["order_id"]
        total_amount = ord_row["total_amount"]
        attempt_count = ord_row["attempt_count"]

        # 75% chance customer retries, 25% customer abandons
        if random.random() < 0.75 and attempt_count < 3:
            selected_method = random.choices(payment_methods, weights=method_weights, k=1)[0]
            method_name = selected_method["method_name"]
            pm_id = selected_method["payment_method_id"]
            payment_ref = generate_payment_reference(method_name, order_id)

            if method_name == "Cash on Delivery":
                # Customer switches to Cash on Delivery
                connection.execute(
                    payments_table.insert().values({
                        "order_id": order_id,
                        "payment_method_id": pm_id,
                        "payment_status": "PENDING",
                        "amount": total_amount,
                        "transaction_reference": payment_ref,
                        "paid_at": None,
                        "created_at": now,
                        "updated_at": now,
                    })
                )
                connection.execute(
                    text("""
                        UPDATE orders
                        SET order_status = 'PLACED',
                            updated_at = :now
                        WHERE order_id = :order_id
                    """),
                    {"now": now, "order_id": order_id},
                )
                retried_success += 1
            else:
                # 85% success rate on retry attempts
                if random.random() < 0.85:
                    connection.execute(
                        payments_table.insert().values({
                            "order_id": order_id,
                            "payment_method_id": pm_id,
                            "payment_status": "SUCCESS",
                            "amount": total_amount,
                            "transaction_reference": payment_ref,
                            "paid_at": now,
                            "created_at": now,
                            "updated_at": now,
                        })
                    )
                    connection.execute(
                        text("""
                            UPDATE orders
                            SET order_status = 'CONFIRMED',
                                updated_at = :now
                            WHERE order_id = :order_id
                        """),
                        {"now": now, "order_id": order_id},
                    )
                    retried_success += 1
                else:
                    # Retry declined / failed again
                    connection.execute(
                        payments_table.insert().values({
                            "order_id": order_id,
                            "payment_method_id": pm_id,
                            "payment_status": "FAILED",
                            "amount": total_amount,
                            "transaction_reference": payment_ref,
                            "paid_at": None,
                            "created_at": now,
                            "updated_at": now,
                        })
                    )
                    retried_failed += 1

                    # Cancel after 3 failed attempts
                    if attempt_count + 1 >= 3:
                        connection.execute(
                            text("""
                                UPDATE orders
                                SET order_status = 'CANCELLED',
                                    updated_at = :now
                                WHERE order_id = :order_id
                            """),
                            {"now": now, "order_id": order_id},
                        )
                        connection.execute(
                            text("""
                                UPDATE order_items
                                SET item_status = 'CANCELLED',
                                    updated_at = :now
                                WHERE order_id = :order_id
                            """),
                            {"now": now, "order_id": order_id},
                        )
                        # Restock items
                        items = connection.execute(
                            text(
                                "SELECT product_id, quantity FROM order_items "
                                "WHERE order_id = :order_id"
                            ),
                            {"order_id": order_id},
                        ).mappings().all()
                        for itm in items:
                            connection.execute(
                                text("""
                                    UPDATE products
                                    SET stock_quantity = stock_quantity + :qty,
                                        updated_at = :now
                                    WHERE product_id = :prod_id
                                """),
                                {"qty": itm["quantity"], "now": now, "prod_id": itm["product_id"]},
                            )
        else:
            # Customer abandons order
            connection.execute(
                text("""
                    UPDATE orders
                    SET order_status = 'CANCELLED',
                        updated_at = :now
                    WHERE order_id = :order_id
                """),
                {"now": now, "order_id": order_id},
            )
            connection.execute(
                text("""
                    UPDATE order_items
                    SET item_status = 'CANCELLED',
                        updated_at = :now
                    WHERE order_id = :order_id
                """),
                {"now": now, "order_id": order_id},
            )
            # Restock items
            items = connection.execute(
                text(
                    "SELECT product_id, quantity FROM order_items "
                    "WHERE order_id = :order_id"
                ),
                {"order_id": order_id},
            ).mappings().all()
            for itm in items:
                connection.execute(
                    text("""
                        UPDATE products
                        SET stock_quantity = stock_quantity + :qty,
                            updated_at = :now
                        WHERE product_id = :prod_id
                    """),
                    {"qty": itm["quantity"], "now": now, "prod_id": itm["product_id"]},
                )
            abandoned += 1

    return {
        "retried_success": retried_success,
        "retried_failed": retried_failed,
        "abandoned": abandoned,
    }


# ---------------------------------------------------------------------------
# PRODUCT RETURNS SIMULATION (PARTIAL & FULL RETURNS)
# ---------------------------------------------------------------------------

def process_product_returns(connection, return_count: int) -> dict:
    """
    Simulate real-world product returns on delivered orders:
    - A customer who ordered multiple items can return 1 specific item (Partial Return)
    - Or return the entire order (Full Return)
    - Updates order_items.item_status = 'RETURNED'
    - Updates orders.order_status = 'PARTIALLY_RETURNED' or 'RETURNED'
    - Inserts a REFUNDED payment record for the refunded amount
    - Restocks the returned product quantity back to inventory.
    """
    now = datetime.now(UTC)

    delivered_orders = connection.execute(
        text("""
            SELECT o.order_id, o.customer_id, o.total_amount
            FROM orders o
            WHERE o.order_status = 'DELIVERED'
              AND EXISTS (
                  SELECT 1
                  FROM order_items oi
                  WHERE oi.order_id = o.order_id
                    AND oi.item_status = 'DELIVERED'
              )
            ORDER BY RANDOM()
            LIMIT :limit
        """),
        {"limit": return_count},
    ).mappings().all()

    # Sort deterministically to prevent concurrent return deadlocks
    delivered_orders = sorted(delivered_orders, key=lambda o: o["order_id"])

    if not delivered_orders:
        return {
            "partial_returns": 0,
            "full_returns": 0,
            "items_returned": 0,
            "refund_amount_total": Decimal("0.00"),
        }

    partial_returns = 0
    full_returns = 0
    items_returned = 0
    refund_amount_total = Decimal("0.00")

    for ord_row in delivered_orders:
        order_id = ord_row["order_id"]

        items = connection.execute(
            text("""
                SELECT order_item_id, product_id, quantity, unit_price
                FROM order_items
                WHERE order_id = :order_id
                  AND item_status = 'DELIVERED'
            """),
            {"order_id": order_id},
        ).mappings().all()

        if not items:
            continue

        # Look up original payment method if possible
        last_pm_id = connection.execute(
            text("""
                SELECT payment_method_id
                FROM payments
                WHERE order_id = :order_id
                ORDER BY payment_id DESC
                LIMIT 1
            """),
            {"order_id": order_id},
        ).scalar() or 1

        if len(items) > 1 and random.random() < 0.70:
            # PARTIAL RETURN: Customer returns exactly ONE product from their multi-item order
            item_to_return = random.choice(items)
            returned_qty = item_to_return["quantity"]
            item_refund = money(item_to_return["unit_price"] * returned_qty)

            # 1. Update only that specific item to RETURNED
            connection.execute(
                text("""
                    UPDATE order_items
                    SET item_status = 'RETURNED',
                        updated_at = :now
                    WHERE order_item_id = :item_id
                """),
                {"item_id": item_to_return["order_item_id"], "now": now},
            )

            # 2. Update order status to PARTIALLY_RETURNED
            connection.execute(
                text("""
                    UPDATE orders
                    SET order_status = 'PARTIALLY_RETURNED',
                        updated_at = :now
                    WHERE order_id = :order_id
                """),
                {"order_id": order_id, "now": now},
            )

            # 3. Insert partial refund payment record
            ref_code = f"REF-PART-{order_id}-{random.randint(1000, 9999)}"
            connection.execute(
                payments_table.insert().values({
                    "order_id": order_id,
                    "payment_method_id": last_pm_id,
                    "payment_status": "REFUNDED",
                    "amount": item_refund,
                    "transaction_reference": ref_code,
                    "paid_at": now,
                    "created_at": now,
                    "updated_at": now,
                })
            )

            # 4. Restock the returned product inventory
            connection.execute(
                text("""
                    UPDATE products
                    SET stock_quantity = stock_quantity + :qty,
                        updated_at = :now
                    WHERE product_id = :prod_id
                """),
                {"qty": returned_qty, "now": now, "prod_id": item_to_return["product_id"]},
            )

            partial_returns += 1
            items_returned += 1
            refund_amount_total += item_refund

        else:
            # FULL RETURN: Customer returns ALL items in the order
            total_refund = Decimal("0.00")
            for itm in items:
                connection.execute(
                    text("""
                        UPDATE order_items
                        SET item_status = 'RETURNED',
                            updated_at = :now
                        WHERE order_item_id = :item_id
                    """),
                    {"item_id": itm["order_item_id"], "now": now},
                )
                connection.execute(
                    text("""
                        UPDATE products
                        SET stock_quantity = stock_quantity + :qty,
                            updated_at = :now
                        WHERE product_id = :prod_id
                    """),
                    {"qty": itm["quantity"], "now": now, "prod_id": itm["product_id"]},
                )
                total_refund += money(itm["unit_price"] * itm["quantity"])
                items_returned += 1

            connection.execute(
                text("""
                    UPDATE orders
                    SET order_status = 'RETURNED',
                        updated_at = :now
                    WHERE order_id = :order_id
                """),
                {"order_id": order_id, "now": now},
            )

            ref_code = f"REF-FULL-{order_id}-{random.randint(1000, 9999)}"
            connection.execute(
                payments_table.insert().values({
                    "order_id": order_id,
                    "payment_method_id": last_pm_id,
                    "payment_status": "REFUNDED",
                    "amount": total_refund,
                    "transaction_reference": ref_code,
                    "paid_at": now,
                    "created_at": now,
                    "updated_at": now,
                })
            )

            full_returns += 1
            refund_amount_total += total_refund

    return {
        "partial_returns": partial_returns,
        "full_returns": full_returns,
        "items_returned": items_returned,
        "refund_amount_total": refund_amount_total,
    }


# ---------------------------------------------------------------------------
# CORE GENERATION RUNNER
# ---------------------------------------------------------------------------

def run_incremental_seed_generation(batch_number: int = 1) -> dict:
    """
    Execute one complete batch of realistic Flipkart e-commerce events.
    """
    start_time = time.time()
    logger.info("=" * 65)
    logger.info("FLIPKART EVENT SIMULATOR | BATCH #%d START", batch_number)
    logger.info("=" * 65)

    # Phase 1 & 2: Products (Insert & Update)
    def _phase_products():
        with engine.begin() as connection:
            p_to_create = random.randint(NEW_PRODUCTS_MIN, NEW_PRODUCTS_MAX)
            p_inserted = create_incremental_products(connection, p_to_create)
            p_to_update = random.randint(PRODUCT_UPDATES_MIN, PRODUCT_UPDATES_MAX)
            p_updated = update_products_inventory(connection, p_to_update)
            return p_inserted, p_updated

    products_inserted, products_updated = with_deadlock_retry(_phase_products)

    # Phase 3 & 4: Customers (Signups & Profile Updates)
    def _phase_customers():
        with engine.begin() as connection:
            c_to_create = random.randint(NEW_CUSTOMERS_MIN, NEW_CUSTOMERS_MAX)
            new_c_ids = create_incremental_customers(connection, c_to_create)
            c_to_update = random.randint(CUSTOMER_UPDATES_MIN, CUSTOMER_UPDATES_MAX)
            c_updated = update_customers_profile(connection, c_to_update)
            return new_c_ids, c_updated

    new_customer_ids, customers_updated = with_deadlock_retry(_phase_customers)

    # Phase 5: Fetch Master Data Pools (Read-only query without write locks)
    with engine.connect() as connection:
        all_customer_ids = connection.execute(
            select(customers_table.c.customer_id)
        ).scalars().all()

        store_ids = connection.execute(
            select(stores_table.c.store_id)
        ).scalars().all()

        payment_methods = connection.execute(
            select(
                payment_methods_table.c.payment_method_id,
                payment_methods_table.c.method_name,
            )
        ).mappings().all()

        products = connection.execute(
            select(
                products_table.c.product_id,
                products_table.c.price,
            ).where(
                products_table.c.is_active.is_(True),
                products_table.c.stock_quantity > 0,
            )
        ).mappings().all()

    active_products = [
        {"product_id": p["product_id"], "price": money(p["price"])}
        for p in products
    ]

    # Phase 6: Orders & Linked Payments
    def _phase_orders():
        with engine.begin() as connection:
            order_count = random.randint(NEW_ORDERS_MIN, NEW_ORDERS_MAX)
            return generate_orders_and_payments(
                connection=connection,
                order_count=order_count,
                existing_customer_ids=all_customer_ids,
                new_customer_ids=new_customer_ids,
                store_ids=store_ids,
                products=active_products,
                payment_methods=payment_methods,
            )

    order_metrics = with_deadlock_retry(_phase_orders)

    # Phase 7: Payment Retries (1:N)
    method_weights = []
    for pm in payment_methods:
        name = pm["method_name"]
        if "UPI" in name:
            method_weights.append(55)
        elif "Credit" in name:
            method_weights.append(18)
        elif "Debit" in name:
            method_weights.append(12)
        elif "Cash" in name:
            method_weights.append(10)
        else:
            method_weights.append(5)

    def _phase_retries():
        with engine.begin() as connection:
            retries_to_process = random.randint(PAYMENT_RETRIES_MIN, PAYMENT_RETRIES_MAX)
            return retry_pending_payments(
                connection=connection,
                payment_methods=payment_methods,
                method_weights=method_weights,
                max_retries_to_process=retries_to_process,
            )

    retry_metrics = with_deadlock_retry(_phase_retries)

    # Phase 8: Order Lifecycle Transitions
    def _phase_lifecycle():
        with engine.begin() as connection:
            order_update_count = random.randint(ORDER_UPDATES_MIN, ORDER_UPDATES_MAX)
            return update_orders_lifecycle(connection, order_update_count)

    orders_updated = with_deadlock_retry(_phase_lifecycle)

    # Phase 9: Product Returns
    def _phase_returns():
        with engine.begin() as connection:
            returns_to_process = random.randint(ORDER_RETURNS_MIN, ORDER_RETURNS_MAX)
            return process_product_returns(connection, returns_to_process)

    return_metrics = with_deadlock_retry(_phase_returns)

    elapsed = time.time() - start_time

    # 10. PRINT RICH SUMMARY REPORT
    logger.info("=" * 65)
    logger.info("FLIPKART SIMULATION SUMMARY | BATCH #%d (%.2fs)", batch_number, elapsed)
    logger.info("=" * 65)
    logger.info("  New Products Added      : %d", products_inserted)
    logger.info("  Products Updated        : %d", products_updated)
    logger.info("  New Customers Signed Up : %d", len(new_customer_ids))
    logger.info("  Customers Updated       : %d", customers_updated)
    logger.info("  Orders Placed           : %d", order_metrics["orders_inserted"])
    logger.info("  Order Items Purchased   : %d", order_metrics["order_items_inserted"])
    logger.info("  Orders Updated/Delivered: %d", orders_updated)
    logger.info("  Payments Created        : %d", order_metrics["payments_inserted"])
    logger.info("    -> Successful (UPI/Card): %d", order_metrics["successful_payments"])
    logger.info("    -> Pending (Cash on Del): %d", order_metrics["pending_payments"])
    logger.info("    -> Failed / Declined    : %d", order_metrics["failed_payments"])
    logger.info("  Payment Retries (1:N)   : %d retried (%d success, %d failed, %d abandoned)",
                retry_metrics["retried_success"] + retry_metrics["retried_failed"],
                retry_metrics["retried_success"],
                retry_metrics["retried_failed"],
                retry_metrics["abandoned"])
    logger.info(
        "  Product Returns (1:N)   : %d partial, %d full (%d items returned, ₹%.2f refunded)",
        return_metrics["partial_returns"],
        return_metrics["full_returns"],
        return_metrics["items_returned"],
        return_metrics["refund_amount_total"],
    )
    logger.info("=" * 65)

    return {
        "batch_number": batch_number,
        "elapsed_seconds": round(elapsed, 2),
        "products_inserted": products_inserted,
        "customers_inserted": len(new_customer_ids),
        "orders_inserted": order_metrics["orders_inserted"],
        "order_items_inserted": order_metrics["order_items_inserted"],
        "payments_inserted": order_metrics["payments_inserted"],
        "orders_updated": orders_updated,
        "retry_metrics": retry_metrics,
        "return_metrics": return_metrics,
    }


def continuous_generation_loop(interval_seconds: int = 60) -> None:
    """
    Continuously emit Flipkart e-commerce event batches at regular intervals (e.g. every minute).
    """
    logger.info("Starting Continuous Flipkart Event Generator")
    logger.info("Interval: %d seconds | Press Ctrl+C to terminate cleanly", interval_seconds)

    batch = 1
    try:
        while True:
            run_incremental_seed_generation(batch_number=batch)
            logger.info("Batch #%d completed. Sleeping for %d seconds...", batch, interval_seconds)
            time.sleep(interval_seconds)
            batch += 1
    except KeyboardInterrupt:
        logger.info("\nGenerator terminated gracefully by user.")
        sys.exit(0)


# ---------------------------------------------------------------------------
# CLI ENTRY POINT
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Flipkart E-Commerce Live Activity Data Generator"
    )
    parser.add_argument(
        "--continuous", "-c",
        action="store_true",
        help="Run continuously in a loop every minute",
    )
    parser.add_argument(
        "--interval", "-i",
        type=int,
        default=int(os.getenv("CONTINUOUS_INTERVAL_SECONDS", "60")),
        help="Interval between batches in seconds (default: 60)",
    )

    args = parser.parse_args()

    # Also honor CONTINUOUS_MODE env var if set
    env_continuous = os.getenv("CONTINUOUS_MODE", "").lower() in ("true", "1", "yes")
    is_continuous = args.continuous or env_continuous

    if is_continuous:
        continuous_generation_loop(interval_seconds=args.interval)
    else:
        run_incremental_seed_generation(batch_number=1)


if __name__ == "__main__":
    main()
