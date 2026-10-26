"""
Flipkart-style Realistic E-Commerce Catalog & Entity Definitions.

Provides authentic Indian e-commerce data:
- Realistic Flipkart product catalog per category with realistic prices, brands, specs & SKUs
- Authentic Indian customer profiles (Tier-1/2/3 cities, paired states, Indian names)
- Authentic Indian payment references (UPI UTRs, Razorpay/PayU IDs, NetBanking, COD)
"""

from __future__ import annotations

import random
import uuid
from decimal import ROUND_HALF_UP, Decimal


def money(value: Decimal | float | int) -> Decimal:
    """Always keep monetary values at two decimal places."""
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# Indian Cities and their corresponding States
INDIAN_LOCATIONS = [
    ("Bengaluru", "Karnataka"),
    ("Mumbai", "Maharashtra"),
    ("Delhi", "Delhi"),
    ("Hyderabad", "Telangana"),
    ("Chennai", "Tamil Nadu"),
    ("Kolkata", "West Bengal"),
    ("Pune", "Maharashtra"),
    ("Ahmedabad", "Gujarat"),
    ("Jaipur", "Rajasthan"),
    ("Surat", "Gujarat"),
    ("Lucknow", "Uttar Pradesh"),
    ("Kochi", "Kerala"),
    ("Coimbatore", "Tamil Nadu"),
    ("Indore", "Madhya Pradesh"),
    ("Chandigarh", "Punjab"),
    ("Patna", "Bihar"),
    ("Bhopal", "Madhya Pradesh"),
    ("Vadodara", "Gujarat"),
    ("Visakhapatnam", "Andhra Pradesh"),
    ("Nagpur", "Maharashtra"),
    ("Mysore", "Karnataka"),
    ("Madurai", "Tamil Nadu"),
    ("Bhubaneswar", "Odisha"),
    ("Guwahati", "Assam"),
    ("Dehradun", "Uttarakhand"),
    ("Noida", "Uttar Pradesh"),
    ("Gurgaon", "Haryana"),
    ("Thiruvananthapuram", "Kerala"),
    ("Ranchi", "Jharkhand"),
    ("Varanasi", "Uttar Pradesh"),
    ("Jodhpur", "Rajasthan"),
    ("Amritsar", "Punjab"),
    ("Mangalore", "Karnataka"),
    ("Salem", "Tamil Nadu"),
    ("Vijayawada", "Andhra Pradesh"),
]

INDIAN_FIRST_NAMES = [
    "Aarav", "Aditya", "Akash", "Amit", "Ananya", "Anjali", "Arjun", "Deepak",
    "Divya", "Gaurav", "Isha", "Karan", "Karthik", "Kavita", "Kiran", "Manish",
    "Meera", "Neha", "Nikhil", "Nisha", "Pallavi", "Pooja", "Pranav", "Prateek",
    "Preeti", "Priya", "Rahul", "Rajesh", "Rashmi", "Ravi", "Ritu", "Rohan",
    "Rohit", "Sandeep", "Sanjay", "Shilpa", "Shreya", "Shruti", "Sneha", "Suresh",
    "Swati", "Tanvi", "Varun", "Vikram", "Vinay", "Vivek", "Yash", "Zoya"
]

INDIAN_LAST_NAMES = [
    "Agarwal", "Bhat", "Bhattacharya", "Chatterjee", "Choudhury", "Das",
    "Deshmukh", "Ghosh", "Gupta", "Hegde", "Iyer", "Jain", "Joshi", "Kulkarni",
    "Kumar", "Mehta", "Menon", "Mishra", "Mukherjee", "Nair", "Nambiar", "Patel",
    "Pillai", "Rao", "Reddy", "Sen", "Sharma", "Singh", "Verma", "Yadav"
]

EMAIL_DOMAINS = ["gmail.com", "outlook.com", "yahoo.in", "icloud.com"]

# Product Catalog by Category Name
FLIPKART_CATALOG = {
    "Mobiles": {
        "sku_prefix": "FK-MOB",
        "brands": ["Apple", "Samsung", "OnePlus", "Xiaomi", "Realme", "Vivo", "Google", "Motorola", "Nothing", "POCO"],
        "templates": [
            ("Apple iPhone 15 ({color}, {storage})", ["128 GB", "256 GB", "512 GB"], ["Black", "Blue", "Green", "Yellow", "Pink"], (64999, 89999)),
            ("Apple iPhone 15 Pro ({color}, {storage})", ["128 GB", "256 GB", "1 TB"], ["Natural Titanium", "Blue Titanium", "White Titanium", "Black Titanium"], (119999, 149999)),
            ("Apple iPhone 14 ({color}, {storage})", ["128 GB", "256 GB"], ["Midnight", "Starlight", "Purple", "Blue"], (52999, 64999)),
            ("Samsung Galaxy S24 Ultra 5G ({color}, {storage})", ["256 GB", "512 GB"], ["Titanium Gray", "Titanium Black", "Titanium Violet"], (119999, 139999)),
            ("Samsung Galaxy S23 FE 5G ({color}, {storage})", ["128 GB", "256 GB"], ["Mint", "Graphite", "Purple"], (39999, 49999)),
            ("Samsung Galaxy A55 5G ({color}, {storage})", ["128 GB", "256 GB"], ["Awesome Iceblue", "Awesome Navy", "Awesome Lilac"], (34999, 41999)),
            ("OnePlus 12 5G ({color}, {storage})", ["256 GB", "512 GB"], ["Silky Black", "Flowy Emerald"], (59999, 69999)),
            ("OnePlus Nord CE4 5G ({color}, {storage})", ["128 GB", "256 GB"], ["Dark Chrome", "Celadon Marble"], (24999, 26999)),
            ("Xiaomi Redmi Note 13 Pro+ 5G ({color}, {storage})", ["256 GB", "512 GB"], ["Fusion Black", "Fusion Purple", "Fusion White"], (29999, 33999)),
            ("Realme 12 Pro+ 5G ({color}, {storage})", ["128 GB", "256 GB"], ["Submarine Blue", "Navigator Beige"], (27999, 31999)),
            ("Vivo V30 Pro 5G ({color}, {storage})", ["256 GB", "512 GB"], ["Classic Black", "Andaman Blue"], (39999, 44999)),
            ("Google Pixel 8 5G ({color}, {storage})", ["128 GB", "256 GB"], ["Obsidian", "Hazel", "Rose"], (61999, 71999)),
            ("Motorola Edge 50 Pro 5G ({color}, {storage})", ["256 GB"], ["Moonlight Pearl", "Luxe Lavender", "Black Beauty"], (29999, 33999)),
            ("Nothing Phone (2a) 5G ({color}, {storage})", ["128 GB", "256 GB"], ["White", "Black", "Special Edition"], (23999, 27999)),
            ("POCO X6 Pro 5G ({color}, {storage})", ["256 GB", "512 GB"], ["Spectre Black", "Racing Yellow"], (24999, 28999)),
        ]
    },
    "Laptops": {
        "sku_prefix": "FK-LAP",
        "brands": ["Apple", "HP", "Dell", "Lenovo", "ASUS", "Acer"],
        "templates": [
            ("Apple MacBook Air M2 13-inch ({color}, 8GB RAM, 256GB SSD)", ["8GB/256GB"], ["Space Grey", "Midnight", "Starlight", "Silver"], (84990, 94990)),
            ("Apple MacBook Air M3 15-inch ({color}, 16GB RAM, 512GB SSD)", ["16GB/512GB"], ["Midnight", "Starlight", "Space Grey"], (144990, 159990)),
            ("Apple MacBook Pro M3 Pro ({color}, 18GB RAM, 512GB SSD)", ["18GB/512GB"], ["Space Black", "Silver"], (189990, 199990)),
            ("HP Pavilion 15 (13th Gen Intel Core i5, 16GB RAM, 512GB SSD, Silver)", ["Standard"], ["Natural Silver"], (62990, 69990)),
            ("HP Victus Gaming Laptop (AMD Ryzen 5 5600H, RTX 3050, 16GB RAM, 512GB SSD)", ["Standard"], ["Mica Silver"], (58990, 64990)),
            ("Dell Inspiron 3530 (13th Gen Core i3, 8GB RAM, 512GB SSD, Platinum Silver)", ["Standard"], ["Platinum Silver"], (37990, 42990)),
            ("Dell XPS 13 Plus (13th Gen Core i7, 16GB RAM, 1TB SSD, OLED)", ["Standard"], ["Platinum"], (164990, 179990)),
            ("Lenovo IdeaPad Slim 3 (Core i5 12th Gen, 16GB RAM, 512GB SSD)", ["Standard"], ["Arctic Grey"], (48990, 54990)),
            ("Lenovo Legion 5 Gaming (AMD Ryzen 7, RTX 4060, 16GB RAM, 512GB SSD)", ["Standard"], ["Storm Grey"], (99990, 114990)),
            ("ASUS Vivobook 15 (Intel Core i5 12th Gen, 16GB RAM, 512GB SSD)", ["Standard"], ["Quiet Blue", "Cool Silver"], (46990, 52990)),
            ("ASUS TUF Gaming F15 (Core i7 11th Gen, RTX 3050, 16GB RAM, 512GB SSD)", ["Standard"], ["Graphite Black"], (68990, 74990)),
            ("Acer Swift Go 14 OLED (Intel Core i5 13th Gen, 16GB RAM, 512GB SSD)", ["Standard"], ["Pure Silver"], (59990, 64990)),
        ]
    },
    "Electronics": {
        "sku_prefix": "FK-ELEC",
        "brands": ["Sony", "boAt", "JBL", "Noise", "Bose", "Marshall", "Samsung", "OnePlus"],
        "templates": [
            ("boAt Rockerz 450 Bluetooth On-Ear Headphones ({color})", ["Standard"], ["Luscious Black", "Hazel Beige", "Aqua Blue"], (1299, 1699)),
            ("boAt Airdopes 141 ANC True Wireless Earbuds ({color})", ["Standard"], ["Gunmetal Grey", "Active Black", "Cider"], (1499, 1999)),
            ("Sony WH-1000XM5 Wireless Active Noise Cancelling Headphones ({color})", ["Standard"], ["Black", "Silver", "Midnight Blue"], (26990, 29990)),
            ("Sony WF-C500 Truly Wireless Bluetooth Earbuds ({color})", ["Standard"], ["Black", "White", "Orange"], (4490, 5990)),
            ("JBL Flip 6 Portable Bluetooth Speaker with IP67 Waterproof ({color})", ["Standard"], ["Black", "Blue", "Red", "Teal"], (8999, 10999)),
            ("JBL Tune 770NC Wireless Over-Ear Active Noise Cancelling Headphones", ["Standard"], ["Black", "White", "Blue"], (5999, 6999)),
            ("Noise ColorFit Pulse 2 Max 1.85-inch Display Smartwatch ({color})", ["Standard"], ["Jet Black", "Deep Wine", "Rose Pink"], (1299, 1799)),
            ("Noise Diva Smartwatch with Diamond Cut Dial ({color})", ["Standard"], ["Silver Link", "Rose Gold", "Gold Mesh"], (2999, 3999)),
            ("Bose QuietComfort 45 Bluetooth Wireless Noise Cancelling Headphones", ["Standard"], ["Triple Black", "White Smoke"], (24990, 28990)),
            ("Marshall Emberton II Portable Bluetooth Speaker ({color})", ["Standard"], ["Black and Brass", "Cream"], (14999, 17499)),
            ("OnePlus Bullets Wireless Z2 Bluetooth Neckband ({color})", ["Standard"], ["Magico Black", "Acoustic Red"], (1599, 1999)),
            ("Samsung Galaxy Watch6 LTE 44mm Smartwatch ({color})", ["Standard"], ["Graphite", "Silver"], (24999, 29999)),
        ]
    },
    "Accessories": {
        "sku_prefix": "FK-ACC",
        "brands": ["Spigen", "SanDisk", "Portronics", "Ambrane", "Mi", "Anker", "TP-Link"],
        "templates": [
            ("Spigen Ultra Hybrid Back Case Cover for iPhone ({color})", ["Standard"], ["Crystal Clear", "Matte Black"], (1199, 1499)),
            ("SanDisk Ultra 128GB MicroSDXC Memory Card UHS-I Class 10", ["Standard"], ["Red/Grey"], (849, 1099)),
            ("SanDisk Extreme 1TB Portable External NVMe SSD 1050MB/s", ["Standard"], ["Black/Orange"], (8999, 10999)),
            ("Ambrane 20000mAh Power Bank with 22.5W Fast Charging ({color})", ["Standard"], ["Metallic Black", "Green"], (1499, 1999)),
            ("Portronics 6-in-1 Type-C Multi-Port USB Hub with HDMI 4K", ["Standard"], ["Grey"], (999, 1499)),
            ("Mi 33W SonicCharge 2.0 Fast Wall Charger with Type-C Cable", ["Standard"], ["White"], (799, 999)),
            ("Anker PowerLine III USB-C to USB-C 100W Fast Charging Cable 6ft", ["Standard"], ["Black", "White"], (899, 1299)),
            ("TP-Link Archer C6 Dual Band Gigabit AC1200 Wi-Fi Router", ["Standard"], ["Black"], (2199, 2699)),
            ("Boat Rugged v3 Micro USB 1.5m Fast Charging Cable", ["Standard"], ["Black"], (299, 449)),
        ]
    },
    "Fashion": {
        "sku_prefix": "FK-FASH",
        "brands": ["Levi's", "Roadster", "Allen Solly", "Peter England", "US Polo Assn", "Biba", "W", "Flying Machine", "Wrogn"],
        "templates": [
            ("Levi's Men 511 Slim Fit Mid-Rise Stretch Blue Jeans ({size})", ["30", "32", "34", "36"], ["Dark Blue", "Mid Blue", "Washed Black"], (2299, 3699)),
            ("Roadster Men Solid Casual 100% Cotton Spread Collar Shirt ({size})", ["S", "M", "L", "XL"], ["Navy Blue", "Olive Green", "Maroon", "White"], (699, 1199)),
            ("Allen Solly Men Slim Fit Formal Poly-Viscose Trousers ({size})", ["30", "32", "34", "36"], ["Black", "Dark Charcoal", "Navy"], (1499, 2499)),
            ("Peter England Men Regular Fit Solid Formal Cotton Shirt ({size})", ["38", "40", "42", "44"], ["Sky Blue", "White", "Light Pink"], (999, 1599)),
            ("US Polo Assn Men Solid Polo Collar Pique T-Shirt ({size})", ["S", "M", "L", "XL"], ["Classic Navy", "Bright Red", "White", "Black"], (1199, 1899)),
            ("Biba Women Anarkali Embroidered Pure Cotton Kurta with Dupatta ({size})", ["S", "M", "L", "XL"], ["Royal Blue", "Mustard Yellow", "Teal"], (2499, 4299)),
            ("W for Woman Printed Straight Cotton Kurta ({size})", ["S", "M", "L", "XL"], ["Coral Pink", "Mint Green", "Beige"], (1299, 2199)),
            ("Flying Machine Men Skinny Fit Washed Stretch Jeans ({size})", ["30", "32", "34", "36"], ["Ice Blue", "Vintage Indigo"], (1499, 2299)),
            ("Wrogn Men Solid Slim Fit Casual Denim Jacket ({size})", ["M", "L", "XL"], ["Light Blue", "Washed Charcoal"], (1999, 3299)),
            ("HIGHLANDER Men Regular Fit Solid Casual Cotton Chinos ({size})", ["30", "32", "34", "36"], ["Khaki", "Navy", "Olive"], (799, 1299)),
        ]
    },
    "Footwear": {
        "sku_prefix": "FK-FOOT",
        "brands": ["Nike", "Adidas", "Puma", "Bata", "Woodland", "Sparx", "Skechers", "Red Tape", "Campus"],
        "templates": [
            ("Nike Air Monarch IV Cross Trainer Gym Shoes ({size})", ["UK 7", "UK 8", "UK 9", "UK 10"], ["White/Metallic Silver", "Black"], (4295, 5495)),
            ("Nike Revolution 6 Running Shoes for Men ({size})", ["UK 7", "UK 8", "UK 9", "UK 10"], ["Black/White", "Midnight Navy"], (3195, 3995)),
            ("Adidas Originals Superstar Classic Leather Sneakers ({size})", ["UK 7", "UK 8", "UK 9", "UK 10"], ["Cloud White/Core Black"], (6999, 8999)),
            ("Adidas Men Runfalcon 3.0 Road Running Shoes ({size})", ["UK 7", "UK 8", "UK 9", "UK 10"], ["Core Black", "Lucid Cyan"], (2999, 3999)),
            ("Puma Smash v2 L Leather Casual Sneakers ({size})", ["UK 7", "UK 8", "UK 9", "UK 10"], ["Puma White", "Puma Black"], (2499, 3499)),
            ("Bata Men Genuine Leather Formal Derby Shoes ({size})", ["UK 7", "UK 8", "UK 9", "UK 10"], ["Classic Black", "Dark Tan"], (1499, 2299)),
            ("Woodland Men Camel Genuine Leather Casual Outdoor Boots ({size})", ["UK 7", "UK 8", "UK 9", "UK 10"], ["Camel Khaki", "Olive Green"], (3795, 4995)),
            ("Skechers Go Walk 6 Slip-On Breathable Walking Shoes ({size})", ["UK 7", "UK 8", "UK 9", "UK 10"], ["Navy/Blue", "Charcoal"], (4499, 5999)),
            ("Red Tape Men Athleisure Walking Sneaker Shoes ({size})", ["UK 7", "UK 8", "UK 9", "UK 10"], ["White/Grey", "Black/Red"], (1299, 1899)),
            ("Campus Men North Plus Running Shoes ({size})", ["UK 7", "UK 8", "UK 9", "UK 10"], ["Blue/Orange", "All Black"], (999, 1499)),
        ]
    },
    "Home & Kitchen": {
        "sku_prefix": "FK-HOME",
        "brands": ["Prestige", "Philips", "Pigeon", "Milton", "Butterfly", "Bajaj", "Crompton"],
        "templates": [
            ("Prestige Iris 750 Watt Heavy Duty Mixer Grinder with 4 Jars", ["Standard"], ["White and Blue"], (2799, 3499)),
            ("Philips HD9252/90 4.1L Digital Air Fryer with Rapid Air Technology", ["Standard"], ["Glossy Black"], (7499, 9499)),
            ("Pigeon by Stovekraft Cruise 1800 Watt Induction Cooktop", ["Standard"], ["Black"], (1499, 1999)),
            ("Milton Thermosteel Duo Deluxe 1000ml Vacuum Insulated Flask", ["Standard"], ["Silver Stainless Steel"], (799, 1099)),
            ("Butterfly Smart 2-Burner Glass Top Gas Stove (Manual Ignition)", ["Standard"], ["Black Toughened Glass"], (2199, 2899)),
            ("Bajaj New Shakti Neo 15L Vertical Storage Water Geyser", ["Standard"], ["White and Blue"], (5499, 6499)),
            ("Crompton Energion Hyperjet 1200mm 5-Star BLDC Ceiling Fan with Remote", ["Standard"], ["Opal White", "Brown"], (2899, 3499)),
            ("Prestige Nakshatra Plus 3-Litre Hard Anodized Pressure Cooker", ["Standard"], ["Anodized Black"], (1399, 1799)),
            ("Milton Spotzero Prime Spin Mop with Wheels and Stainless Steel Wringer", ["Standard"], ["Aqua Green"], (999, 1399)),
        ]
    },
    "Beauty": {
        "sku_prefix": "FK-BEAUT",
        "brands": ["Minimalist", "Mamaearth", "The Derma Co", "Maybelline", "Lakme", "Cetaphil", "Nivea", "Plum"],
        "templates": [
            ("Minimalist 10% Niacinamide Face Serum with Zinc (30ml)", ["30ml"], ["Standard"], (549, 599)),
            ("Minimalist 2% Salicylic Acid Face Serum for Acne & Blackheads (30ml)", ["30ml"], ["Standard"], (499, 549)),
            ("Mamaearth Onion Hair Fall Control Oil with Redensyl (250ml)", ["250ml"], ["Standard"], (449, 599)),
            ("The Derma Co 1% Hyaluronic Sunscreen Aqua Gel SPF 50 PA++++ (50g)", ["50g"], ["Standard"], (449, 499)),
            ("Maybelline New York Super Stay Matte Ink Liquid Lipstick ({color})", ["5ml"], ["Seductress", "Pioneer", "Ruler", "Amazonian"], (549, 699)),
            ("Maybelline New York Colossal Waterproof Mascara (9.2ml)", ["9.2ml"], ["Very Black"], (349, 429)),
            ("Cetaphil Gentle Skin Cleanser for Sensitive Skin (250ml)", ["250ml"], ["Standard"], (520, 610)),
            ("Lakme Absolute Skin Natural Mousse Matte Foundation ({color})", ["25g"], ["Rose Fair", "Golden Medium", "Beige Honey"], (649, 799)),
            ("Plum Green Tea Alcohol-Free Face Toner for Oily Skin (200ml)", ["200ml"], ["Standard"], (349, 425)),
            ("Nivea Nourishing Body Milk Body Lotion with Deep Moisture (400ml)", ["400ml"], ["Standard"], (349, 475)),
        ]
    },
    "Sports": {
        "sku_prefix": "FK-SPORT",
        "brands": ["Yonex", "Nivia", "Cosco", "Decathlon", "SG", "DSC", "Vector X", "Strauss"],
        "templates": [
            ("Yonex Nanoray 7000 Badminton Racquet with Full Cover ({color})", ["G4"], ["Bright Red", "Electric Blue"], (1990, 2490)),
            ("Yonex Mavis 350 Nylon Shuttlecock Pack of 6 ({color})", ["Pack of 6"], ["Yellow", "White"], (999, 1250)),
            ("Nivia Storm Football - Size 5 Rubber Molded Match Ball", ["Size 5"], ["White/Black"], (449, 649)),
            ("Cosco Light Cricket Tennis Ball Pack of 6 Heavy Duty", ["Pack of 6"], ["Florescent Yellow"], (399, 499)),
            ("Decathlon Domyos 6mm Cushioned Anti-Slip Yoga Mat ({color})", ["6mm"], ["Midnight Blue", "Forest Green", "Purple"], (699, 999)),
            ("SG Club Leather Cricket Ball 4-Piece Alum Tanned (Red)", ["Single"], ["Red", "White"], (499, 699)),
            ("Vector X Weight Lifting Gym Gloves with Wrist Support Strap", ["M", "L", "XL"], ["Black/Orange", "All Black"], (399, 549)),
            ("Strauss Adjustable PVC High-Speed Skipping Jump Rope", ["Standard"], ["Black/Neon"], (199, 299)),
            ("DSC Condor Kashmir Willow Full Size Cricket Bat with Cover", ["Short Handle"], ["Natural Wood"], (1499, 2199)),
        ]
    },
    "Books": {
        "sku_prefix": "FK-BOOK",
        "brands": ["Penguin", "HarperCollins", "Rupa", "Bloomsbury", "Pan Macmillan"],
        "templates": [
            ("Atomic Habits: An Easy & Proven Way to Build Good Habits by James Clear", ["Paperback"], ["Standard"], (399, 550)),
            ("The Psychology of Money by Morgan Housel (Paperback)", ["Paperback"], ["Standard"], (299, 399)),
            ("Ikigai: The Japanese Secret to a Long and Happy Life (Hardcover)", ["Hardcover"], ["Standard"], (349, 499)),
            ("Sapiens: A Brief History of Humankind by Yuval Noah Harari", ["Paperback"], ["Standard"], (449, 599)),
            ("Rich Dad Poor Dad: What the Rich Teach Their Kids About Money", ["Paperback"], ["Standard"], (299, 399)),
            ("Do Epic Shit by Ankur Warikoo (Paperback)", ["Paperback"], ["Standard"], (249, 349)),
            ("The Alchemist by Paulo Coelho (25th Anniversary Edition)", ["Paperback"], ["Standard"], (249, 320)),
            ("Deep Work: Rules for Focused Success by Cal Newport", ["Paperback"], ["Standard"], (349, 450)),
            ("Show Your Work! by Austin Kleon (Paperback)", ["Paperback"], ["Standard"], (299, 380)),
            ("Think Like a Monk by Jay Shetty (Paperback)", ["Paperback"], ["Standard"], (349, 499)),
            ("The Subtle Art of Not Giving a F*ck by Mark Manson", ["Paperback"], ["Standard"], (320, 420)),
        ]
    }
}


def generate_flipkart_product(category_name: str, sku_counter: int) -> dict:
    """
    Generate an authentic Flipkart product record for a given category.
    """
    cat_data = FLIPKART_CATALOG.get(category_name)
    if not cat_data:
        # Fallback for unknown category
        return {
            "product_name": f"Flipkart Certified {category_name} Item #{sku_counter}",
            "sku": f"FK-GEN-{sku_counter:06d}",
            "price": money(random.uniform(299, 9999)),
            "stock_quantity": random.randint(20, 400),
            "is_active": True,
        }

    template, sizes, colors, price_range = random.choice(cat_data["templates"])
    size = random.choice(sizes)
    color = random.choice(colors)

    try:
        product_name = template.format(size=size, storage=size, color=color)
    except KeyError:
        product_name = template.replace("{size}", size).replace("{storage}", size).replace("{color}", color)
    prefix = cat_data["sku_prefix"]
    unique_suffix = uuid.uuid4().hex[:6].upper()
    sku = f"{prefix}-{sku_counter:05d}-{unique_suffix}"

    price = money(round(random.uniform(price_range[0], price_range[1]), 2))
    stock_quantity = random.randint(15, 350)

    return {
        "product_name": product_name,
        "sku": sku,
        "price": price,
        "stock_quantity": stock_quantity,
        "is_active": True,
    }


def generate_indian_customer() -> dict:
    """
    Generate an authentic Indian e-commerce customer profile.
    """
    first_name = random.choice(INDIAN_FIRST_NAMES)
    last_name = random.choice(INDIAN_LAST_NAMES)
    city, state = random.choice(INDIAN_LOCATIONS)

    # Unique email format
    unique_id = uuid.uuid4().hex[:5]
    domain = random.choice(EMAIL_DOMAINS)
    email = f"{first_name.lower()}.{last_name.lower()}.{unique_id}@{domain}"

    # Realistic Indian mobile number format (+91 9/8/7 followed by 9 digits)
    prefix = random.choice(["98", "99", "97", "96", "95", "91", "88", "87", "80", "70", "79"])
    rest = "".join(str(random.randint(0, 9)) for _ in range(8))
    phone = f"+91-{prefix}{rest}"

    return {
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "phone": phone,
        "city": city,
        "state": state,
        "country": "India",
    }


def generate_payment_reference(method_name: str, order_id: int) -> str:
    """
    Generate realistic Indian payment gateway transaction references.
    """
    hex_suffix = uuid.uuid4().hex[:10].upper()

    if method_name == "UPI":
        # Realistic Indian UPI UTR (12 digits, often starting with year/Julian date or provider prefix)
        utr = f"4{random.randint(10000000000, 99999999999)}"
        provider = random.choice(["phonepe", "gpay", "paytm", "axisbhim", "sbi"])
        return f"UPI/{provider}/{utr}"

    elif method_name in ("Credit Card", "Debit Card"):
        # Realistic Razorpay / PayU / Juspay token
        gateway = random.choice(["rzp_pay", "payu_txn", "juspay_ord"])
        return f"{gateway}_{hex_suffix}"

    elif method_name == "Net Banking":
        bank = random.choice(["HDFC", "ICICI", "SBI", "AXIS", "KOTAK"])
        return f"NB_{bank}_{order_id}_{hex_suffix[:6]}"

    elif method_name == "Cash on Delivery":
        return f"COD_AWB_{order_id}_{hex_suffix[:8]}"

    return f"TXN_{hex_suffix}"
