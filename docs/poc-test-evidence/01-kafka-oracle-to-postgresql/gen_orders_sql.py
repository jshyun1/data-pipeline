import random
from datetime import date, timedelta

random.seed(42)
PRODUCTS = ["Widget-A","Widget-B","Gadget-X","Gadget-Y","Tool-Pro","Tool-Lite","Device-Z","Kit-Standard","Kit-Deluxe","Part-001"]
N = 10000
BATCH = 1000

base_date = date(2026, 1, 1)

batches = []
batch_lines = []
for i in range(1, N + 1):
    d = base_date + timedelta(days=random.randint(0, 200))
    cust = f"Customer_{i:05d}"
    prod = random.choice(PRODUCTS)
    qty = random.randint(1, 20)
    price = round(random.uniform(10, 1000), 2)
    into = (f"  INTO CSB.KAFKA_PERF_TEST (order_id, order_date, customer_name, product_name, quantity, unit_price, updated_at) "
            f"VALUES ({i}, DATE '{d.isoformat()}', '{cust}', '{prod}', {qty}, {price}, SYSTIMESTAMP)")
    batch_lines.append(into)
    if len(batch_lines) == BATCH:
        batches.append("INSERT ALL\n" + "\n".join(batch_lines) + "\nSELECT * FROM dual")
        batch_lines = []
if batch_lines:
    batches.append("INSERT ALL\n" + "\n".join(batch_lines) + "\nSELECT * FROM dual")

for idx, content in enumerate(batches):
    with open(f"/tmp/claude-1000/-home-user-data-pipeline/3e46d72a-00dc-4644-80fb-f8aa62d4e61e/scratchpad/orders_batch_{idx:02d}.sql", "w") as f:
        f.write(content)

print(f"생성된 배치 수: {len(batches)}, 총 {N}건")
