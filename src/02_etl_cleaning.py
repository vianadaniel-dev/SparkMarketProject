import os
import sys

# Configurar HADOOP_HOME y PATH para compatibilidad nativa en Windows
hadoop_dir = os.path.abspath("hadoop")
if os.path.exists(os.path.join(hadoop_dir, "bin")):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")

# Garantizar que los workers de Spark usen el mismo interprete de Python en Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def create_spark_session():
    """
    Inicializa la sesion local de Spark para el pipeline ETL.
    """
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("02-etl-cleaning-taller-data-engineer")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def try_cast(col_name, target_type):
    """
    Aplica try_cast en Spark SQL para tolerar valores mal formados o 'NULL'.
    """
    return F.expr(f"try_cast({col_name} AS {target_type})")


def load_and_type_data(spark):
    """
    Fase 1: Ingesta tolerante a nulos literales y tipado explícito.
    """
    print("=" * 80)
    print("1. INGESTA DE ARCHIVOS CRUDOS CON TOLERANCIA A 'NULL'")
    print("=" * 80)

    raw_cust = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("nullValue", "NULL")
        .csv("datos/customers.csv")
        .withColumn("signup_date", try_cast("signup_date", "date"))
    )

    raw_prod = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("nullValue", "NULL")
        .csv("datos/products.csv")
        .withColumn("unit_price", try_cast("unit_price", "double"))
        .withColumn("active", try_cast("active", "boolean"))
    )

    raw_orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("nullValue", "NULL")
        .csv("datos/orders.csv")
        .withColumn("order_ts", try_cast("order_ts", "timestamp"))
        .withColumn("updated_at", try_cast("updated_at", "timestamp"))
    )

    raw_items = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("nullValue", "NULL")
        .csv("datos/order_items.csv")
        .withColumn("quantity", try_cast("quantity", "int"))
        .withColumn("discount_pct", try_cast("discount_pct", "double"))
    )

    print(f"[*] Registros cargados:")
    print(f"    - Customers:   {raw_cust.count():,} filas")
    print(f"    - Products:    {raw_prod.count():,} filas")
    print(f"    - Orders:      {raw_orders.count():,} filas")
    print(f"    - Order Items: {raw_items.count():,} filas\n")

    return raw_cust, raw_prod, raw_orders, raw_items


def deduplicate_and_standardize(raw_cust, raw_prod, raw_orders, raw_items):
    """
    Fase 2: Deduplicación y Estandarización de reglas de negocio.
    """
    print("=" * 80)
    print("2. DEDUPLICACION Y ESTANDARIZACION")
    print("=" * 80)

    # 1. Customers: Imputar email nulo y deduplicar por customer_id
    customers_std = (
        raw_cust
        .withColumn(
            "email",
            F.when(
                F.col("email").isNull() | (F.trim(F.col("email")) == ""),
                "sin_email@example.com"
            ).otherwise(F.col("email"))
        )
        .dropDuplicates(["customer_id"])
    )

    # 2. Products: Deduplicar por product_id
    products_std = raw_prod.dropDuplicates(["product_id"])

    # 3. Orders: Normalizar status (trim + lower + correccion de typo) y deduplicar por updated_at DESC
    w_orders = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    orders_std = (
        raw_orders
        .withColumn(
            "status",
            F.when(F.trim(F.lower(F.col("status"))) == "deliverd", "delivered")
             .otherwise(F.trim(F.lower(F.col("status"))))
        )
        .withColumn("row_num", F.row_number().over(w_orders))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # 4. Order Items: Deduplicar por line_id
    items_std = raw_items.dropDuplicates(["line_id"])

    print(f"[*] Tras deduplicación y correcciones:")
    print(f"    - Customers:   {customers_std.count():,} filas (PK C0020 deduplicada, C0013 con email imputado)")
    print(f"    - Products:    {products_std.count():,} filas")
    print(f"    - Orders:      {orders_std.count():,} filas (PK O00012 deduplicada por updated_at más reciente)")
    print(f"    - Order Items: {items_std.count():,} filas\n")

    return customers_std, products_std, orders_std, items_std


def segregate_and_quarantine(customers_clean, products_std, orders_std, items_std):
    """
    Fase 2 (Continuación): Segregación explícita de calidad y persistencia en cuarentena.
    Regla de oro: NO descartar filas silenciosamente; añadir columna quarantine_reason.
    """
    print("=" * 80)
    print("3. SEGREGACION DE CALIDAD Y EXPORTACION A CUARENTENA")
    print("=" * 80)

    # --- 1. PRODUCTOS ---
    cond_prod_quarantine = F.col("unit_price").isNull() | (F.col("unit_price") <= 0)
    products_quarantine = (
        products_std
        .filter(cond_prod_quarantine)
        .withColumn(
            "quarantine_reason",
            F.when(F.col("unit_price").isNull(), "unit_price is null")
             .otherwise("unit_price <= 0")
        )
    )
    products_clean = products_std.filter(~cond_prod_quarantine)

    products_quarantine.write.mode("overwrite").parquet("output/quarantine/products/")
    print(f"[*] Cuarentena Products: {products_quarantine.count():,} filas guardadas en output/quarantine/products/")

    # --- 2. PEDIDOS (ORDERS) ---
    orders_joined_cust = orders_std.join(
        customers_clean.select(F.col("customer_id").alias("_valid_cid")),
        orders_std.customer_id == F.col("_valid_cid"),
        "left"
    )

    cond_ord_cust_null = F.col("customer_id").isNull()
    cond_ord_cust_orphan = F.col("_valid_cid").isNull()
    cond_ord_ts_null = F.col("order_ts").isNull()
    cond_orders_quarantine = cond_ord_cust_null | cond_ord_cust_orphan | cond_ord_ts_null

    orders_quarantine = (
        orders_joined_cust
        .filter(cond_orders_quarantine)
        .withColumn(
            "quarantine_reason",
            F.when(cond_ord_cust_null, "customer_id is null")
             .when(cond_ord_cust_orphan, "customer_id not found in clean customers")
             .when(cond_ord_ts_null, "order_ts is null")
             .otherwise("invalid order attribute")
        )
        .drop("_valid_cid")
    )
    orders_clean = (
        orders_joined_cust
        .filter(~cond_orders_quarantine)
        .drop("_valid_cid")
    )

    orders_quarantine.write.mode("overwrite").parquet("output/quarantine/orders/")
    print(f"[*] Cuarentena Orders:   {orders_quarantine.count():,} filas guardadas en output/quarantine/orders/")

    # --- 3. ITEMS DE PEDIDO (ORDER_ITEMS) ---
    items_joined_refs = (
        items_std
        .join(
            orders_clean.select(F.col("order_id").alias("_valid_oid")),
            items_std.order_id == F.col("_valid_oid"),
            "left"
        )
        .join(
            products_clean.select(F.col("product_id").alias("_valid_pid")),
            items_std.product_id == F.col("_valid_pid"),
            "left"
        )
    )

    cond_item_qty_invalid = F.col("quantity").isNull() | (F.col("quantity") <= 0)
    cond_item_disc_invalid = F.col("discount_pct").isNull() | (F.col("discount_pct") < 0) | (F.col("discount_pct") > 1)
    cond_item_order_orphan = F.col("_valid_oid").isNull()
    cond_item_prod_orphan = F.col("_valid_pid").isNull()

    cond_items_quarantine = (
        cond_item_qty_invalid |
        cond_item_disc_invalid |
        cond_item_order_orphan |
        cond_item_prod_orphan
    )

    items_quarantine = (
        items_joined_refs
        .filter(cond_items_quarantine)
        .withColumn(
            "quarantine_reason",
            F.concat_ws("; ",
                F.when(cond_item_qty_invalid, "quantity <= 0 or null"),
                F.when(cond_item_disc_invalid, "discount_pct out of range [0, 1]"),
                F.when(cond_item_order_orphan, "order_id not found in clean orders"),
                F.when(cond_item_prod_orphan, "product_id not found in clean products")
            )
        )
        .drop("_valid_oid", "_valid_pid")
    )

    items_clean = (
        items_joined_refs
        .filter(~cond_items_quarantine)
        .drop("_valid_oid", "_valid_pid")
    )

    items_quarantine.write.mode("overwrite").parquet("output/quarantine/order_items/")
    print(f"[*] Cuarentena Items:    {items_quarantine.count():,} filas guardadas en output/quarantine/order_items/\n")

    return products_clean, orders_clean, items_clean


def build_sales_detail(items_clean, orders_clean, customers_clean, products_clean):
    """
    Fase 3: Uniones garantizando la granularidad de línea de pedido y cálculo del importe neto.
    net_amount = round(quantity * unit_price * (1 - discount_pct), 2)
    """
    print("=" * 80)
    print("4. JOINS Y CALCULO DEL IMPORTE NETO (FASE 3)")
    print("=" * 80)

    # Tabla base a nivel granular de línea de pedido (order_items)
    sales_detail_clean = (
        items_clean
        .join(
            orders_clean.select("order_id", "customer_id", "order_ts", "status", "channel", "updated_at"),
            on="order_id",
            how="inner"
        )
        .join(
            customers_clean.select("customer_id", "first_name", "last_name", "email", "country", "signup_date", "segment"),
            on="customer_id",
            how="inner"
        )
        .join(
            products_clean.select("product_id", "product_name", "category", "unit_price", "active"),
            on="product_id",
            how="inner"
        )
        .withColumn(
            "net_amount",
            F.round(
                F.col("quantity") * F.col("unit_price") * (F.lit(1.0) - F.col("discount_pct")),
                2
            )
        )
    )

    sales_detail_clean.write.mode("overwrite").parquet("output/sales_detail_clean/")
    total_lines = sales_detail_clean.count()
    print(f"[*] Detalle de ventas limpio: {total_lines:,} registros exportados a output/sales_detail_clean/\n")

    return sales_detail_clean


def aggregate_monthly_sales(sales_detail_clean):
    """
    Fase 4: Agregación Analítica de Ventas Netas Mensuales.
    Agrupación: year_month, country, channel, category.
    """
    print("=" * 80)
    print("5. AGREGACION ANALITICA MENSUAL (FASE 4)")
    print("=" * 80)

    sales_monthly_summary = (
        sales_detail_clean
        .withColumn("year_month", F.date_format("order_ts", "yyyy-MM"))
        .groupBy("year_month", "country", "channel", "category")
        .agg(
            F.round(F.sum("net_amount"), 2).alias("total_net_amount"),
            F.sum("quantity").alias("total_quantity"),
            F.count("line_id").alias("total_lines")
        )
        .orderBy("year_month", "country", "channel", "category")
    )

    sales_monthly_summary.write.mode("overwrite").parquet("output/sales_monthly_summary/")
    total_groups = sales_monthly_summary.count()
    print(f"[*] Resumen mensual analítico: {total_groups:,} grupos exportados a output/sales_monthly_summary/\n")

    return sales_monthly_summary


def verify_financial_reconciliation(sales_detail_clean, sales_monthly_summary):
    """
    Control de Calidad: Reconciliación financiera entre detalle limpio y tabla agregada.
    """
    print("=" * 80)
    print("6. CONTROL DE CALIDAD: RECONCILIACION FINANCIERA (QA CHECK)")
    print("=" * 80)

    sum_detail = sales_detail_clean.select(F.round(F.sum("net_amount"), 2)).collect()[0][0]
    sum_summary = sales_monthly_summary.select(F.round(F.sum("total_net_amount"), 2)).collect()[0][0]
    diff = round(abs(sum_detail - sum_summary), 2)

    print(f"[*] Suma total net_amount en ventas_detail:   ${sum_detail:,.2f}")
    print(f"[*] Suma total net_amount en monthly_summary: ${sum_summary:,.2f}")
    print(f"[*] Diferencia absoluta:                      ${diff:,.2f}")

    if diff == 0.0:
        print("[+] CHECK EXITOSO: Reconciliación financiera al 100% (cero discrepancia).\n")
    else:
        print(f"[!] ADVERTENCIA: Se detectó una discrepancia de ${diff:,.2f}.\n")

    print("[*] Muestra de 'sales_monthly_summary':")
    sales_monthly_summary.show(10, truncate=False)


def main():
    spark = create_spark_session()
    try:
        # 1. Ingesta
        raw_cust, raw_prod, raw_orders, raw_items = load_and_type_data(spark)

        # 2. Deduplicación y Estandarización
        customers_clean, products_std, orders_std, items_std = deduplicate_and_standardize(
            raw_cust, raw_prod, raw_orders, raw_items
        )

        # 3. Segregación y Cuarentena
        products_clean, orders_clean, items_clean = segregate_and_quarantine(
            customers_clean, products_std, orders_std, items_std
        )

        # 4. Joins y Cálculo de Importe Neto (Fase 3)
        sales_detail_clean = build_sales_detail(
            items_clean, orders_clean, customers_clean, products_clean
        )

        # 5. Agregación Analítica (Fase 4)
        sales_monthly_summary = aggregate_monthly_sales(sales_detail_clean)

        # 6. Verificación y Control de Calidad
        verify_financial_reconciliation(sales_detail_clean, sales_monthly_summary)

        print("=" * 80)
        print("PIPELINE DE FASE 3 Y FASE 4 COMPLETADO EXITOSAMENTE")
        print("=" * 80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
