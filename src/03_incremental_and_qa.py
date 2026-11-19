import os
import sys

# 1. Configurar HADOOP_HOME y PATH para compatibilidad nativa en Windows
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
    Inicializa la sesion local de Spark para el pipeline incremental y QA.
    """
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("03-incremental-and-qa-taller-data-engineer")
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


def load_historical_clean_data(spark):
    """
    Carga y limpia los datos maestros e historicos base.
    """
    print("=" * 80)
    print("1. CARGA Y LIMPIEZA DE DATOS HISTORICOS BASE")
    print("=" * 80)

    # Clientes
    raw_cust = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("nullValue", "NULL")
        .csv("datos/customers.csv")
        .withColumn("signup_date", try_cast("signup_date", "date"))
    )
    customers_clean = (
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

    # Productos
    raw_prod = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("nullValue", "NULL")
        .csv("datos/products.csv")
        .withColumn("unit_price", try_cast("unit_price", "double"))
        .withColumn("active", try_cast("active", "boolean"))
    )
    products_std = raw_prod.dropDuplicates(["product_id"])
    cond_prod_valid = F.col("unit_price").isNotNull() & (F.col("unit_price") > 0)
    products_clean = products_std.filter(cond_prod_valid)

    # Ordenes historicas
    raw_orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("nullValue", "NULL")
        .csv("datos/orders.csv")
        .withColumn("order_ts", try_cast("order_ts", "timestamp"))
        .withColumn("updated_at", try_cast("updated_at", "timestamp"))
        .withColumn(
            "status",
            F.when(F.trim(F.lower(F.col("status"))) == "deliverd", "delivered")
             .otherwise(F.trim(F.lower(F.col("status"))))
        )
    )
    w_ord = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    orders_std = (
        raw_orders
        .withColumn("rn", F.row_number().over(w_ord))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    # Filtro de calidad ordenes historicas
    orders_joined_cust = orders_std.join(
        customers_clean.select(F.col("customer_id").alias("_valid_cid")),
        orders_std.customer_id == F.col("_valid_cid"),
        "left"
    )
    cond_orders_clean = (
        F.col("customer_id").isNotNull() &
        F.col("_valid_cid").isNotNull() &
        F.col("order_ts").isNotNull()
    )
    orders_clean = orders_joined_cust.filter(cond_orders_clean).drop("_valid_cid")

    # Items historicos
    raw_items = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("nullValue", "NULL")
        .csv("datos/order_items.csv")
        .withColumn("quantity", try_cast("quantity", "int"))
        .withColumn("discount_pct", try_cast("discount_pct", "double"))
    )
    items_std = raw_items.dropDuplicates(["line_id"])
    items_joined_refs = (
        items_std
        .join(orders_clean.select(F.col("order_id").alias("_valid_oid")), items_std.order_id == F.col("_valid_oid"), "left")
        .join(products_clean.select(F.col("product_id").alias("_valid_pid")), items_std.product_id == F.col("_valid_pid"), "left")
    )
    cond_items_clean = (
        F.col("quantity").isNotNull() & (F.col("quantity") > 0) &
        F.col("discount_pct").isNotNull() & (F.col("discount_pct") >= 0) & (F.col("discount_pct") <= 1) &
        F.col("_valid_oid").isNotNull() &
        F.col("_valid_pid").isNotNull()
    )
    items_clean = items_joined_refs.filter(cond_items_clean).drop("_valid_oid", "_valid_pid")

    print(f"[*] Historicos limpios cargados:")
    print(f"    - Clientes:   {customers_clean.count():,} filas")
    print(f"    - Productos:  {products_clean.count():,} filas")
    print(f"    - Ordenes:    {orders_clean.count():,} filas")
    print(f"    - Items:      {items_clean.count():,} filas\n")

    return customers_clean, products_clean, orders_clean, items_clean


def ingest_and_segregate_incremental(spark, customers_clean, products_clean):
    """
    Fase 5: Ingesta del lote incremental en JSON Lines y segregacion de calidad.
    """
    print("=" * 80)
    print("2. INGESTA Y SEGREGACION DEL LOTE INCREMENTAL (JSON LINES)")
    print("=" * 80)

    # 1. Cargar Orders Incremental
    raw_inc_orders = (
        spark.read
        .json("datos/incremental/orders_incremental.jsonl")
        .withColumn("order_ts", try_cast("order_ts", "timestamp"))
        .withColumn("updated_at", try_cast("updated_at", "timestamp"))
        .withColumn(
            "status",
            F.when(F.trim(F.lower(F.col("status"))) == "deliverd", "delivered")
             .otherwise(F.trim(F.lower(F.col("status"))))
        )
    )

    # Validar integridad referencial y nulos en orders
    inc_ord_joined = raw_inc_orders.join(
        customers_clean.select(F.col("customer_id").alias("_cid")),
        raw_inc_orders.customer_id == F.col("_cid"),
        "left"
    )
    cond_inc_ord_valid = (
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("_cid").isNotNull() &
        F.col("order_ts").isNotNull()
    )
    inc_orders_clean = inc_ord_joined.filter(cond_inc_ord_valid).drop("_cid")
    inc_orders_quarantine = (
        inc_ord_joined
        .filter(~cond_inc_ord_valid)
        .withColumn("quarantine_reason", F.lit("invalid customer or timestamp in incremental order"))
        .drop("_cid")
    )

    # 2. Cargar Order Items Incremental
    raw_inc_items = (
        spark.read
        .json("datos/incremental/order_items_incremental.jsonl")
        .withColumn("quantity", try_cast("quantity", "int"))
        .withColumn("discount_pct", try_cast("discount_pct", "double"))
    )

    # Validar integridad referencial con productos y órdenes válidas
    inc_items_joined = (
        raw_inc_items
        .join(products_clean.select(F.col("product_id").alias("_pid")), raw_inc_items.product_id == F.col("_pid"), "left")
    )
    cond_inc_items_valid = (
        F.col("line_id").isNotNull() &
        F.col("order_id").isNotNull() &
        F.col("product_id").isNotNull() &
        F.col("_pid").isNotNull() &
        F.col("quantity").isNotNull() & (F.col("quantity") > 0) &
        F.col("discount_pct").isNotNull() & (F.col("discount_pct") >= 0) & (F.col("discount_pct") <= 1)
    )
    inc_items_clean = inc_items_joined.filter(cond_inc_items_valid).drop("_pid")
    inc_items_quarantine = (
        inc_items_joined
        .filter(~cond_inc_items_valid)
        .withColumn(
            "quarantine_reason",
            F.when(F.col("_pid").isNull(), "product_id not found in clean products")
             .otherwise("invalid quantity or discount in incremental item")
        )
        .drop("_pid")
    )

    count_inc_orders_clean = inc_orders_clean.count()
    count_inc_orders_quar = inc_orders_quarantine.count()
    count_inc_items_clean = inc_items_clean.count()
    count_inc_items_quar = inc_items_quarantine.count()

    print(f"[*] Ingesta Incremental procesada:")
    print(f"    - Ordenes Incremental: {count_inc_orders_clean:,} limpias | {count_inc_orders_quar:,} en cuarentena")
    print(f"    - Items Incremental:   {count_inc_items_clean:,} limpios | {count_inc_items_quar:,} en cuarentena\n")

    return inc_orders_clean, inc_orders_quarantine, inc_items_clean, inc_items_quarantine


def merge_and_upsert_idempotent(orders_clean, items_clean, inc_orders_clean, inc_items_clean):
    """
    Fase 5: Upsert/Merge idempotente combinando historico e incremental.
    - Orders: Deduplicacion por order_id con precedencia de updated_at DESC.
    - Items: Deduplicacion por line_id garantizando idempotencia ante reprocesamiento.
    """
    print("=" * 80)
    print("3. MERGE / UPSERT IDEMPOTENTE (HISTORICO + INCREMENTAL)")
    print("=" * 80)

    # 1. Unir y deduplicar pedidos (Orders)
    # Etiquetar prioridad: incremental tiene precedencia en caso de empate en updated_at
    orders_hist_tagged = orders_clean.withColumn("_source_priority", F.lit(1))
    orders_inc_tagged = inc_orders_clean.withColumn("_source_priority", F.lit(2))

    all_orders = orders_hist_tagged.unionByName(orders_inc_tagged)
    w_orders_upsert = (
        Window
        .partitionBy("order_id")
        .orderBy(F.col("updated_at").desc(), F.col("_source_priority").desc())
    )

    merged_orders = (
        all_orders
        .withColumn("row_num", F.row_number().over(w_orders_upsert))
        .filter(F.col("row_num") == 1)
        .drop("row_num", "_source_priority")
    )

    # 2. Unir y deduplicar items
    items_hist_tagged = items_clean.withColumn("_source_priority", F.lit(1))
    items_inc_tagged = inc_items_clean.withColumn("_source_priority", F.lit(2))

    all_items = items_hist_tagged.unionByName(items_inc_tagged)
    w_items_upsert = (
        Window
        .partitionBy("line_id")
        .orderBy(F.col("_source_priority").desc())
    )

    merged_items = (
        all_items
        .withColumn("row_num", F.row_number().over(w_items_upsert))
        .filter(F.col("row_num") == 1)
        .drop("row_num", "_source_priority")
    )

    # Filtrar items cuyos pedidos esten validos en merged_orders
    merged_items = merged_items.join(
        merged_orders.select(F.col("order_id").alias("_m_oid")),
        merged_items.order_id == F.col("_m_oid"),
        "inner"
    ).drop("_m_oid")

    return merged_orders, merged_items


def build_pipeline_tables(merged_items, merged_orders, customers_clean, products_clean):
    """
    Construye las tablas finales 'sales_detail_clean' y 'sales_monthly_summary'.
    """
    sales_detail_clean = (
        merged_items
        .join(
            merged_orders.select("order_id", "customer_id", "order_ts", "status", "channel", "updated_at"),
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

    return sales_detail_clean, sales_monthly_summary


def run_qa_validations(sales_detail_clean, sales_monthly_summary, merged_orders, merged_items,
                       orders_clean, items_clean, inc_orders_clean, inc_items_clean,
                       customers_clean, products_clean):
    """
    Fase 6: 3 Controles de Calidad Automatizados con 'assert' estrictos.
    """
    print("=" * 80)
    print("4. CONTROLES DE CALIDAD AUTOMATIZADOS (FASE 6 - QA SUITE)")
    print("=" * 80)

    # -------------------------------------------------------------------------
    # QA Check 1: Reconciliacion Financiera Detalle vs Resumen Mensual
    # -------------------------------------------------------------------------
    sum_detail = sales_detail_clean.select(F.round(F.sum("net_amount"), 2)).collect()[0][0]
    sum_summary = sales_monthly_summary.select(F.round(F.sum("total_net_amount"), 2)).collect()[0][0]
    financial_diff = round(abs(sum_detail - sum_summary), 2)

    print(f"[*] [QA Check 1] Reconciliacion Financiera:")
    print(f"    - Suma Detalle (sales_detail_clean):     ${sum_detail:,.2f}")
    print(f"    - Suma Resumen (sales_monthly_summary): ${sum_summary:,.2f}")
    print(f"    - Diferencia absoluta:                  ${financial_diff:,.2f}")

    assert financial_diff < 0.01, f"ERROR QA 1: Discrepancia financiera detectada: ${financial_diff}"
    print("    [+] QA CHECK 1 APROBADO: Reconciliacion perfecta (diff < 0.01).\n")

    # -------------------------------------------------------------------------
    # QA Check 2: Unicidad e Integridad Referencial Post-Incremental
    # -------------------------------------------------------------------------
    total_orders = merged_orders.count()
    distinct_orders = merged_orders.select("order_id").distinct().count()
    dup_orders = total_orders - distinct_orders

    total_items = merged_items.count()
    distinct_items = merged_items.select("line_id").distinct().count()
    dup_items = total_items - distinct_items

    # Nulos en columnas criticas de sales_detail_clean
    null_counts = sales_detail_clean.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in ["line_id", "order_id", "customer_id", "product_id", "net_amount"]
    ]).collect()[0].asDict()

    print(f"[*] [QA Check 2] Unicidad e Integridad de Claves:")
    print(f"    - Duplicados en order_id: {dup_orders}")
    print(f"    - Duplicados en line_id:  {dup_items}")
    print(f"    - Conteo de nulos en columnas criticas: {null_counts}")

    assert dup_orders == 0, f"ERROR QA 2: Se encontraron {dup_orders} order_id duplicados"
    assert dup_items == 0, f"ERROR QA 2: Se encontraron {dup_items} line_id duplicados"
    for col_name, null_val in null_counts.items():
        assert null_val == 0, f"ERROR QA 2: Columna critica '{col_name}' contiene {null_val} valores nulos"
    print("    [+] QA CHECK 2 APROBADO: Cero duplicados y cero nulos en atributos clave.\n")

    # -------------------------------------------------------------------------
    # QA Check 3: Prueba Real de Idempotencia (Segunda Pasada sobre Mismos Datos)
    # -------------------------------------------------------------------------
    print(f"[*] [QA Check 3] Prueba Real de Idempotencia (Segunda Pasada Incremental)...")
    pass1_lines = sales_detail_clean.count()
    pass1_amount = sum_detail

    # Simular una segunda pasada combinando el dataset ya mergeado nuevamente con el lote incremental
    pass2_orders, pass2_items = merge_and_upsert_idempotent(
        merged_orders, merged_items, inc_orders_clean, inc_items_clean
    )
    pass2_detail, _ = build_pipeline_tables(
        pass2_items, pass2_orders, customers_clean, products_clean
    )

    pass2_lines = pass2_detail.count()
    pass2_amount = pass2_detail.select(F.round(F.sum("net_amount"), 2)).collect()[0][0]
    amount_idempotent_diff = round(abs(pass1_amount - pass2_amount), 2)

    print(f"    - Pasada 1: {pass1_lines:,} lineas | Importe Total: ${pass1_amount:,.2f}")
    print(f"    - Pasada 2: {pass2_lines:,} lineas | Importe Total: ${pass2_amount:,.2f}")
    print(f"    - Diferencia en lineas:   {abs(pass1_lines - pass2_lines)}")
    print(f"    - Diferencia en importes: ${amount_idempotent_diff:,.2f}")

    assert pass1_lines == pass2_lines, f"ERROR QA 3: Fallo de idempotencia en conteo de lineas ({pass1_lines} != {pass2_lines})"
    assert amount_idempotent_diff == 0.0, f"ERROR QA 3: Fallo de idempotencia en suma monetaria (${amount_idempotent_diff})"
    print("    [+] QA CHECK 3 APROBADO: Idempotencia absoluta verificada (diff == 0).\n")

    return sum_detail, pass1_lines


def main():
    spark = create_spark_session()
    try:
        # 1. Cargar historicos limpios
        customers_clean, products_clean, orders_clean, items_clean = load_historical_clean_data(spark)

        # 2. Ingesta y segregacion del lote incremental
        (
            inc_orders_clean,
            inc_orders_quarantine,
            inc_items_clean,
            inc_items_quarantine
        ) = ingest_and_segregate_incremental(spark, customers_clean, products_clean)

        # Persistir cuarentena incremental adicional
        if inc_items_quarantine.count() > 0:
            inc_items_quarantine.write.mode("append").parquet("output/quarantine/order_items/")
            print(f"[*] Guardados {inc_items_quarantine.count()} items incrementales en output/quarantine/order_items/")

        # 3. Merge / Upsert Idempotente
        merged_orders, merged_items = merge_and_upsert_idempotent(
            orders_clean, items_clean, inc_orders_clean, inc_items_clean
        )

        total_orders_final = merged_orders.count()
        total_items_final = merged_items.count()
        print(f"[*] Datasets post-merge consolidados:")
        print(f"    - Pedidos unificados: {total_orders_final:,} (incluye actualizacion tardia O00005)")
        print(f"    - Items unificados:   {total_items_final:,}\n")

        # 4. Construir tablas analiticas finales
        print("=" * 80)
        print("5. CONSTRUCCION Y PERSISTENCIA DE TABLAS FINALES")
        print("=" * 80)
        sales_detail_clean, sales_monthly_summary = build_pipeline_tables(
            merged_items, merged_orders, customers_clean, products_clean
        )

        # 5. Ejecutar Suite de Calidad (QA) con asserts estrictos
        final_amount, final_lines = run_qa_validations(
            sales_detail_clean, sales_monthly_summary, merged_orders, merged_items,
            orders_clean, items_clean, inc_orders_clean, inc_items_clean,
            customers_clean, products_clean
        )

        # 6. Sobrescribir fisicamente las salidas en Parquet
        print("=" * 80)
        print("6. PERSISTENCIA FISICA FINAL EN PARQUET")
        print("=" * 80)
        sales_detail_clean.write.mode("overwrite").parquet("output/sales_detail_clean/")
        print(f"[*] 'output/sales_detail_clean/' sobrescrito con exito ({final_lines:,} filas).")

        sales_monthly_summary.write.mode("overwrite").parquet("output/sales_monthly_summary/")
        groups_count = sales_monthly_summary.count()
        print(f"[*] 'output/sales_monthly_summary/' sobrescrito con exito ({groups_count:,} grupos analiticos).\n")

        print("=" * 80)
        print("PIPELINE COMPLETO (FASES 1 A 6) FINALIZADO EXITOSAMENTE")
        print(f"[*] Importe Neto Consolidado: ${final_amount:,.2f}")
        print("=" * 80)

        print("\n[*] Muestra de 'sales_monthly_summary' post-incremental:")
        sales_monthly_summary.show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
