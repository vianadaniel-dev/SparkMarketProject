import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnull, trim, countDistinct
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, TimestampType, DateType, BooleanType
)


def create_spark_session():
    """
    Inicializa la sesion local de Spark optimizada para el taller.
    """
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("01-profiling-taller-data-engineer")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_raw_data(spark):
    """
    Fase 1: Ingesta en crudo.
    Lee los 4 CSV con header=True e inferSchema=False para evitar inferencias automaticas.
    """
    print("=" * 80)
    print("FASE 1: INGESTA DE ARCHIVOS CSV EN CRUDO (inferSchema=False)")
    print("=" * 80)
    
    raw_customers = spark.read.option("header", True).option("inferSchema", False).csv("datos/customers.csv")
    raw_products = spark.read.option("header", True).option("inferSchema", False).csv("datos/products.csv")
    raw_orders = spark.read.option("header", True).option("inferSchema", False).csv("datos/orders.csv")
    raw_order_items = spark.read.option("header", True).option("inferSchema", False).csv("datos/order_items.csv")
    
    print(f"[*] Registros cargados:")
    print(f"    - Customers:   {raw_customers.count():,} filas")
    print(f"    - Products:    {raw_products.count():,} filas")
    print(f"    - Orders:      {raw_orders.count():,} filas")
    print(f"    - Order Items: {raw_order_items.count():,} filas\n")
    
    return raw_customers, raw_products, raw_orders, raw_order_items


def clean_null_string(c):
    """
    Normaliza valores ausentes textuales ('NULL', vacios) a un valor nulo real de SQL.
    """
    return when(isnull(col(c)) | (trim(col(c)) == "") | (col(c) == "NULL"), None).otherwise(col(c))


def apply_explicit_types(raw_customers, raw_products, raw_orders, raw_order_items):
    """
    Fase 1 (Continuacion): Tipado explicito de columnas clave.
    Convierte cadenas a fechas, timestamps, enteros y flotantes/doubles.
    """
    print("=" * 80)
    print("FASE 1: APLICACION DE CASTS Y TIPADO EXPLICITO")
    print("=" * 80)
    
    # 1. Clientes: Normalizar email y castear signup_date a DateType
    typed_customers = (
        raw_customers
        .withColumn("email", clean_null_string("email"))
        .withColumn("signup_date", clean_null_string("signup_date").cast(DateType()))
    )
    
    # 2. Productos: unit_price a DoubleType, active a BooleanType
    typed_products = (
        raw_products
        .withColumn("unit_price", clean_null_string("unit_price").cast(DoubleType()))
        .withColumn("active", clean_null_string("active").cast(BooleanType()))
    )
    
    # 3. Pedidos: customer_id normalizado, order_ts y updated_at a TimestampType
    typed_orders = (
        raw_orders
        .withColumn("customer_id", clean_null_string("customer_id"))
        .withColumn("order_ts", clean_null_string("order_ts").cast(TimestampType()))
        .withColumn("updated_at", clean_null_string("updated_at").cast(TimestampType()))
    )
    
    # 4. Items de pedido: quantity a IntegerType, discount_pct a DoubleType
    typed_order_items = (
        raw_order_items
        .withColumn("quantity", clean_null_string("quantity").cast(IntegerType()))
        .withColumn("discount_pct", clean_null_string("discount_pct").cast(DoubleType()))
    )
    
    print("[*] Esquema de Customers tipado:")
    typed_customers.printSchema()
    
    print("[*] Esquema de Products tipado:")
    typed_products.printSchema()
    
    print("[*] Esquema de Orders tipado:")
    typed_orders.printSchema()
    
    print("[*] Esquema de Order Items tipado:")
    typed_order_items.printSchema()
    
    return typed_customers, typed_products, typed_orders, typed_order_items


def profile_primary_keys(typed_customers, typed_products, typed_orders, typed_order_items):
    """
    Fase 2.1: Perfilamiento de Claves Primarias (Unicidad).
    """
    print("\n" + "=" * 80)
    print("FASE 2.1: PERFILAMIENTO DE UNICIDAD EN CLAVES PRIMARIAS (PK)")
    print("=" * 80)
    
    tables = [
        ("Customers", typed_customers, "customer_id"),
        ("Products", typed_products, "product_id"),
        ("Orders", typed_orders, "order_id"),
        ("Order Items", typed_order_items, "line_id"),
    ]
    
    for name, df, pk in tables:
        total = df.count()
        distinct_pks = df.select(pk).distinct().count()
        duplicate_count = total - distinct_pks
        print(f"[*] Tabla '{name}': Total={total} | Distinct PKs={distinct_pks} | Duplicados={duplicate_count}")
        if duplicate_count > 0:
            print(f"    [!] ALERTA: Se detectaron claves duplicadas en '{name}' ({pk}):")
            dups = df.groupBy(pk).count().filter("count > 1")
            dups.show(truncate=False)


def profile_nulls_and_blanks(typed_customers, typed_products, typed_orders, typed_order_items):
    """
    Fase 2.2: Perfilamiento de Nulos y Valores Vacios por Columna.
    """
    print("=" * 80)
    print("FASE 2.2: PERFILAMIENTO DE NULOS Y CAMPOS VACIOS")
    print("=" * 80)
    
    tables = [
        ("Customers", typed_customers),
        ("Products", typed_products),
        ("Orders", typed_orders),
        ("Order Items", typed_order_items),
    ]
    
    for name, df in tables:
        print(f"\n[*] Conteo de nulos en '{name}':")
        exprs = []
        for c in df.columns:
            # Si es tipo String comprobamos tambien espacios vacios
            if isinstance(df.schema[c].dataType, StringType):
                exprs.append(count(when(isnull(col(c)) | (trim(col(c)) == ""), c)).alias(c))
            else:
                exprs.append(count(when(isnull(col(c)), c)).alias(c))
        df.select(exprs).show()


def profile_domain_and_business_rules(typed_products, typed_orders, typed_order_items):
    """
    Fase 2.3: Perfilamiento de Reglas de Negocio y Dominios de Valores.
    """
    print("=" * 80)
    print("FASE 2.3: PERFILAMIENTO DE DOMINIO Y REGLAS DE NEGOCIO")
    print("=" * 80)
    
    # 1. Productos: Precios invalidos (<= 0 o nulos)
    invalid_prices = typed_products.filter((col("unit_price") <= 0) | isnull(col("unit_price")))
    print(f"[*] Productos con unit_price <= 0 o nulo ({invalid_prices.count()} filas):")
    invalid_prices.show(truncate=False)
    
    # 2. Orders: Estados distintos y posibles typos
    print("[*] Valores distintos en status de Orders (deteccion de typos):")
    typed_orders.select("status").distinct().show(truncate=False)
    
    # 3. Order Items: Cantidad fuera de rango (quantity <= 0)
    invalid_qty = typed_order_items.filter((col("quantity") <= 0) | isnull(col("quantity")))
    print(f"[*] Order Items con quantity <= 0 o nulo ({invalid_qty.count()} filas):")
    invalid_qty.show(truncate=False)
    
    # 4. Order Items: Descuentos fuera de rango (< 0 o > 1.0)
    invalid_disc = typed_order_items.filter(
        (col("discount_pct") < 0.0) | (col("discount_pct") > 1.0) | isnull(col("discount_pct"))
    )
    print(f"[*] Order Items con discount_pct fuera del rango [0.0, 1.0] ({invalid_disc.count()} filas):")
    invalid_disc.show(truncate=False)


def profile_referential_integrity(typed_customers, typed_products, typed_orders, typed_order_items):
    """
    Fase 2.4: Perfilamiento de Integridad Referencial (Claves Foraneas Huerfanas).
    """
    print("=" * 80)
    print("FASE 2.4: PERFILAMIENTO DE INTEGRIDAD REFERENCIAL (FK HUERFANAS)")
    print("=" * 80)
    
    # 1. Orders -> Customers (orders.customer_id sin match en customers)
    orphan_cust_orders = typed_orders.join(typed_customers, on="customer_id", how="left_anti")
    print(f"[*] Orders con customer_id huerfano o nulo ({orphan_cust_orders.count()} filas):")
    orphan_cust_orders.show(truncate=False)
    
    # 2. Order Items -> Orders (order_items.order_id sin match en orders)
    orphan_order_items = typed_order_items.join(typed_orders, on="order_id", how="left_anti")
    print(f"[*] Order Items con order_id huerfano ({orphan_order_items.count()} filas):")
    orphan_order_items.show(truncate=False)
    
    # 3. Order Items -> Products (order_items.product_id sin match en products)
    orphan_prod_items = typed_order_items.join(typed_products, on="product_id", how="left_anti")
    print(f"[*] Order Items con product_id huerfano ({orphan_prod_items.count()} filas):")
    orphan_prod_items.show(truncate=False)


def main():
    spark = create_spark_session()
    try:
        # Ingesta en crudo
        raw_cust, raw_prod, raw_ord, raw_items = load_raw_data(spark)
        
        # Tipado explicito
        typed_cust, typed_prod, typed_ord, typed_items = apply_explicit_types(
            raw_cust, raw_prod, raw_ord, raw_items
        )
        
        # Perfilamiento sistematico
        profile_primary_keys(typed_cust, typed_prod, typed_ord, typed_items)
        profile_nulls_and_blanks(typed_cust, typed_prod, typed_ord, typed_items)
        profile_domain_and_business_rules(typed_prod, typed_ord, typed_items)
        profile_referential_integrity(typed_cust, typed_prod, typed_ord, typed_items)
        
        print("\n" + "=" * 80)
        print("PERFILAMIENTO EXPLORATORIO COMPLETADO EXITOSAMENTE")
        print("=" * 80)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
