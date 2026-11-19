# Fase 3 y Fase 4: Limpieza, Transformación, Enriquecimiento y Agregación Analítica

## 1. Objetivos del Pipeline
1. **Fase 3 (Limpieza y Enriquecimiento a Nivel Detalle):**
   - Estandarizar estados y corregir errores tipográficos (`'deliverd'` → `'delivered'`).
   - Deduplicar entidades aplicando criterios de negocio (más reciente por `updated_at`).
   - Segregar anomalías a cuarentena persistente con trazabilidad (`quarantine_reason`) sin descarte silencioso.
   - Enriquecer mediante uniones (`inner joins`) garantizando la granularidad de línea de pedido sin duplicar importes.
   - Calcular el importe neto financiero exacto:
     $$\text{net\_amount} = \text{round}(\text{quantity} \times \text{unit\_price} \times (1 - \text{discount\_pct}), 2)$$

2. **Fase 4 (Agregación Analítica Mensual):**
   - Construir el datamart analítico agrupado por `year_month`, `country`, `channel` y `category`.
   - Agregar métricas de ingresos netos (`total_net_amount`), unidades vendidas (`total_quantity`) y volumen de líneas (`total_lines`).
   - Realizar la reconciliación financiera formal entre la tabla de detalle y la tabla agregada con tolerancia cero a discrepancias.

---

## 2. Volumetría y Balances de Segregación

A continuación se detalla la trazabilidad completa desde los datos crudos hasta las tablas limpias y segregadas a cuarentena:

| Entidad | Ingesta Cruda | Deduplicados / Estandarizados | Cuarentena (`output/quarantine/`) | Dataset Limpio Final | Motivo de Segregación / Corrección |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Clientes (`customers`)** | 1,001 | 1,000 | 0 | **1,000** | Deduplicado por `customer_id` (`C0020`); `C0013` imputado con `sin_email@example.com`. |
| **Productos (`products`)** | 100 | 100 | **2** | **98** | `unit_price <= 0` (`P088`: -12.50) o nulo (`P089`). |
| **Pedidos (`orders`)** | 20,001 | 20,000 | **3** | **19,997** | `customer_id` nulo (`O00011`) o no existente en clientes limpios (`O00013`, etc.). `O00012` deduplicado por `updated_at`. |
| **Líneas de Detalle (`order_items`)** | 40,026 | 40,026 | **836** | **39,190** | Cantidades `<= 0`, descuentos fuera de `[0, 1]`, o referencias huérfanas a pedidos/productos en cuarentena. |

---

## 3. Lógica de Enriquecimiento (Joins) y Prevención del Fan-Out

Para asegurar que las métricas monetarias no se inflen artificialmente al cruzar entidades:
1. **Punto Focal:** Se utiliza `order_items` limpio como la tabla dimensional motriz (grano: 1 fila por línea de pedido).
2. **Cardinalidad 1:N Garantizada:**
   - `order_items` $\bowtie$ `orders_clean` en `order_id` (1 orden tiene $N$ líneas; la orden está previamente deduplicada).
   - Resultado $\bowtie$ `customers_clean` en `customer_id` (1 cliente tiene $N$ órdenes; el cliente está previamente deduplicado).
   - Resultado $\bowtie$ `products_clean` en `product_id` (1 producto aparece en $N$ líneas; el catálogo está previamente deduplicado).
3. **Resultado:** Se obtienen exactamente **39,190 registros limpios**, preservando la unicidad de cada `line_id` sin duplicación de métricas.

---

## 4. Agregación Analítica Mensual (`sales_monthly_summary`)

La tabla agregada condensa la visión de negocio a través de 4 dimensiones:
* **Dimensiones de Corte:**
  * `year_month` (derivado de `date_format(order_ts, 'yyyy-MM')`)
  * `country` (país del cliente)
  * `channel` (canal de venta: `web`, `mobile`, `store`)
  * `category` (categoría del producto: `books`, `electronics`, `home`, `sports`)
* **Métricas Calculadas:**
  * `total_net_amount`: $\sum \text{net\_amount}$ redondeado a 2 decimales.
  * `total_quantity`: $\sum \text{quantity}$.
  * `total_lines`: $\text{count}(\text{line\_id})$.
* **Volumetría Generada:** **144 grupos analíticos** exportados a `output/sales_monthly_summary/`.

---

## 5. Control de Calidad: Reconciliación Financiera (QA Check)

Para auditar la integridad del proceso de agregación distribuida en Apache Spark, se ejecutó la validación cruzada:

$$\text{Diferencia Absoluta} = \left| \sum \text{net\_amount}_{\text{detail}} - \sum \text{total\_net\_amount}_{\text{summary}} \right|$$

* **Suma Total en `sales_detail_clean`:** **\$95,297,643.79**
* **Suma Total en `sales_monthly_summary`:** **\$95,297,643.79**
* **Discrepancia:** **\$0.00**
* **Veredicto:** **APROBADO AL 100%**. Demuestra que las transformaciones, filtros y agregaciones no perdieron ni un solo centavo de información válida.

---

## 6. Salidas Físicas Generadas en Parquet

La ejecución de `src/02_etl_cleaning.py` persistió exitosamente en disco:

```text
output/
├── quarantine/
│   ├── products/       (2 registros - Parquet Snappy + _SUCCESS)
│   ├── orders/         (3 registros - Parquet Snappy + _SUCCESS)
│   └── order_items/    (836 registros - Parquet Snappy + _SUCCESS)
├── sales_detail_clean/ (39,190 registros - Parquet Snappy + _SUCCESS)
└── sales_monthly_summary/ (144 grupos - Parquet Snappy + _SUCCESS)
```
