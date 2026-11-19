# Fase 1 y 2: Ingesta Cruda y Perfilamiento de Calidad de Datos

## 1. Objetivo
Cargar los 4 datasets históricos en formato CSV (customers, products, orders, order_items) aplicando lectura controlada (inferSchema=False, 
ullValue="NULL") y tipado explícito (F.try_cast), para diagnosticar anomalías de unicidad, integridad referencial, nulos y dominios de negocio antes de transformar los datos.

## 2. Volumetría Inicial
| Tabla | Filas Totales | Clave Primaria (PK) | PKs Únicas | Duplicados |
| :--- | :---: | :--- | :---: | :---: |
| \customers.csv\ | 1,001 | \customer_id\ | 1,000 | 1 (\C0020\) |
| \products.csv\ | 100 | \product_id\ | 100 | 0 |
| \orders.csv\ | 20,001 | \order_id\ | 20,000 | 1 (\O00012\) |
| \order_items.csv\ | 40,026 | \line_id\ | 40,026 | 0 |

## 3. Anomalías Detectadas y Estrategia de Tratamiento
1. **Duplicados en Claves Primarias:**
   - \customers\: \C0020\ repetido 2 veces -> *Decisión:* Deduplicar conservando el registro más completo.
   - \orders\: \O00012\ repetido 2 veces -> *Decisión:* Deduplicar conservando el registro más reciente por \updated_at\.
2. **Nulos Literales y Mal Formados:**
   - Cadenas literales \"NULL"\ en fechas (\signup_date\) y atributos (\email\ en \C0013\, \customer_id\ en \O00011\) -> *Decisión:* Tolerar con \	ry_cast\, imputar \email\ no crítico y enviar pedidos sin cliente a cuarentena.
3. **Error Tipográfico en Dominio (\status\):**
   - Valor \'deliverd'\ en \orders\ -> *Decisión:* Estandarizar a \'delivered'\.
4. **Valores Fuera de Rango:**
   - \products\: \P088\ con \unit_price = -12.50\ y \P089\ con precio nulo -> *Decisión:* Enviar a cuarentena.
   - \order_items\: \L0000001\ (\quantity = 0\), \L0000002\ (\quantity = -2\) y \L0000005\ (\discount_pct = 1.20\) -> *Decisión:* Enviar a cuarentena.
5. **Integridad Referencial (Registros Huérfanos):**
   - \orders\: \O00007\ referencia \customer_id = C9999\ inexistente -> *Decisión:* Enviar a cuarentena.
   - \order_items\: \L0000003\ referencia \O99999\ y \L0000004\ referencia \P999\ -> *Decisión:* Enviar a cuarentena.
