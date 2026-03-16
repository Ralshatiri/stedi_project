# Validation

## Customer Trusted Validation

This section validates that the `customer_trusted` table contains only customers who agreed to share their data for research purposes.

### 1. Row Count Check

**SQL Query**
```sql
SELECT COUNT(*) FROM stedi.customer_trusted;

```

![Customer Trusted Count](screenshots/customer_trusted_count.png)

---

### 2. Null Consent Validation

**SQL Query**
```sql
SELECT COUNT(*)
FROM stedi.customer_trusted
WHERE sharewithresearchasofdate IS NULL;

```

![Customer Trusted Null Check](screenshots/customer_trusted_null_check.png)

## Accelerometer Trusted Validation

This section validates that the `accelerometer_trusted` table contains only accelerometer readings from customers who agreed to share their data for research purposes.

### 1. Row Count Check

**SQL Query**
```sql
SELECT COUNT(*) FROM stedi.accelerometer_trusted;

```

![Accelerometer Trusted Count](screenshots/accelerometer_trusted_count.png)

---

### 2. Column Validation

This query verifies that the table contains only accelerometer fields and no customer fields.

**SQL Query**
```sql
SELECT * FROM stedi.accelerometer_trusted LIMIT 5;

```

![Accelerometer Trusted Columns](screenshots/accelerometer_trusted_columns.png)

## Customers Curated Validation

This section validates that the `customers_curated` table contains only the customer records that are valid for curated processing.

### 1. Row Count Check

**SQL Query**
```sql
SELECT COUNT(*) FROM stedi.customers_curated;

```

![Customers Curated Count](screenshots/customers_curated_count.png)

---

### 2. Column Validation

This query verifies that the table contains only customer fields and no accelerometer fields.

**SQL Query**
```sql
SELECT * FROM stedi.customers_curated LIMIT 5;

```

![Customers Curated Columns](screenshots/customers_curated_columns.png)
