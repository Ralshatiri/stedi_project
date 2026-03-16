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
