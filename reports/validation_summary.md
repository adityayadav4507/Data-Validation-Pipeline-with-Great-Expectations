# Validation Summary

- Run timestamp (UTC): 2026-03-03T08:49:07.550434+00:00
- Engine: great_expectations
- Overall success: False
- Evaluated expectations: 17
- Successful expectations: 9
- Failed expectations: 8
- Data docs URL: data_docs/index.html

## Failed Expectations
- `expect_column_values_to_be_unique` on `order_id` -> unexpected_count=2, sample_unexpected=[1007, 1007]
- `expect_column_values_to_not_be_null` on `customer_id` -> unexpected_count=1, sample_unexpected=[None]
- `expect_column_values_to_not_be_null` on `product_id` -> unexpected_count=1, sample_unexpected=[None]
- `expect_column_values_to_not_be_null` on `order_date` -> unexpected_count=1, sample_unexpected=[None]
- `expect_column_values_to_be_between` on `quantity` -> unexpected_count=2, sample_unexpected=[0, 25]
- `expect_column_values_to_be_between` on `unit_price` -> unexpected_count=1, sample_unexpected=[-40.0]
- `expect_column_values_to_be_between` on `discount_pct` -> unexpected_count=1, sample_unexpected=[75]
- `expect_column_values_to_be_in_set` on `payment_method` -> unexpected_count=1, sample_unexpected=['crypto']
