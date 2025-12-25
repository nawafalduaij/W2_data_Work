# Week 2 Summary ETL + EDA

## Key findings
- **Revenue Concentration:** Saudi Arabia (SA) accounted for 100% of the revenue (145.5 SAR), while the United Arab Emirates (AE) contributed 0.0.
- **Refund Rates:** There is a significant difference in refund rates. The bootstrap analysis shows the difference in means is -1.0 (CI: -1.0, -1.0), suggesting the single AE order was a refund while SA orders were not.
- **Volume:** The dataset contains a total of 5 orders across 4 unique users.

## Definitions
- **Revenue:** Sum of `amount` for all orders (winsorized).
- **Refund rate:** The proportion of orders where `status_clean` equals "refund".
- **Time window:** Data covers the timestamp range available in the raw `orders.csv`.

## Data quality caveats
- **Missingness:** 20% of orders (1 out of 5) were missing both `quantity` and `amount`.
- **Join coverage:** 100% of orders successfully joined to a user country (no orders found with null country).
- **Outliers:** One order was flagged as an outlier in the `amount` column based on the IQR method.

## Next questions
- Why did the single order from UAE result in a refund/zero revenue?
- Can we recover the missing amounts for the incomplete orders?