## KPI analysis

This project includes three business KPIs computed from the NovaShop warehouse (`output/warehouse.db`).

### KPI 1 — Monthly net revenue
This KPI measures the monthly net revenue after returns, based on the `sales_monthly` table.  
It also includes the month-over-month variation percentage and the margin rate.

**Main findings:**
- Revenue is relatively stable over the year, with moderate seasonality.
- The highest value appears in August 2024.
- The margin rate remains consistent, generally between 26% and 32%.

### KPI 2 — Return rate by product category
This KPI measures the return rate by product category using the `fct_sales` and `dim_product` tables.

**Main findings:**
- The highest return rate among real categories is observed for Jardin.
- Informatique shows the lowest return rate.
- The differences between categories are moderate, which suggests a fairly balanced return behavior.

### KPI 3 — Average basket by customer segment
This KPI measures the average basket per customer segment using the historical segment stored in `fct_sales.segment_at_order`.

**Main findings:**
- The Particulier segment generates the highest number of orders and the highest average basket.
- Pro and Premium segments remain important but slightly below Particulier.
- The use of SCD Type 2 ensures that the segment used in the analysis is the one valid at the time of the order.

### Conclusion
The KPI results confirm that the warehouse is correctly loaded and that the SCD2 logic is working as expected.  
The company shows stable monthly revenue, controlled return rates, and a strong contribution from the Particulier segment.