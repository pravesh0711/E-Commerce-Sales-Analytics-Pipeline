# Power BI Dashboard: E-Commerce Sales Analytics

Build time: about 45 minutes in Power BI Desktop (free, Windows). Everything below is tested against your pipeline's output.

## 1. Load the data (pick ONE)

**A. Quick start (CSV).** Home > Get Data > Text/CSV, and load all six files in `data/`:
`fact_sales`, `fact_feedback`, `dim_customers`, `dim_products`, `dim_stores`, `dim_promotions`.
Set `transaction_date` (fact_sales), `date` (fact_feedback), `signup_date` and `opened_date` to type **Date**.

**B. Real pipeline (BigQuery).** In the notebook add `to_bigquery(silver, "fact_sales")` to the BigQuery cell, then
Get Data > Google BigQuery > project `gen-lang-client-0000887766` > dataset `ecommerce`.
Use **Import** mode. Sign in with your Google account (do not use the service-account key in Power BI).

## 2. Date table (Modeling > New table)
```DAX
dim_date =
ADDCOLUMNS(
    CALENDAR(DATE(2023,1,1), DATE(2026,12,31)),
    "Year", YEAR([Date]),
    "Quarter", "Q" & QUARTER([Date]),
    "Year-Quarter", YEAR([Date]) & "-Q" & QUARTER([Date]),
    "Month No", MONTH([Date]),
    "Month", FORMAT([Date], "MMM")
)
```
Select the table, then Table tools > **Mark as date table** (column `Date`). Sort `Month` by `Month No`.

## 3. Model view: relationships (all many-to-one, single direction)

| From (many) | To (one) |
|---|---|
| fact_sales[customer_id] | dim_customers[customer_id] |
| fact_sales[product_id] | dim_products[product_id] |
| fact_sales[store_id] | dim_stores[store_id] |
| fact_sales[transaction_date] | dim_date[Date] |
| fact_feedback[product_id] | dim_products[product_id] |
| fact_feedback[customer_id] | dim_customers[customer_id] |
| fact_feedback[date] | dim_date[Date] |
| dim_promotions[product_id] | dim_products[product_id] |

Hide the key columns in the fact tables (right-click > Hide in report view).

## 4. Measures (create a table "_Measures": Home > Enter Data, empty, then add these)
```DAX
Net Revenue      = SUM(fact_sales[final_amount])
Gross Revenue    = SUM(fact_sales[total_amount])
Discount Given   = SUM(fact_sales[discount_amount])
Orders           = DISTINCTCOUNT(fact_sales[transaction_id])
Units Sold       = SUM(fact_sales[quantity])
Avg Order Value  = DIVIDE([Net Revenue], [Orders])
Active Customers = DISTINCTCOUNT(fact_sales[customer_id])
Avg Rating       = AVERAGE(fact_feedback[rating])
Promo Order %    = DIVIDE(CALCULATE([Orders], fact_sales[promo_applied] = TRUE()), [Orders])
Net Revenue LY   = CALCULATE([Net Revenue], SAMEPERIODLASTYEAR(dim_date[Date]))
Revenue YoY %    = DIVIDE([Net Revenue] - [Net Revenue LY], [Net Revenue LY])
Revenue Share %  = DIVIDE([Net Revenue], CALCULATE([Net Revenue], ALLSELECTED()))
Product Rank     = RANKX(ALLSELECTED(dim_products[product_name]), [Net Revenue])
```
Format Net Revenue / AOV as whole numbers with thousands separator; the % measures as percentage.

## 5. Theme
View > Themes > Browse for themes > pick `ecommerce_theme.json`.

## 6. Page layout (16:9, Canvas background #F3F6FB)

**Row 1, header:** text box "E-Commerce Sales Analytics" (Segoe UI Semibold 24) plus subtitle "Databricks, Delta Lake, BigQuery".

**Row 2, slicers (dropdown style):** `dim_date[Year]`, `dim_products[category]`, `dim_customers[region]`, `dim_customers[segment]`.

**Row 3, six KPI cards:** Net Revenue, Orders, Avg Order Value, Active Customers, Discount Given, Avg Rating.

**Row 4:**
- Line/area chart: X = `dim_date[Year-Quarter]`, Y = Net Revenue. Add Revenue YoY % as a tooltip.
- Clustered bar: Y = `dim_products[category]`, X = Net Revenue (sorted descending).

**Row 5:**
- Bar: Top 10 products. Y = `dim_products[product_name]`, X = Net Revenue; Filters pane > Top N = 10 by Net Revenue.
- Map or bar: `dim_stores[location]` (city) by Net Revenue.
- Donut: `dim_customers[segment]` by Net Revenue (hide "No purchases" with a visual filter).

**Row 6:**
- Bar: `dim_products[category]` by Avg Rating (axis 0 to 5).
- Table or matrix: product_name, Units Sold, Net Revenue, Avg Rating, Product Rank.

Turn on Edit interactions so the slicers filter every visual. Use Insert > Buttons > Bookmark navigator for a second "Customer & Promo" page if you want.

## 7. Verify against the pipeline (all must match)
- Net Revenue = **3,743,592.23** | Gross Revenue = **3,768,955.27**
- Orders = **100** | Active Customers = **68** | Products sold = **49** | Stores = **40**
- Promo Order % = **3%**
If Net Revenue is higher, a relationship is duplicating rows (check the table above).

## 8. Publish and share on LinkedIn
- Take a high-resolution screenshot (View > Full screen, then Snipping Tool) or File > Export > PDF.
- Publishing to the web with a public link is optional: Publish to Power BI service > File > Embed report > Publish to web. Only do this with sample data like this dataset.
