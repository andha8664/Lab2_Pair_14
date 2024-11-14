version: 2

sources:
 - name: raw_data
   database: dev
   schema: raw_data
   tables:
     - name: market_data 
       columns:
         - name: stock
           tests:
             - not_null
             - unique
         - name: date
           tests:
             - not_null
             - unique
             - dbt_utils.expression_is_true:
                 expression: "date <= current_date"
         - name: close
           tests:
             - not_null
             - dbt_utils.not_negative
             - dbt_utils.expression_is_true:
                 expression: "close > 0"