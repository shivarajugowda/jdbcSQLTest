--q32.sql--
-- define _LIMIT=100

 [_LIMITA] select [_LIMITB] sum(cs_ext_discount_amt) as "excess discount amount"
 from
    catalog_sales, item, date_dim
 where
   i_manufact_id = 977
   and i_item_sk = cs_item_sk
   and d_date between date'2000-01-27' and (date'2000-01-27' + interval '90' day)
   and d_date_sk = cs_sold_date_sk
   and cs_ext_discount_amt > (
          select 1.3 * avg(cs_ext_discount_amt)
          from catalog_sales, date_dim
          where cs_item_sk = i_item_sk
           and d_date between date'2000-01-27' and (date'2000-01-27' + interval '90' day)
           and d_date_sk = cs_sold_date_sk)
[_LIMITC]
            
