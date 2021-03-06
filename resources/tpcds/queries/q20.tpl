--q20.sql--
-- define _LIMIT=100

[_LIMITA] select [_LIMITB] i_item_id, i_item_desc
       ,i_category
       ,i_class
       ,i_current_price
       ,sum(cs_ext_sales_price) as itemrevenue
       ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
           (partition by i_class) as revenueratio
 from catalog_sales, item, date_dim
 where cs_item_sk = i_item_sk
   and i_category in ('Sports', 'Books', 'Home')
   and cs_sold_date_sk = d_date_sk
 and d_date between date'1999-02-22'
 				and (date'1999-02-22' + interval '30' day)
 group by i_item_id, i_item_desc, i_category, i_class, i_current_price
 order by i_category, i_class, i_item_id, i_item_desc, revenueratio
 [_LIMITC]
            
