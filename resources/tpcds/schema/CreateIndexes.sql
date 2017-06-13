-- indexes
CREATE INDEX cs_item_order_number_idx ON catalog_sales ( cs_item_sk, cs_order_number);

CREATE INDEX cr_item_order_number_idx ON catalog_returns ( cr_item_sk, cr_order_number);

CREATE INDEX inv_date_item_warehouse_cluidx ON inventory ( inv_date_sk, inv_item_sk, inv_warehouse_sk );

CREATE INDEX ss_item_ticket_number_idx ON store_sales (ss_item_sk, ss_ticket_number);

CREATE INDEX sr_item_ticket_number_idx ON store_returns (sr_item_sk, sr_ticket_number);

CREATE INDEX ws_item_order_number_idx ON web_sales ( ws_item_sk, ws_order_number);

CREATE INDEX wr_item_order_number_idx ON web_returns ( wr_item_sk, wr_order_number);

-- additional indexes for performance
CREATE INDEX cs_sold_date_sk_cluidx ON catalog_sales(cs_sold_date_sk); 

CREATE INDEX cr_returned_date_cluidx ON catalog_returns(cr_returned_date_sk); 

CREATE INDEX ss_sold_date_sk_cluidx ON store_sales(ss_sold_date_sk); 

CREATE INDEX sr_returned_date_cluidx ON store_returns(sr_returned_date_sk); 

CREATE INDEX ws_sold_date_sk_cluidx ON web_sales(ws_sold_date_sk); 

CREATE INDEX wr_returnd_date_cluidx ON web_returns(wr_returned_date_sk); 

-- primary keys
alter table store add constraint pk_s_store_sk primary key ( s_store_sk );

alter table call_center add constraint pk_cc_call_center_sk primary key ( cc_call_center_sk );

alter table catalog_page add constraint pk_cp_catalog_page_sk primary key ( cp_catalog_page_sk );

alter table web_site add constraint pk_web_site_sk primary key ( web_site_sk );

alter table web_page add constraint pk_wp_web_page_sk primary key ( wp_web_page_sk );

alter table warehouse add constraint pk_w_warehouse_sk primary key ( w_warehouse_sk );

alter table customer add constraint pk_c_customer_sk primary key ( c_customer_sk );

alter table customer_address add constraint pk_ca_address_sk primary key ( ca_address_sk );

alter table customer_demographics add constraint pk_cd_demo_sk primary key ( cd_demo_sk );

alter table date_dim add constraint pk_d_date_sk primary key ( d_date_sk );

alter table household_demographics add constraint pk_hd_demo_sk primary key ( hd_demo_sk );

alter table item add constraint pk_i_item_sk primary key ( i_item_sk );

alter table income_band add constraint pk_ib_income_band_sk primary key ( ib_income_band_sk );

alter table promotion add constraint pk_p_promo_sk primary key ( p_promo_sk );

alter table reason add constraint pk_r_reason_sk primary key ( r_reason_sk );

alter table ship_mode add constraint pk_sm_ship_mode_sk primary key ( sm_ship_mode_sk );

alter table time_dim add constraint pk_t_time_sk primary key ( t_time_sk );




