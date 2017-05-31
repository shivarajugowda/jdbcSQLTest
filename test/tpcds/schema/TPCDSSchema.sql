create table store_sales (
                ss_sold_date_sk          bigint,
                ss_sold_time_sk          bigint,
                ss_item_sk               bigint not null,
                ss_customer_sk           bigint,
                ss_cdemo_sk              bigint, 
                ss_hdemo_sk              bigint, 
                ss_addr_sk               bigint,
                ss_store_sk              bigint,
                ss_promo_sk              bigint,
                ss_ticket_number         bigint not null,
                ss_quantity              bigint,
                ss_wholesale_cost        decimal(7,2),
                ss_list_price            decimal(7,2),
                ss_sales_price           decimal(7,2),
                ss_ext_discount_amt      decimal(7,2),
                ss_ext_sales_price       decimal(7,2),
                ss_ext_wholesale_cost    decimal(7,2),
                ss_ext_list_price        decimal(7,2),
                ss_ext_tax               decimal(7,2),
                ss_coupon_amt            decimal(7,2),
                ss_net_paid              decimal(7,2),
                ss_net_paid_inc_tax      decimal(7,2),
                ss_net_profit            decimal(7,2));

create table store_returns (
                sr_returned_date_sk      bigint,
                sr_return_time_sk        bigint,
                sr_item_sk               bigint not null,
                sr_customer_sk           bigint,
                sr_cdemo_sk              bigint,
                sr_hdemo_sk              bigint,
                sr_addr_sk               bigint,
                sr_store_sk              bigint,
                sr_reason_sk             bigint,
                sr_ticket_number         bigint not null,
                sr_return_quantity       bigint,
                sr_return_amt            decimal(7,2),
                sr_return_tax            decimal(7,2),
                sr_return_amt_inc_tax    decimal(7,2),
                sr_fee                   decimal(7,2),
                sr_return_ship_cost      decimal(7,2),
                sr_refunded_cash         decimal(7,2),
                sr_reversed_charge       decimal(7,2),
                sr_store_credit          decimal(7,2),
                sr_net_loss              decimal(7,2));

create table catalog_sales (
                cs_sold_date_sk          bigint,
                cs_sold_time_sk          bigint,
                cs_ship_date_sk          bigint,
                cs_bill_customer_sk      bigint,
                cs_bill_cdemo_sk         bigint,
                cs_bill_hdemo_sk         bigint,
                cs_bill_addr_sk          bigint,
                cs_ship_customer_sk      bigint,
                cs_ship_cdemo_sk         bigint,
                cs_ship_hdemo_sk         bigint,
                cs_ship_addr_sk          bigint,
                cs_call_center_sk        bigint,
                cs_catalog_page_sk       bigint,
                cs_ship_mode_sk          bigint,
                cs_warehouse_sk          bigint,
                cs_item_sk               bigint not null,
                cs_promo_sk              bigint,
                cs_order_number          bigint not null,
                cs_quantity              bigint,
                cs_wholesale_cost        decimal(7,2),
                cs_list_price            decimal(7,2),
                cs_sales_price           decimal(7,2),
                cs_ext_discount_amt      decimal(7,2),
                cs_ext_sales_price       decimal(7,2),
                cs_ext_wholesale_cost    decimal(7,2),
                cs_ext_list_price        decimal(7,2),
                cs_ext_tax               decimal(7,2),
                cs_coupon_amt            decimal(7,2),
                cs_ext_ship_cost         decimal(7,2),
                cs_net_paid              decimal(7,2),
                cs_net_paid_inc_tax      decimal(7,2),
                cs_net_paid_inc_ship     decimal(7,2),
                cs_net_paid_inc_ship_tax decimal(7,2),
                cs_net_profit            decimal(7,2));

create table catalog_returns (
                cr_returned_date_sk      bigint,
                cr_return_time_sk        bigint,
                cr_item_sk               bigint not null,
                cr_refunded_customer_sk  bigint,
                cr_refunded_cdemo_sk     bigint,
                cr_refunded_hdemo_sk     bigint,
                cr_refunded_addr_sk      bigint,
                cr_returning_customer_sk bigint,
                cr_returning_cdemo_sk    bigint,
                cr_returning_hdemo_sk    bigint,
                cr_returning_addr_sk     bigint,
                cr_call_center_sk        bigint,
                cr_catalog_page_sk       bigint,
                cr_ship_mode_sk          bigint,
                cr_warehouse_sk          bigint,
                cr_reason_sk             bigint,
                cr_order_number          bigint not null,
                cr_return_quantity       bigint,
                cr_return_amount         decimal(7,2),
                cr_return_tax            decimal(7,2),
                cr_return_amt_inc_tax    decimal(7,2),
                cr_fee                   decimal(7,2),
                cr_return_ship_cost      decimal(7,2),
                cr_refunded_cash         decimal(7,2),
                cr_reversed_charge       decimal(7,2),
                cr_store_credit          decimal(7,2),
                cr_net_loss              decimal(7,2));

create table web_sales (
                ws_sold_date_sk          bigint,
                ws_sold_time_sk          bigint,
                ws_ship_date_sk          bigint,
                ws_item_sk               bigint not null,
                ws_bill_customer_sk      bigint,
                ws_bill_cdemo_sk         bigint,
                ws_bill_hdemo_sk         bigint,
                ws_bill_addr_sk          bigint,
                ws_ship_customer_sk      bigint,
                ws_ship_cdemo_sk         bigint,
                ws_ship_hdemo_sk         bigint,
                ws_ship_addr_sk          bigint,
		ws_web_page_sk           bigint,
		ws_web_site_sk           bigint,
		ws_ship_mode_sk          bigint,
		ws_warehouse_sk          bigint,
		ws_promo_sk              bigint,
                ws_order_number          bigint not null,
                ws_quantity               bigint,
                ws_wholesale_cost        decimal(7,2),
                ws_list_price            decimal(7,2),
                ws_sales_price           decimal(7,2),
                ws_ext_discount_amt      decimal(7,2),
                ws_ext_sales_price       decimal(7,2),
                ws_ext_wholesale_cost    decimal(7,2),
                ws_ext_list_price        decimal(7,2),
                ws_ext_tax               decimal(7,2),
                ws_coupon_amt            decimal(7,2),
                ws_ext_ship_cost         decimal(7,2),
                ws_net_paid              decimal(7,2),
                ws_net_paid_inc_tax      decimal(7,2),
                ws_net_paid_inc_ship     decimal(7,2),
                ws_net_paid_inc_ship_tax decimal(7,2),
                ws_net_profit            decimal(7,2));

create table web_returns (
                wr_returned_date_sk      bigint,
                wr_returned_time_sk      bigint,
                wr_item_sk               bigint not null,
                wr_refunded_customer_sk  bigint,
                wr_refunded_cdemo_sk     bigint,
                wr_refunded_hdemo_sk     bigint,
                wr_refunded_addr_sk      bigint,
                wr_returning_customer_sk bigint,
                wr_returning_cdemo_sk    bigint,
                wr_returning_hdemo_sk    bigint,
                wr_returning_addr_sk     bigint,
                wr_web_page_sk           bigint,
                wr_reason_sk             bigint,
                wr_order_number          bigint not null,
                wr_return_quantity       bigint,
                wr_return_amt            decimal(7,2),
                wr_return_tax            decimal(7,2),
                wr_return_amt_inc_tax    decimal(7,2),
                wr_fee                   decimal(7,2),
                wr_return_ship_cost      decimal(7,2),
                wr_refunded_cash         decimal(7,2),
                wr_reversed_charge       decimal(7,2),
                wr_amount_credit         decimal(7,2),
                wr_net_loss              decimal(7,2));

create table inventory (
                inv_date_sk              bigint not null,
                inv_item_sk              bigint not null,
                inv_warehouse_sk         bigint not null,
                inv_quantity_on_hand    bigint);

create table store (
                s_store_sk               bigint not null,
                s_store_id               char(16) not null,
		s_rec_start_date         date,
		s_rec_end_date           date,
		s_closed_date_sk         bigint,
		s_store_name             varchar(50),
		s_number_employees       bigint,
		s_floor_space            bigint,
		s_hours                  char(20),
		s_manager                varchar(40),
		s_market_id              bigint,
		s_geography_class        varchar(100),
		s_market_desc            varchar(100),
		s_market_manager         varchar(40),
		s_division_id            bigint,
		s_division_name          varchar(40),
                s_company_id             bigint,
                s_company_name           varchar(50),
		s_street_number          varchar(10),
		s_street_name            varchar(60),
		s_street_type            varchar(15),
		s_suite_number           varchar(10),
		s_city                   varchar(60),
		s_county                 varchar(30),
		s_state                  char(2),
		s_zip                    char(10),
		s_country                varchar(20),
                s_gmt_offset             decimal(5,2),
                s_tax_percentage         decimal(5,2));

create table call_center (
                cc_call_center_sk        bigint not null,
                cc_call_center_id        char(16) not null,
                cc_rec_start_date        date,
                cc_rec_end_date          date,
                cc_closed_date_sk        bigint,
                cc_open_date_sk          bigint,
                cc_name                  varchar(50),
                cc_class                 varchar(50),
                cc_employees             bigint,
                cc_sq_ft                 bigint,
                cc_hours                 varchar(20),
                cc_manager               varchar(40),
                cc_mkt_id                bigint,
                cc_mkt_class             varchar(50),
                cc_mkt_desc              varchar(100),
                cc_market_manager        varchar(40),
                cc_division              bigint,
                cc_division_name         varchar(50),
                cc_company               bigint,
                cc_company_name          varchar(50),
                cc_street_number         varchar(10),
                cc_street_name           varchar(60),
                cc_street_type           varchar(15),
                cc_suite_number          varchar(10),
                cc_city                  varchar(60),
                cc_county                varchar(30),
                cc_state                 char(2),
                cc_zip                   char(10),
                cc_country               varchar(20),
                cc_gmt_offset            decimal(5,2),
                cc_tax_percentage        decimal(5,2));

create table catalog_page (
		cp_catalog_page_sk       bigint not null,
                cp_catalog_page_id       char(16) not null,
                cp_start_date_sk         bigint,
                cp_end_date_sk           bigint,
                cp_department            varchar(50),
                cp_catalog_number        bigint,
                cp_catalog_page_number   bigint,
                cp_description           varchar(100),
                cp_type                  varchar(100));

create table web_site (
		web_site_sk              bigint not null,
                web_site_id              char(16) not null,
                web_rec_start_date       date,
                web_rec_end_date         date,
                web_name                 varchar(50),
                web_open_date_sk         bigint,
                web_close_date_sk        bigint,
                web_class                varchar(50),
                web_manager              varchar(40),
                web_mkt_id               bigint,
                web_mkt_class            varchar(50),
                web_mkt_desc             varchar(100),
                web_market_manager       varchar(40),
                web_company_id           bigint,
                web_company_name         varchar(50),
                web_street_number        char(10),
                web_street_name          varchar(60),
                web_street_type          char(15),
                web_suite_number         char(10),
                web_city                 varchar(60),
                web_county               varchar(30),
                web_state                char(2),
                web_zip                  char(10),
                web_country              varchar(20),
               	web_gmt_offset           decimal(5,2),
                web_tax_percentage       decimal(5,2));

create table web_page (
		wp_web_page_sk           bigint not null,
                wp_web_page_id           char(16) not null,
                wp_rec_start_date        date,
                wp_rec_end_date          date,
                wp_creation_date_sk      bigint,
                wp_access_date_sk        bigint,
                wp_autogen_flag          char(1),
                wp_customer_sk           bigint,
                wp_url                   varchar(100),
                wp_type                  char(50),
                wp_char_count            bigint,
                wp_link_count            bigint,
                wp_image_count           bigint,
                wp_max_ad_count          bigint);

create table warehouse (
                w_warehouse_sk           bigint not null,
                w_warehouse_id           char(16) not null,
                w_warehouse_name         varchar(20),
                w_warehouse_sq_ft        bigint,
                w_street_number          char(10),
                w_street_name            varchar(60),
                w_street_type            char(15),
                w_suite_number           char(10),
                w_city                   varchar(60),
                w_county                 varchar(30),
                w_state                  char(2),
                w_zip                    char(10),
                w_country                varchar(20),
                w_gmt_offset             decimal(5,2));

create table customer (
                c_customer_sk            bigint not null,
                c_customer_id            char(16) not null,
                c_current_cdemo_sk       bigint,
                c_current_hdemo_sk       bigint,
                c_current_addr_sk        bigint,
                c_first_shipto_date_sk   bigint,
                c_first_sales_date_sk    bigint,
                c_salutation             char(10),
                c_first_name             char(20),
                c_last_name              char(30),
                c_preferred_cust_flag    char(1),
                c_birth_day              bigint,
                c_birth_month            bigint,
                c_birth_year             bigint,
                c_birth_country          varchar(20),
                c_login                  char(13),
                c_email_address          char(50),
                c_last_review_date       bigint);

create table customer_address (
               	ca_address_sk            bigint not null,
                ca_address_id            char(16) not null,
                ca_street_number         char(10),
                ca_street_name           varchar(60),
                ca_street_type           char(15),
                ca_suite_number          char(10),
                ca_city                  varchar(60),
                ca_county                varchar(30),
                ca_state                 char(2),
                ca_zip                   char(10),
                ca_country               varchar(20),
                ca_gmt_offset            decimal(5,2),
                ca_location_type         char(20));

create table customer_demographics (
                cd_demo_sk               bigint not null,
                cd_gender                char(1),
                cd_marital_status        char(1),
                cd_education_status      char(20),
                cd_purchase_estimate     bigint,
                cd_credit_rating         char(10),
                cd_dep_count             bigint,
                cd_dep_employed_count    bigint,
                cd_dep_college_count     bigint);

create table date_dim (
                d_date_sk                bigint not null,
                d_date_id                char(16) not null,
                d_date                   date,
                d_month_seq              bigint,
                d_week_seq               bigint,
                d_quarter_seg            bigint,
                d_year                   bigint,
                d_dow                    bigint,
                d_moy                    bigint,
                d_dom                    bigint,
                d_qoy                    bigint,
                d_fy_year                bigint,
                d_fy_quarter_seq         bigint,
                d_fy_week_seq            bigint,
                d_day_name               char(9),
                d_quarter_name           char(6),
                d_holiday                char(1),
                d_weekend                char(1),
                d_following_holiday      char(1),
                d_first_dom              bigint,
                d_last_dom               bigint,
                d_same_day_1y            bigint,
                d_same_day_1q            bigint,
                d_current_day            char(1),
                d_current_week           char(1),
                d_current_month          char(1),
                d_current_quarter        char(1),
                d_current_year           char(1));

create table household_demographics (
                hd_demo_sk               bigint not null,
                hd_income_band_sk        bigint not null,
                hd_buy_potential         char(15),
                hd_dep_count             bigint,
                hd_vehicle_count         bigint);

create table item (
                i_item_sk                bigint not null,
                i_item_id                char(16) not null,
                i_rec_start_date         date,
                i_rec_end_date           date,
                i_item_desc              varchar(200),
                i_current_price          decimal(7,2),
                i_wholesale_cost         decimal(7,2),
                i_brand_id               bigint,
                i_brand                  char(50),
                i_class_id               bigint,
                i_class                  char(50),
                i_category_id            bigint,
                i_category               char(50),
                i_manufact_id            bigint,
                i_manufact               char(50),
                i_size                   char(20),
                i_formulation            char(20),
                i_color                  char(20),
                i_units                  char(10),
                i_container              char(10),
                i_manager_id             bigint,
                i_product_name           char(50));

create table income_band (
                ib_income_band_sk        bigint not null,
                ib_lower_bound           bigint,
                ib_upper_bound           bigint);

create table promotion (
                p_promo_sk               bigint not null,
                p_promo_id               char(16) not null,
                p_start_date_sk          bigint,
                p_end_date_sk            bigint,
                p_item_sk                bigint,
                p_cost                   decimal(15,2),
                p_response_target        bigint,
                p_promo_name             char(50),
                p_channel_dmail          char(1),
                p_channel_email          char(1),
                p_channel_catalog        char(1),
                p_channel_tv             char(1),
                p_channel_radio          char(1),
                p_channel_press          char(1),
                p_channel_event          char(1),
                p_channel_demo           char(1),
                p_channel_details        varchar(100),
                p_purpose                char(15),
                p_discount_active        char(1));

create table reason (
                r_reason_sk              bigint not null,
                r_reason_id              char(16) not null,
                r_reason_desc            char(100));

create table ship_mode (
                sm_ship_mode_sk          bigint not null,
                sm_ship_mode_id          char(16) not null,
                sm_type                  char(30),
                sm_code                  char(10),
                sm_carrier               char(20),
                sm_contract              char(20));

create table time_dim (
                t_time_sk                bigint not null,
                t_time_id                char(16) not null,
                t_time                   bigint,
                t_hour                   bigint,
                t_minute                 bigint,
                t_second                 bigint,
                t_am_pm                  char(2),
                t_shift                  char(20),
                t_sub_shift              char(20),
                t_meal_time              char(20));

create table dsdgen_version (
                dv_version               varchar(24) not null,
                dv_create_date           date not null,
                dv_create_time           time not null,
                dv_cmdline_args          varchar(200));


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






