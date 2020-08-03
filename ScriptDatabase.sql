/**
@CreatedDate: Sat Aug 01 08:00:00 2020
@Author: Carlos Quintanilla
**/
SET SESSION AUTHORIZATION 'postgres';
SET client_encoding= 'UTF8';
------------------------------------------------------------------------
--Database
------------------------------------------------------------------------
CREATE DATABASE applaudo_test
WITH TEMPLATE = template0
ENCODING = 'UTF8' LC_COLLATE = 'English_United States.1252' LC_CTYPE = 'English_United States.1252';

------------------------------------------------------------------------
--Schema
------------------------------------------------------------------------
CREATE SCHEMA staging AUTHORIZATION postgres;

------------------------------------------------------------------------
--Schema: Public
------------------------------------------------------------------------

------------------------------------------------------------------------
--PRODUCT_DETAILS
------------------------------------------------------------------------
--TABLE
CREATE TABLE public.product_details
(
    "PROD_PRODUCT_DETAILS_KEY"      bigint NOT NULL,
    "PRODUCT_NAME"                  character varying(200),
    "AISLE"                         character varying(100),
    "DEPARTMENT"                    character varying(100)
);

--SEQUENCE
CREATE SEQUENCE public."product_details_PROD_PRODUCT_DETAILS_KEY_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--ALTER
ALTER TABLE ONLY public.product_details
ALTER COLUMN "PROD_PRODUCT_DETAILS_KEY"
SET DEFAULT nextval ('public."product_details_PROD_PRODUCT_DETAILS_KEY_seq"'::regclass);

ALTER TABLE public.product_details ADD CONSTRAINT unique_product_det UNIQUE ("PRODUCT_NAME", "AISLE", "DEPARTMENT");

ALTER SEQUENCE public."product_details_PROD_PRODUCT_DETAILS_KEY_seq" OWNED BY public.product_details."PROD_PRODUCT_DETAILS_KEY";

ALTER TABLE ONLY public.product_details
    ADD CONSTRAINT product_details_pkey PRIMARY KEY ("PROD_PRODUCT_DETAILS_KEY");

--COMMENTS
COMMENT ON TABLE public.product_details IS 'Table used to load product catalog, is filled by update_product_catalog function using information of staging_product_details table. If a product has the same name but different details, it is a new product.';
COMMENT ON COLUMN product_details."PROD_PRODUCT_DETAILS_KEY" IS 'Product Primary Key';
COMMENT ON COLUMN product_details."PRODUCT_NAME" IS 'Product Name';
COMMENT ON COLUMN product_details."AISLE" IS 'Product Aisle';
COMMENT ON COLUMN product_details."DEPARTMENT" IS 'Product Department';
COMMENT ON SEQUENCE "product_details_PROD_PRODUCT_DETAILS_KEY_seq" IS 'Used to generate primary key of product_details table';
COMMENT ON CONSTRAINT "unique_product_det" ON public.product_details IS 'Constrainst used to merge products';

------------------------------------------------------------------------
--Table: order_details
------------------------------------------------------------------------
--TABLE
CREATE TABLE public.order_details
(
    "PROD_ORDER_DETAILS_KEY"        bigint NOT NULL,
    "ORDER_ID"                      character varying(100),
    "USER_ID"                       character varying(100),
    "ORDER_NUMBER"                  character varying(100),
    "ORDER_DOW"                     character varying(100),
    "ORDER_HOUR_OF_DAY"             integer,
    "DAYS_SINCE_PRIOR_ORDER"        integer,
    "PRODUCT"                       character varying(200),
    "AISLE"                         character varying(100),
    "DEPARTMENT"                    character varying(100),
    "ADD_TO_CART_ORDER"             numeric(6,2)
);

--SEQUENCE
CREATE SEQUENCE public."order_details_PROD_ORDER_DETAILS_KEY_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--ALTER
ALTER TABLE ONLY public.order_details
ALTER COLUMN "PROD_ORDER_DETAILS_KEY"
SET DEFAULT nextval ('public."order_details_PROD_ORDER_DETAILS_KEY_seq"'::regclass);

ALTER TABLE ONLY public.order_details
ADD CONSTRAINT prod_order_details_pkey PRIMARY KEY
("PROD_ORDER_DETAILS_KEY");


ALTER SEQUENCE public."order_details_PROD_ORDER_DETAILS_KEY_seq" OWNED BY public.order_details."PROD_ORDER_DETAILS_KEY";


--COMMENTS
COMMENT ON TABLE public.order_details IS 'Table used to load order details, is filled by upload_order_details function using information of staging_order_details table.';
COMMENT ON COLUMN order_details."PROD_ORDER_DETAILS_KEY" is 'Table Primary Key';
COMMENT ON COLUMN order_details."ORDER_ID" is 'Order identifier';
COMMENT ON COLUMN order_details."USER_ID" is 'User identifier';
COMMENT ON COLUMN order_details."ORDER_NUMBER" is 'Order number';
COMMENT ON COLUMN order_details."ORDER_DOW" is 'Day of the week the order was placed (0 being Sunday and 6 being Saturday)';
COMMENT ON COLUMN order_details."ORDER_HOUR_OF_DAY" is 'Time (in hours) the order was placed';
COMMENT ON COLUMN order_details."DAYS_SINCE_PRIOR_ORDER" is 'how many days since the previous placed order';
COMMENT ON COLUMN order_details."PRODUCT" is 'Product Name';
COMMENT ON COLUMN order_details."AISLE" is 'Product Aisle';
COMMENT ON COLUMN order_details."DEPARTMENT" is 'Product Department';
COMMENT ON COLUMN order_details."ADD_TO_CART_ORDER" is 'Number of products';
COMMENT ON SEQUENCE "order_details_PROD_ORDER_DETAILS_KEY_seq" IS 'Used to generate primary key of order_details table';


------------------------------------------------------------------------
--Table: user_category
------------------------------------------------------------------------
--TABLE
CREATE TABLE public.user_category (
    "USER_CATEGORY_KEY"             bigint NOT NULL,
    "USER_ID"                       character varying(100),
    "USER_CATEGORY"                 character varying(50)
);

--SEQUENCE
CREATE SEQUENCE public."user_category_USER_CATEGORY_KEY_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--ALTER
ALTER SEQUENCE public."user_category_USER_CATEGORY_KEY_seq" OWNED BY public.user_category."USER_CATEGORY_KEY";

ALTER TABLE ONLY public.user_category
ALTER COLUMN "USER_CATEGORY_KEY"
SET DEFAULT nextval('public."user_category_USER_CATEGORY_KEY_seq"'::regclass);


ALTER TABLE ONLY public.user_category ADD CONSTRAINT unique_user_id UNIQUE ("USER_ID");

--COMMENTS
COMMENT ON TABLE public.user_category IS 'Incremental table used to set a user category (Mom, Single, Pet Friendly). There is a process that update the existing row otherwise load a new row for new customers.';
COMMENT ON COLUMN user_category."USER_CATEGORY_KEY" is 'Table Primary Key';
COMMENT ON COLUMN user_category."USER_ID" is 'User identifier';
COMMENT ON COLUMN user_category."USER_CATEGORY" is 'User assigned category';
COMMENT ON SEQUENCE "user_category_USER_CATEGORY_KEY_seq" IS 'Used to generate primary key of user_category table';
COMMENT ON CONSTRAINT "unique_user_id" ON public.user_category IS 'Constrainst used to merge user category';

------------------------------------------------------------------------
--Table: user_segmentation
------------------------------------------------------------------------

--TABLE
CREATE TABLE public.user_segmentation
(
    "USER_ID" text,
    "USER_SEGMENTATION" text
);

--COMMENTS
COMMENT ON TABLE public.user_category IS 'Table used to generate reports of customer segmentation (Youve got a frriend in me, Baby come back, Special Offers) , this table is calculated during every load.';
COMMENT ON COLUMN user_segmentation."USER_ID" is 'User identifier';
COMMENT ON COLUMN user_segmentation."USER_SEGMENTATION" is 'User Segmentation';
------------------------------------------------------------------------
--Schema: Staging
------------------------------------------------------------------------

------------------------------------------------------------------------
--Table: staging_product_details
------------------------------------------------------------------------
--TABLE
CREATE TABLE staging.staging_product_details
(
    "PRODUCT_DETAILS_KEY"           bigint NOT NULL,
    "PRODUCT_NAME"                  character varying(200),
    "AISLE"                         character varying(100),
    "DEPARTMENT"                    character varying(100)
);

--SEQUENCE
CREATE SEQUENCE staging."staging_product_details_PRODUCT_DETAILS_KEY_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--ALTER
ALTER SEQUENCE staging."staging_product_details_PRODUCT_DETAILS_KEY_seq"
OWNED BY staging.staging_product_details."PRODUCT_DETAILS_KEY";

ALTER TABLE ONLY staging.staging_product_details
ALTER COLUMN "PRODUCT_DETAILS_KEY"
SET DEFAULT nextval ('staging."staging_product_details_PRODUCT_DETAILS_KEY_seq"'::regclass);

ALTER TABLE ONLY staging.staging_product_details
ADD CONSTRAINT staging_product_details_pkey PRIMARY KEY
("PRODUCT_DETAILS_KEY");

COMMENT ON TABLE staging."staging_product_details" IS 'Table used to load order details from CSV and database.';
COMMENT ON COLUMN staging."staging_product_details"."PRODUCT_DETAILS_KEY" is 'Table Primary Key';
COMMENT ON COLUMN staging."staging_product_details"."PRODUCT_NAME" is 'Product Name';
COMMENT ON COLUMN staging."staging_product_details"."AISLE" is 'Aisle Product';
COMMENT ON COLUMN staging."staging_product_details"."DEPARTMENT" is 'Product Department';
COMMENT ON SEQUENCE staging."staging_product_details_PRODUCT_DETAILS_KEY_seq" IS 'Used to generate primary key of staging_product_details table';

------------------------------------------------------------------------
--Table: staging_order_details
------------------------------------------------------------------------
--TABLE
CREATE TABLE staging.staging_order_details
(
    "ORDER_DETAILS_KEY"             bigint NOT NULL,
    "ORDER_ID"                      character varying(100),
    "USER_ID"                       character varying(100),
    "ORDER_NUMBER"                  character varying(100),
    "ORDER_DOW"                     character varying(100),
    "ORDER_HOUR_OF_DAY"             integer,
    "DAYS_SINCE_PRIOR_ORDER"        integer,
    "PRODUCT"                       character varying(200),
    "AISLE"                         character varying(100),
    "DEPARTMENT"                    character varying(100),
    "ADD_TO_CART_ORDER"             numeric(6,2),
    "SOURCE"                        character varying(50)
);

--SEQUENCE
CREATE SEQUENCE staging."staging_order_details_ORDER_DETAILS_KEY_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--ALTER
ALTER SEQUENCE staging."staging_order_details_ORDER_DETAILS_KEY_seq"
OWNED BY staging.staging_order_details."ORDER_DETAILS_KEY";


ALTER TABLE ONLY staging.staging_order_details
ALTER COLUMN "ORDER_DETAILS_KEY"
SET DEFAULT nextval('staging."staging_order_details_ORDER_DETAILS_KEY_seq"'::regclass);

ALTER TABLE ONLY staging.staging_order_details
ADD CONSTRAINT order_details_pkey PRIMARY KEY
("ORDER_DETAILS_KEY");


--COMMENTS
COMMENT ON TABLE  staging."staging_order_details" IS 'Table used to load order details from CSV and database.';
COMMENT ON COLUMN  staging."staging_order_details"."ORDER_DETAILS_KEY" is 'Table Primary Key';
COMMENT ON COLUMN  staging."staging_order_details"."ORDER_ID" is 'Order identifier';
COMMENT ON COLUMN  staging."staging_order_details"."USER_ID" is 'User identifier';
COMMENT ON COLUMN  staging."staging_order_details"."ORDER_NUMBER" is 'Order number';
COMMENT ON COLUMN  staging."staging_order_details"."ORDER_DOW" is 'Day of the week the order was placed (0 being Sunday and 6 being Saturday)';
COMMENT ON COLUMN  staging."staging_order_details"."ORDER_HOUR_OF_DAY" is 'Time (in hours) the order was placed';
COMMENT ON COLUMN  staging."staging_order_details"."DAYS_SINCE_PRIOR_ORDER" is 'how many days since the previous placed order';
COMMENT ON COLUMN  staging."staging_order_details"."PRODUCT" is 'Product Name';
COMMENT ON COLUMN  staging."staging_order_details"."AISLE" is 'Product Aisle';
COMMENT ON COLUMN  staging."staging_order_details"."DEPARTMENT" is 'Product Department';
COMMENT ON COLUMN  staging."staging_order_details"."ADD_TO_CART_ORDER" is 'Number of products';
COMMENT ON COLUMN  staging."staging_order_details"."SOURCE" is 'To store cvs file name or database source name';
COMMENT ON SEQUENCE staging."staging_order_details_ORDER_DETAILS_KEY_seq" IS 'Used to generate primary key of staging_order_details table';

------------------------------------------------------------------------
--Table: staging_order_details
------------------------------------------------------------------------
--TABLE
CREATE TABLE staging.staging_user_category
(
    "STG_USER_CATEGORY_KEY"             bigint NOT NULL,
    "USER_ID"                           character varying(100),
    "BUY_MOM_QTY"                       numeric,
    "BUY_SINGLE_QTY"                    numeric,
    "BUY_PET_FRIENDLY_QTY"              numeric,
    "BUY_COMPLETE_MYSTERY_QTY"          numeric,
    "BUY_TOTAL"                         numeric,
    "USER_CATEGORY"                     character varying(50)
);

--SEQUENCE
CREATE SEQUENCE staging."staging_user_category_USER_CATEGORY_KEY_seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--ALTER
ALTER SEQUENCE staging."staging_user_category_USER_CATEGORY_KEY_seq"
OWNED BY staging.staging_user_category."STG_USER_CATEGORY_KEY";

ALTER TABLE ONLY staging.staging_user_category
ALTER COLUMN "STG_USER_CATEGORY_KEY"
SET DEFAULT nextval('staging."staging_user_category_USER_CATEGORY_KEY_seq"'::regclass);

--COMMENTS
COMMENT ON TABLE staging.staging_user_category IS 'Table used to set a user category (Mom, Single, Pet Friendly). There is a process that update the existing row otherwise load a new row for new customers.';
COMMENT ON COLUMN staging.staging_user_category."STG_USER_CATEGORY_KEY" is 'Table used to load order details from CSV and database.';
COMMENT ON COLUMN staging.staging_user_category."USER_ID" is 'User identifier';
COMMENT ON COLUMN staging.staging_user_category."BUY_MOM_QTY" is 'Sum of Products from Mom category (dairy eggs,bakery,household,babies)';
COMMENT ON COLUMN staging.staging_user_category."BUY_SINGLE_QTY" is 'Sum of Products from Single category (canned goods,meat seafood,alcohol,snacks,beverages)';
COMMENT ON COLUMN staging.staging_user_category."BUY_PET_FRIENDLY_QTY" is 'Sum of Products from Pet Friendly category (canned goods,pets,frozen)';
COMMENT ON COLUMN staging.staging_user_category."BUY_COMPLETE_MYSTERY_QTY" is '';
COMMENT ON COLUMN staging.staging_user_category."BUY_TOTAL" is 'Sum of products per user';
COMMENT ON COLUMN staging.staging_user_category."USER_CATEGORY" is 'User catergory (Mom, Single, Pet Friendly)';
COMMENT ON SEQUENCE staging."staging_user_category_USER_CATEGORY_KEY_seq" IS 'Used to generate primary key of staging_user_category table';

------------------------------------------------------------------------
--Functions
------------------------------------------------------------------------

------------------------------------------------------------------------
--Function: update_product_catalog
------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.update_product_catalog() RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO staging.staging_product_details
        ("PRODUCT_NAME", "AISLE", "DEPARTMENT")
    SELECT
        DISTINCT "PRODUCT", "AISLE", NULL "DEPARTMENT"
    FROM staging.staging_order_details sod
    WHERE
		NOT EXISTS(
			SELECT 1
    FROM staging.staging_product_details spd
    WHERE UPPER(sod."PRODUCT") = UPPER(spd."PRODUCT_NAME")
        AND UPPER(sod."AISLE") = UPPER(spd."AISLE")
		);
END
$$;

COMMENT ON FUNCTION public.update_product_catalog IS 'Function used to load data into staging_product_details table';

CREATE OR REPLACE FUNCTION public.upload_product_details(
	)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
    
AS $BODY$
BEGIN
    DELETE FROM product_details pd WHERE EXISTS(
		SELECT
			1--"PRODUCT_NAME", "AISLE", "DEPARTMENT"
		FROM staging.staging_product_details spd
		WHERE 
			spd."PRODUCT_NAME" = pd."PRODUCT_NAME"
			AND spd."AISLE" = pd."AISLE"
			AND spd."DEPARTMENT" = pd."DEPARTMENT"
	);
	INSERT INTO product_details ("PRODUCT_NAME", "AISLE", "DEPARTMENT")
	SELECT
		"PRODUCT_NAME", "AISLE", "DEPARTMENT"
	FROM staging.staging_product_details;
END
$BODY$;
COMMENT ON FUNCTION public.upload_product_details IS 'Function used to load data into product_details table from staging_product_details';


CREATE OR REPLACE FUNCTION public.upload_order_details() RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO order_details
        ("ORDER_ID", "USER_ID", "ORDER_NUMBER", "ORDER_DOW", "ORDER_HOUR_OF_DAY", "DAYS_SINCE_PRIOR_ORDER", "PRODUCT", "AISLE", "DEPARTMENT", "ADD_TO_CART_ORDER")
    SELECT
        "ORDER_ID", "USER_ID", "ORDER_NUMBER", "ORDER_DOW", "ORDER_HOUR_OF_DAY", "DAYS_SINCE_PRIOR_ORDER", "PRODUCT", "AISLE", "DEPARTMENT", "ADD_TO_CART_ORDER"
    FROM staging.staging_order_details sod;
END
$$;
COMMENT ON FUNCTION public.upload_order_details IS 'Function used to load data into order_details table from staging_order_details';

CREATE OR REPLACE FUNCTION public.user_product_summary() RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO staging.staging_user_category
        ("USER_ID", "BUY_MOM_QTY", "BUY_SINGLE_QTY", "BUY_PET_FRIENDLY_QTY", "BUY_COMPLETE_MYSTERY_QTY", "BUY_TOTAL", "USER_CATEGORY")
    SELECT
        "USER_ID",
        SUM(BUY_MOM_QTY) AS BUY_MOM_QTY, SUM(BUY_SINGLE_QTY) AS BUY_SINGLE_QTY,
        SUM(BUY_PET_FRIENDLY_QTY) AS BUY_PET_FRIENDLY_QTY, SUM(BUY_COMPLETE_MYSTERY_QTY) AS BUY_COMPLETE_MYSTERY_QTY,
        SUM(BUY_TOTAL) AS BUY_TOTAL
    FROM
        (
	SELECT
            "USER_ID",
            CASE
			WHEN LOWER("DEPARTMENT") IN ('dairy eggs', 'bakery', 'household', 'babies')
				THEN SUM("ADD_TO_CART_ORDER")
		END AS BUY_MOM_QTY,
            CASE
			WHEN LOWER("DEPARTMENT") IN ('canned goods', 'meat seafood', 'alcohol', 'snacks', 'beverages')
				THEN SUM("ADD_TO_CART_ORDER")
		END AS BUY_SINGLE_QTY,
            CASE
			WHEN LOWER("DEPARTMENT") IN ('canned goods', 'pets', 'frozen')
				THEN SUM("ADD_TO_CART_ORDER")
		END AS BUY_PET_FRIENDLY_QTY,
            CASE
			WHEN LOWER("DEPARTMENT") NOT IN (
									'dairy eggs', 'bakery', 'household', 'babies', 
									'canned goods', 'meat seafood', 'alcohol', 'snacks', 'beverages', 
									'canned goods', 'pets', 'frozen'
									)
				THEN SUM("ADD_TO_CART_ORDER")
		END AS BUY_COMPLETE_MYSTERY_QTY,
            SUM("ADD_TO_CART_ORDER") AS BUY_TOTAL
        FROM staging.staging_order_details
        GROUP BY
		"USER_ID",
		"DEPARTMENT"
		) percentages
    GROUP BY
	"USER_ID";
END
$$;

COMMENT ON FUNCTION public.user_product_summary IS 'Function used to load data into staging_user_category calculating from staging_order_details';

CREATE OR REPLACE FUNCTION public.get_user_products_data() RETURNS TABLE("OUT_USER_ID" character varying,
    "OUT_ORDER_DOW" character varying,
    "OUT_DAYS_SINCE_PRIOR_ORDER" integer,
    "OUT_SEGMENTATION" character varying,
    "OUT_TOTAL_PRODUCTS" numeric)
    LANGUAGE plpgsql
    AS $$
begin
    return query
    SELECT
        "USER_ID",
        "ORDER_DOW",
        "DAYS_SINCE_PRIOR_ORDER",
        'YGAFIM'
    ::VARCHAR AS "SEGMENTATION",
		SUM
    ("ADD_TO_CART_ORDER") AS "TOTAL_PRODUCTS"
	FROM
		order_details
	WHERE
		"DAYS_SINCE_PRIOR_ORDER" <= 7
	GROUP BY
		"USER_ID",
		"ORDER_DOW",
		"DAYS_SINCE_PRIOR_ORDER"
	UNION ALL
    SELECT
        "USER_ID",
        "ORDER_DOW",
        "DAYS_SINCE_PRIOR_ORDER",
        'BCB'
    ::VARCHAR AS "SEGMENTATION",
		SUM
    ("ADD_TO_CART_ORDER") AS "TOTAL_PRODUCTS"
	FROM
		order_details
	WHERE
		"DAYS_SINCE_PRIOR_ORDER" >= 10 
		AND "DAYS_SINCE_PRIOR_ORDER" <= 19 
	GROUP BY
		"USER_ID",
		"ORDER_DOW",
		"DAYS_SINCE_PRIOR_ORDER"
	UNION ALL
    SELECT
        "USER_ID",
        "ORDER_DOW",
        "DAYS_SINCE_PRIOR_ORDER",
        'SO'
    ::VARCHAR AS "SEGMENTATION",
		SUM
    ("ADD_TO_CART_ORDER") AS "TOTAL_PRODUCTS"
	FROM
		order_details
	WHERE
		"DAYS_SINCE_PRIOR_ORDER" >= 20
	GROUP BY
		"USER_ID",
		"ORDER_DOW",
		"DAYS_SINCE_PRIOR_ORDER";
end;$$;

COMMENT ON FUNCTION public.get_user_products_data IS 'Function used to return query about products for calculations in Python';

CREATE OR REPLACE FUNCTION public.user_category_label() RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO user_category
        ("USER_ID", "USER_CATEGORY")
    SELECT
        "USER_ID",
        CASE
		WHEN SUM("BUY_MOM_QTY") / SUM("BUY_TOTAL") > 0.5 THEN 'Mom'
		WHEN SUM("BUY_SINGLE_QTY") / SUM("BUY_TOTAL") > 0.6 THEN 'Single'
		WHEN SUM("BUY_PET_FRIENDLY_QTY") / SUM("BUY_TOTAL") > 0.3 THEN 'Pet Friendly'
		ELSE 'A complete mystery'
	END AS "USER_CATEGORY"
    FROM
        staging.staging_user_category
    GROUP BY
	"USER_ID"
    ON CONFLICT
    ("USER_ID") 
	DO
    UPDATE SET "USER_CATEGORY" = excluded."USER_CATEGORY";

END
$$;
COMMENT ON FUNCTION public.user_category_label IS 'Function used to return query about user category for calculations in Python';

CREATE OR REPLACE FUNCTION public.get_segmentation_data() RETURNS TABLE("OUT_ORDER_ID" character varying, "OUT_ORDER_DOW" character varying, "OUT_DAYS_SINCE_PRIOR_ORDER" integer, "OUT_SEGMENTATION" character varying, "OUT_ADD_TO_CART_ORDER" numeric, "OUT_Q1" numeric, "OUT_Q2" numeric, "OUT_Q3" numeric)
    LANGUAGE plpgsql
    AS $$
begin
	return query 
	SELECT 
		"ORDER_ID",
		"ORDER_DOW",
		"DAYS_SINCE_PRIOR_ORDER",
		'YGAFIM'::varchar AS "SEGMENTATION",
		SUM("ADD_TO_CART_ORDER") AS "ADD_TO_CART_ORDER_TOTAL",
		0::numeric as "Q1",
		0::numeric as "Q2",
		0::numeric as "Q3"
	FROM
		order_details
	WHERE
		"DAYS_SINCE_PRIOR_ORDER" <= 7
	GROUP BY "ORDER_ID",
			"ORDER_DOW",
			"DAYS_SINCE_PRIOR_ORDER"
	UNION ALL
	SELECT 
		"ORDER_ID",
		"ORDER_DOW",
		"DAYS_SINCE_PRIOR_ORDER",
		'BCB'::varchar AS "SEGMENTATION",
		SUM("ADD_TO_CART_ORDER") AS "ADD_TO_CART_ORDER_TOTAL",
		0::numeric as "Q1",
		0::numeric as "Q2",
		0::numeric as "Q3"
	FROM
		order_details
	WHERE
		"DAYS_SINCE_PRIOR_ORDER" >= 10 
		AND "DAYS_SINCE_PRIOR_ORDER" <= 19 
	GROUP BY "ORDER_ID",
			"ORDER_DOW",
			"DAYS_SINCE_PRIOR_ORDER"
	UNION ALL
	SELECT 
		"ORDER_ID",
		"ORDER_DOW",
		"DAYS_SINCE_PRIOR_ORDER",
		'SO'::varchar AS "SEGMENTATION",
		SUM("ADD_TO_CART_ORDER") AS "ADD_TO_CART_ORDER_TOTAL",
		0::numeric as "Q1",
		0::numeric as "Q2",
		0::numeric as "Q3"
	FROM
		order_details
	WHERE
		"DAYS_SINCE_PRIOR_ORDER" >= 20
	GROUP BY "ORDER_ID",
			"ORDER_DOW",
			"DAYS_SINCE_PRIOR_ORDER";
end;$$;

COMMENT ON FUNCTION public.get_segmentation_data IS 'Function used to return query about user segmentation for calculations in Python';