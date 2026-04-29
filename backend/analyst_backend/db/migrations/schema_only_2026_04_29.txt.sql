--
-- PostgreSQL database dump
--

\restrict V4swM1x6XKacKsRGCqvd9xiqUMOKsBWzSflGTIojMxbceZtqhkT8gAskVc8CILv

-- Dumped from database version 18.3 (Postgres.app)
-- Dumped by pg_dump version 18.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: pg_database_owner
--

COMMENT ON SCHEMA public IS '';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: uuid-ossp; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;


--
-- Name: EXTENSION "uuid-ossp"; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION "uuid-ossp" IS 'generate universally unique identifiers (UUIDs)';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: active_tokens; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.active_tokens (
    token character varying(64) NOT NULL,
    user_id character varying(30) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone DEFAULT (now() + '24:00:00'::interval) NOT NULL
);


ALTER TABLE public.active_tokens OWNER TO chaitanya;

--
-- Name: audit_log; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.audit_log (
    id bigint NOT NULL,
    ts timestamp with time zone DEFAULT now() NOT NULL,
    user_id character varying(30),
    user_email character varying(255),
    client_id character varying(30),
    action_type character varying(60) NOT NULL,
    details text,
    ip_address character varying(45),
    outcome character varying(20) DEFAULT 'success'::character varying NOT NULL
);


ALTER TABLE public.audit_log OWNER TO chaitanya;

--
-- Name: audit_log_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.audit_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.audit_log_id_seq OWNER TO chaitanya;

--
-- Name: audit_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.audit_log_id_seq OWNED BY public.audit_log.id;


--
-- Name: brands; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.brands (
    brand_id integer NOT NULL,
    brand_name character varying(100) NOT NULL,
    brand_description text,
    vendor_id integer,
    active smallint DEFAULT 1,
    not_available smallint DEFAULT 0,
    category_hint character varying(100),
    client_id character varying(20) NOT NULL
);


ALTER TABLE public.brands OWNER TO chaitanya;

--
-- Name: business_segments; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.business_segments (
    segment_id character varying(15) NOT NULL,
    segment_name character varying(50) NOT NULL,
    description text,
    criteria character varying(200),
    recommended_focus text
);


ALTER TABLE public.business_segments OWNER TO chaitanya;

--
-- Name: categories; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.categories (
    category_id integer NOT NULL,
    category_name character varying(100) NOT NULL,
    client_id character varying(20) NOT NULL
);


ALTER TABLE public.categories OWNER TO chaitanya;

--
-- Name: chat_messages; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.chat_messages (
    id integer NOT NULL,
    client_id character varying(20) DEFAULT 'CLT-001'::character varying NOT NULL,
    conversation_id character varying(50) NOT NULL,
    role character varying(10) NOT NULL,
    content text NOT NULL,
    tokens_used integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT chat_messages_role_check CHECK (((role)::text = ANY ((ARRAY['user'::character varying, 'assistant'::character varying])::text[])))
);


ALTER TABLE public.chat_messages OWNER TO chaitanya;

--
-- Name: chat_messages_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.chat_messages_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.chat_messages_id_seq OWNER TO chaitanya;

--
-- Name: chat_messages_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.chat_messages_id_seq OWNED BY public.chat_messages.id;


--
-- Name: churn_scores; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.churn_scores (
    score_id integer NOT NULL,
    client_id character varying(20) NOT NULL,
    customer_id character varying(30) NOT NULL,
    scored_at timestamp with time zone DEFAULT now(),
    churn_probability numeric(5,4),
    risk_tier character varying(10),
    churn_label_simulated boolean DEFAULT false,
    driver_1 character varying(100),
    driver_2 character varying(100),
    driver_3 character varying(100),
    model_version character varying(20) DEFAULT 'v1.0'::character varying,
    batch_run_id character varying(50)
);


ALTER TABLE public.churn_scores OWNER TO chaitanya;

--
-- Name: TABLE churn_scores; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.churn_scores IS 'ML model churn risk scores — refreshed nightly';


--
-- Name: churn_scores_score_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.churn_scores_score_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.churn_scores_score_id_seq OWNER TO chaitanya;

--
-- Name: churn_scores_score_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.churn_scores_score_id_seq OWNED BY public.churn_scores.score_id;


--
-- Name: client_config; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.client_config (
    config_id integer NOT NULL,
    client_id character varying(20) NOT NULL,
    client_name character varying(100) NOT NULL,
    client_code character varying(10) NOT NULL,
    currency character varying(10) DEFAULT 'USD'::character varying,
    timezone character varying(50) DEFAULT 'America/Chicago'::character varying,
    fiscal_year_start date,
    churn_window_days integer DEFAULT 90,
    high_ltv_threshold numeric(10,2) DEFAULT 1000.00,
    mid_ltv_threshold numeric(10,2) DEFAULT 200.00,
    max_discount_pct numeric(5,2) DEFAULT 30.00,
    min_repeat_orders integer DEFAULT 2,
    high_value_percentile integer DEFAULT 75,
    recent_order_gap_window integer DEFAULT 3,
    tier_method character varying(20) DEFAULT 'quartile'::character varying,
    custom_platinum_min numeric(10,2) DEFAULT 500.00,
    custom_gold_min numeric(10,2) DEFAULT 250.00,
    custom_silver_min numeric(10,2) DEFAULT 100.00,
    custom_bronze_min numeric(10,2) DEFAULT 0.00,
    reference_date_mode character varying(10) DEFAULT 'auto'::character varying,
    reference_date date,
    prediction_mode character varying(20) DEFAULT 'churn'::character varying,
    created_at timestamp with time zone DEFAULT now(),
    tier_label_platinum character varying(50) DEFAULT '💎 Platinum'::character varying,
    tier_label_gold character varying(50) DEFAULT '🥇 Gold'::character varying,
    tier_label_silver character varying(50) DEFAULT '🥈 Silver'::character varying,
    tier_label_bronze character varying(50) DEFAULT '🥉 Bronze'::character varying,
    is_active boolean DEFAULT true NOT NULL,
    deactivated_at timestamp with time zone,
    login_window_days integer DEFAULT 30
);


ALTER TABLE public.client_config OWNER TO chaitanya;

--
-- Name: TABLE client_config; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.client_config IS 'Per-tenant client configuration with UI-driven dynamic parameters';


--
-- Name: COLUMN client_config.login_window_days; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON COLUMN public.client_config.login_window_days IS 'Days of login inactivity before a customer is eligible to be flagged churned. Combined with churn_window_days in mv_customer_features: a customer is churned only if BOTH thresholds are exceeded.';


--
-- Name: client_config_config_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.client_config_config_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.client_config_config_id_seq OWNER TO chaitanya;

--
-- Name: client_config_config_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.client_config_config_id_seq OWNED BY public.client_config.config_id;


--
-- Name: customer_price_context; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.customer_price_context (
    context_id bigint NOT NULL,
    customer_id character varying(100) NOT NULL,
    client_id character varying(50) NOT NULL,
    product_name text NOT NULL,
    strategy character varying(30),
    suggested_price numeric(12,2),
    pre_retention_price numeric(12,2),
    discount_pct_applied numeric(6,2),
    churn_probability numeric(5,4),
    risk_tier character varying(10),
    run_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.customer_price_context OWNER TO chaitanya;

--
-- Name: TABLE customer_price_context; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.customer_price_context IS 'Strategist Agent — per-customer price suggestion from latest run';


--
-- Name: customer_price_context_context_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.customer_price_context_context_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.customer_price_context_context_id_seq OWNER TO chaitanya;

--
-- Name: customer_price_context_context_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.customer_price_context_context_id_seq OWNED BY public.customer_price_context.context_id;


--
-- Name: customer_purchase_cycles; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.customer_purchase_cycles (
    cycle_id integer NOT NULL,
    client_id character varying(20) NOT NULL,
    customer_id character varying(30) NOT NULL,
    product_id integer NOT NULL,
    purchase_count integer DEFAULT 0,
    first_purchase_date date,
    last_purchase_date date,
    avg_refill_days numeric(8,1),
    expected_next_date date,
    days_overdue integer,
    missed_refill_count integer DEFAULT 0,
    is_active_subscriber boolean DEFAULT true,
    computed_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.customer_purchase_cycles OWNER TO chaitanya;

--
-- Name: TABLE customer_purchase_cycles; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.customer_purchase_cycles IS 'Per-customer per-product refill pattern tracking';


--
-- Name: customer_purchase_cycles_cycle_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.customer_purchase_cycles_cycle_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.customer_purchase_cycles_cycle_id_seq OWNER TO chaitanya;

--
-- Name: customer_purchase_cycles_cycle_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.customer_purchase_cycles_cycle_id_seq OWNED BY public.customer_purchase_cycles.cycle_id;


--
-- Name: customer_reviews; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.customer_reviews (
    client_id character varying(20) NOT NULL,
    review_id character varying(30) NOT NULL,
    customer_id character varying(30) NOT NULL,
    product_id integer,
    order_id character varying(50),
    rating smallint,
    review_text text,
    review_date date,
    sentiment character varying(20),
    sentiment_score numeric(6,4),
    CONSTRAINT customer_reviews_rating_check CHECK (((rating >= 1) AND (rating <= 5)))
);


ALTER TABLE public.customer_reviews OWNER TO chaitanya;

--
-- Name: TABLE customer_reviews; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.customer_reviews IS 'Customer product ratings and review text';


--
-- Name: customer_rfm_features; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.customer_rfm_features (
    client_id character varying(20) NOT NULL,
    customer_id character varying(30) NOT NULL,
    computed_at timestamp with time zone DEFAULT now(),
    days_since_last_order integer,
    last_order_date date,
    last_order_status character varying(30),
    total_orders integer DEFAULT 0,
    orders_last_30d integer DEFAULT 0,
    orders_last_90d integer DEFAULT 0,
    orders_last_180d integer DEFAULT 0,
    avg_orders_per_month numeric(6,2),
    order_frequency_trend character varying(20),
    total_spend_usd numeric(12,2) DEFAULT 0,
    avg_order_value_usd numeric(10,2),
    spend_last_90d_usd numeric(12,2) DEFAULT 0,
    spend_last_180d_usd numeric(12,2) DEFAULT 0,
    ltv_usd numeric(12,2),
    spend_trend character varying(20),
    recency_score smallint,
    frequency_score smallint,
    monetary_score smallint,
    rfm_total_score smallint,
    rfm_segment character varying(50),
    total_items_purchased integer DEFAULT 0,
    unique_products_bought integer DEFAULT 0,
    top_category character varying(100),
    return_rate_pct numeric(5,2),
    total_discounts_used integer DEFAULT 0,
    total_discount_usd numeric(10,2) DEFAULT 0,
    discount_dependency_pct numeric(5,2),
    account_age_days integer,
    customer_tier character varying(20)
);


ALTER TABLE public.customer_rfm_features OWNER TO chaitanya;

--
-- Name: TABLE customer_rfm_features; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.customer_rfm_features IS 'Computed RFM + engagement features — refreshed nightly';


--
-- Name: customers; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.customers (
    client_id character varying(20) NOT NULL,
    customer_id character varying(30) NOT NULL,
    customer_email character varying(150),
    customer_name character varying(100),
    customer_phone character varying(30),
    account_created_date date,
    registration_channel character varying(100),
    country_code character varying(5) DEFAULT 'US'::character varying,
    state character varying(5),
    city character varying(100),
    zip_code character varying(20),
    shipping_address text,
    preferred_device character varying(50),
    email_opt_in boolean DEFAULT true,
    sms_opt_in boolean DEFAULT false,
    last_login_date date
);


ALTER TABLE public.customers OWNER TO chaitanya;

--
-- Name: TABLE customers; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.customers IS 'Customer master — one row per unique customer per client';


--
-- Name: COLUMN customers.last_login_date; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON COLUMN public.customers.last_login_date IS 'Most recent login (any session, ordered or not). Used by Phase 2 to compute days_since_last_login and apply the two-condition churn rule.';


--
-- Name: entities; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.entities (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    canonical_name text NOT NULL,
    canonical_brand text,
    canonical_variant text,
    query text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.entities OWNER TO chaitanya;

--
-- Name: TABLE entities; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.entities IS 'Canonical products tracked for competitive intelligence';


--
-- Name: entity_listings; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.entity_listings (
    id integer NOT NULL,
    entity_id uuid NOT NULL,
    platform text NOT NULL,
    product_url text NOT NULL,
    title text NOT NULL,
    price numeric(10,2),
    currency text DEFAULT 'INR'::text NOT NULL,
    ingredients text,
    manufacturer text,
    marketed_by text,
    availability text DEFAULT 'unknown'::text,
    last_seen timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.entity_listings OWNER TO chaitanya;

--
-- Name: TABLE entity_listings; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.entity_listings IS 'Per-platform product listings for tracked entities';


--
-- Name: entity_listings_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.entity_listings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.entity_listings_id_seq OWNER TO chaitanya;

--
-- Name: entity_listings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.entity_listings_id_seq OWNED BY public.entity_listings.id;


--
-- Name: line_items; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.line_items (
    client_id character varying(20) NOT NULL,
    line_item_id character varying(30) NOT NULL,
    order_id character varying(50) NOT NULL,
    customer_id character varying(30) NOT NULL,
    product_id integer NOT NULL,
    quantity integer DEFAULT 1 NOT NULL,
    unit_price_usd numeric(10,2),
    final_line_total_usd numeric(10,2),
    item_discount_usd numeric(10,2) DEFAULT 0,
    item_status character varying(30)
);


ALTER TABLE public.line_items OWNER TO chaitanya;

--
-- Name: TABLE line_items; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.line_items IS 'Order line items — one row per product per order';


--
-- Name: llm_cost_log; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.llm_cost_log (
    id integer NOT NULL,
    client_id character varying(20) NOT NULL,
    call_type character varying(50) NOT NULL,
    model character varying(100) NOT NULL,
    input_tokens integer DEFAULT 0 NOT NULL,
    output_tokens integer DEFAULT 0 NOT NULL,
    total_tokens integer DEFAULT 0 NOT NULL,
    input_cost_usd numeric(12,8) DEFAULT 0 NOT NULL,
    output_cost_usd numeric(12,8) DEFAULT 0 NOT NULL,
    total_cost_usd numeric(12,8) DEFAULT 0 NOT NULL,
    over_budget boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.llm_cost_log OWNER TO chaitanya;

--
-- Name: TABLE llm_cost_log; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.llm_cost_log IS 'Cost Tracking — per-call LLM usage + dollar cost';


--
-- Name: llm_cost_log_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.llm_cost_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.llm_cost_log_id_seq OWNER TO chaitanya;

--
-- Name: llm_cost_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.llm_cost_log_id_seq OWNED BY public.llm_cost_log.id;


--
-- Name: message_templates; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.message_templates (
    id character varying(20) NOT NULL,
    client_id character varying(20) NOT NULL,
    tier_name character varying(20) NOT NULL,
    risk_level character varying(20) NOT NULL,
    discount_pct numeric(5,2) DEFAULT 0,
    channel character varying(50) DEFAULT 'email'::character varying,
    action_type character varying(100) DEFAULT ''::character varying,
    message_template text DEFAULT ''::text,
    priority integer DEFAULT 5,
    subject text DEFAULT ''::text,
    body text DEFAULT ''::text,
    active boolean DEFAULT true,
    updated_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.message_templates OWNER TO chaitanya;

--
-- Name: orders; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.orders (
    client_id character varying(20) NOT NULL,
    order_id character varying(50) NOT NULL,
    customer_id character varying(30) NOT NULL,
    order_date timestamp with time zone,
    order_status character varying(30),
    order_value_usd numeric(10,2),
    discount_usd numeric(10,2) DEFAULT 0,
    coupon_code character varying(50),
    payment_method character varying(50),
    order_item_count integer
);


ALTER TABLE public.orders OWNER TO chaitanya;

--
-- Name: TABLE orders; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.orders IS 'Order header — one row per order';


--
-- Name: products; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.products (
    product_id integer NOT NULL,
    sku character varying(50) NOT NULL,
    product_name character varying(200) NOT NULL,
    category_id integer,
    sub_category_id integer,
    sub_sub_category_id integer,
    brand_id integer,
    product_price_id integer,
    rating numeric(3,1),
    active smallint DEFAULT 1,
    not_available smallint DEFAULT 0,
    client_id character varying(20) NOT NULL
);


ALTER TABLE public.products OWNER TO chaitanya;

--
-- Name: support_tickets; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.support_tickets (
    client_id character varying(20) NOT NULL,
    ticket_id character varying(30) NOT NULL,
    customer_id character varying(30) NOT NULL,
    ticket_type character varying(100),
    priority character varying(20),
    status character varying(30),
    channel character varying(50),
    opened_date timestamp with time zone,
    resolved_date timestamp with time zone,
    resolution_time_hrs numeric(8,2)
);


ALTER TABLE public.support_tickets OWNER TO chaitanya;

--
-- Name: TABLE support_tickets; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.support_tickets IS 'Customer support ticket log';


--
-- Name: vw_subscription_products; Type: VIEW; Schema: public; Owner: chaitanya
--

CREATE VIEW public.vw_subscription_products AS
 WITH keyword_flag AS (
         SELECT p.client_id,
            p.product_id,
            p.product_name,
                CASE
                    WHEN (lower((p.product_name)::text) ~~ ANY (ARRAY['%refill%'::text, '%subscription%'::text, '%monthly%'::text, '%daily%'::text, '%vitamin%'::text, '%supplement%'::text, '%tablet%'::text, '%capsule%'::text, '%mg %'::text, '% mg%'::text, '%dose%'::text, '%pill%'::text, '%softgel%'::text, '%gummy%'::text, '%probiotic%'::text, '%omega%'::text, '%protein%'::text, '%insulin%'::text, '%inhaler%'::text, '%drops%'::text, '%syrup%'::text, '%pack of%'::text, '%count)%'::text, '%supply%'::text])) THEN true
                    ELSE false
                END AS is_subscription_by_name
           FROM public.products p
        ), repeat_counts AS (
         SELECT li.client_id,
            li.customer_id,
            li.product_id,
            count(*) AS purchase_count
           FROM public.line_items li
          GROUP BY li.client_id, li.customer_id, li.product_id
        ), purchase_gaps AS (
         SELECT li.client_id,
            li.customer_id,
            li.product_id,
            o.order_date,
            EXTRACT(day FROM (o.order_date - lag(o.order_date) OVER (PARTITION BY li.client_id, li.customer_id, li.product_id ORDER BY o.order_date))) AS gap_days
           FROM (public.line_items li
             JOIN public.orders o ON ((((li.client_id)::text = (o.client_id)::text) AND ((li.order_id)::text = (o.order_id)::text))))
        ), avg_gaps AS (
         SELECT purchase_gaps.client_id,
            purchase_gaps.customer_id,
            purchase_gaps.product_id,
            avg(purchase_gaps.gap_days) AS avg_gap
           FROM purchase_gaps
          WHERE (purchase_gaps.gap_days IS NOT NULL)
          GROUP BY purchase_gaps.client_id, purchase_gaps.customer_id, purchase_gaps.product_id
        ), behaviour_flag AS (
         SELECT li.client_id,
            li.product_id,
            count(DISTINCT li.customer_id) AS total_buyers,
            count(DISTINCT
                CASE
                    WHEN (rc.purchase_count >= 3) THEN li.customer_id
                    ELSE NULL::character varying
                END) AS repeat_buyers,
            round(avg(ag.avg_gap), 1) AS avg_refill_days,
            round(stddev(ag.avg_gap), 1) AS stddev_refill_days
           FROM ((public.line_items li
             LEFT JOIN repeat_counts rc ON ((((li.client_id)::text = (rc.client_id)::text) AND ((li.customer_id)::text = (rc.customer_id)::text) AND (li.product_id = rc.product_id))))
             LEFT JOIN avg_gaps ag ON ((((li.client_id)::text = (ag.client_id)::text) AND ((li.customer_id)::text = (ag.customer_id)::text) AND (li.product_id = ag.product_id))))
          GROUP BY li.client_id, li.product_id
        ), combined AS (
         SELECT p.client_id,
            p.product_id,
            p.product_name,
            p.category_id,
            kf.is_subscription_by_name,
            COALESCE(bf.repeat_buyers, (0)::bigint) AS repeat_buyers,
            COALESCE(bf.total_buyers, (0)::bigint) AS total_buyers,
            COALESCE(bf.avg_refill_days, (0)::numeric) AS avg_refill_days,
            COALESCE(bf.stddev_refill_days, (0)::numeric) AS stddev_refill_days,
                CASE
                    WHEN ((bf.total_buyers > 0) AND ((((bf.repeat_buyers)::numeric * 1.0) / (bf.total_buyers)::numeric) >= 0.30) AND (COALESCE(bf.stddev_refill_days, (999)::numeric) < (15)::numeric)) THEN true
                    ELSE false
                END AS is_subscription_by_behaviour
           FROM ((public.products p
             LEFT JOIN keyword_flag kf ON ((((p.client_id)::text = (kf.client_id)::text) AND (p.product_id = kf.product_id))))
             LEFT JOIN behaviour_flag bf ON ((((p.client_id)::text = (bf.client_id)::text) AND (p.product_id = bf.product_id))))
        )
 SELECT client_id,
    product_id,
    product_name,
    category_id,
    is_subscription_by_name,
    is_subscription_by_behaviour,
    avg_refill_days,
    repeat_buyers,
    total_buyers,
    (is_subscription_by_name OR is_subscription_by_behaviour) AS is_subscription_product,
        CASE
            WHEN (is_subscription_by_name AND is_subscription_by_behaviour) THEN 'both'::text
            WHEN is_subscription_by_name THEN 'keyword'::text
            WHEN is_subscription_by_behaviour THEN 'behaviour'::text
            ELSE 'none'::text
        END AS detection_source
   FROM combined;


ALTER VIEW public.vw_subscription_products OWNER TO chaitanya;

--
-- Name: mv_customer_features; Type: MATERIALIZED VIEW; Schema: public; Owner: chaitanya
--

CREATE MATERIALIZED VIEW public.mv_customer_features AS
 WITH client_ref AS (
         SELECT client_config.client_id,
            client_config.churn_window_days,
            client_config.login_window_days,
            client_config.min_repeat_orders,
            client_config.high_value_percentile,
            client_config.recent_order_gap_window,
            client_config.tier_method,
            client_config.custom_platinum_min,
            client_config.custom_gold_min,
            client_config.custom_silver_min,
            client_config.custom_bronze_min,
                CASE
                    WHEN (((client_config.reference_date_mode)::text = 'fixed'::text) AND (client_config.reference_date IS NOT NULL)) THEN (client_config.reference_date)::timestamp with time zone
                    ELSE now()
                END AS ref_date
           FROM public.client_config
        ), order_returns AS (
         SELECT li.client_id,
            li.order_id,
            sum(COALESCE(li.final_line_total_usd, (li.unit_price_usd * (li.quantity)::numeric), (0)::numeric)) AS returned_value
           FROM public.line_items li
          WHERE ((li.item_status)::text = 'Returned'::text)
          GROUP BY li.client_id, li.order_id
        ), order_agg AS (
         SELECT o.client_id,
            o.customer_id,
            count(*) AS total_orders,
            min(o.order_date) AS first_order_date,
            max(o.order_date) AS last_order_date,
            (EXTRACT(day FROM (cr_1.ref_date - max(o.order_date))))::integer AS days_since_last_order,
            sum(GREATEST((0)::numeric, (o.order_value_usd - COALESCE(oret.returned_value, (0)::numeric)))) AS total_spend_usd,
            round(avg(GREATEST((0)::numeric, (o.order_value_usd - COALESCE(oret.returned_value, (0)::numeric)))), 2) AS avg_order_value_usd,
            max(GREATEST((0)::numeric, (o.order_value_usd - COALESCE(oret.returned_value, (0)::numeric)))) AS max_order_value_usd,
            COALESCE(sum(o.discount_usd), (0)::numeric) AS total_discount_usd,
            sum(
                CASE
                    WHEN (o.order_date >= (cr_1.ref_date - '30 days'::interval)) THEN GREATEST((0)::numeric, (o.order_value_usd - COALESCE(oret.returned_value, (0)::numeric)))
                    ELSE (0)::numeric
                END) AS spend_last_30d_usd,
            sum(
                CASE
                    WHEN (o.order_date >= (cr_1.ref_date - '90 days'::interval)) THEN GREATEST((0)::numeric, (o.order_value_usd - COALESCE(oret.returned_value, (0)::numeric)))
                    ELSE (0)::numeric
                END) AS spend_last_90d_usd,
            sum(
                CASE
                    WHEN (o.order_date >= (cr_1.ref_date - '180 days'::interval)) THEN GREATEST((0)::numeric, (o.order_value_usd - COALESCE(oret.returned_value, (0)::numeric)))
                    ELSE (0)::numeric
                END) AS spend_last_180d_usd,
            count(
                CASE
                    WHEN (o.order_date >= (cr_1.ref_date - '30 days'::interval)) THEN 1
                    ELSE NULL::integer
                END) AS orders_last_30d,
            count(
                CASE
                    WHEN (o.order_date >= (cr_1.ref_date - '90 days'::interval)) THEN 1
                    ELSE NULL::integer
                END) AS orders_last_90d,
            count(
                CASE
                    WHEN (o.order_date >= (cr_1.ref_date - '180 days'::interval)) THEN 1
                    ELSE NULL::integer
                END) AS orders_last_180d,
            count(
                CASE
                    WHEN (o.discount_usd > (0)::numeric) THEN 1
                    ELSE NULL::integer
                END) AS orders_with_discount,
            sum(
                CASE
                    WHEN (o.order_date >= (cr_1.ref_date - '90 days'::interval)) THEN COALESCE(o.discount_usd, (0)::numeric)
                    ELSE (0)::numeric
                END) AS discount_last_90d_usd,
            count(
                CASE
                    WHEN ((o.order_date >= (cr_1.ref_date - '90 days'::interval)) AND (o.discount_usd > (0)::numeric)) THEN 1
                    ELSE NULL::integer
                END) AS orders_with_discount_last_90d
           FROM ((public.orders o
             JOIN client_ref cr_1 ON (((o.client_id)::text = (cr_1.client_id)::text)))
             LEFT JOIN order_returns oret ON ((((o.client_id)::text = (oret.client_id)::text) AND ((o.order_id)::text = (oret.order_id)::text))))
          WHERE ((o.order_status)::text <> ALL (ARRAY['Cancelled'::text, 'Returned'::text]))
          GROUP BY o.client_id, o.customer_id, cr_1.ref_date
        ), order_gaps AS (
         SELECT gaps.client_id,
            gaps.customer_id,
            round(avg(gaps.gap_days), 1) AS avg_days_between_orders,
                CASE
                    WHEN (count(gaps.gap_days) >= 2) THEN round((percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((gaps.gap_days)::double precision)))::numeric, 1)
                    ELSE NULL::numeric
                END AS median_days_between_orders
           FROM ( SELECT orders.client_id,
                    orders.customer_id,
                    EXTRACT(day FROM (orders.order_date - lag(orders.order_date) OVER (PARTITION BY orders.client_id, orders.customer_id ORDER BY orders.order_date))) AS gap_days
                   FROM public.orders
                  WHERE ((orders.order_status)::text <> ALL (ARRAY['Cancelled'::text, 'Returned'::text]))) gaps
          WHERE (gaps.gap_days IS NOT NULL)
          GROUP BY gaps.client_id, gaps.customer_id
        ), line_agg AS (
         SELECT li.client_id,
            li.customer_id,
            count(DISTINCT li.product_id) AS unique_products_purchased,
            round(avg(li.quantity), 2) AS avg_items_per_order,
            round((((count(
                CASE
                    WHEN ((li.item_status)::text = 'Returned'::text) THEN 1
                    ELSE NULL::integer
                END))::numeric * 100.0) / (NULLIF(count(*), 0))::numeric), 1) AS return_rate_pct,
            round(avg(
                CASE
                    WHEN (o.order_date >= (cr_1.ref_date - '90 days'::interval)) THEN li.quantity
                    ELSE NULL::integer
                END), 2) AS avg_items_per_order_last_90d
           FROM ((public.line_items li
             JOIN public.orders o ON ((((li.client_id)::text = (o.client_id)::text) AND ((li.order_id)::text = (o.order_id)::text))))
             JOIN client_ref cr_1 ON (((li.client_id)::text = (cr_1.client_id)::text)))
          WHERE ((o.order_status)::text <> ALL (ARRAY['Cancelled'::text, 'Returned'::text]))
          GROUP BY li.client_id, li.customer_id
        ), cat_agg AS (
         SELECT li.client_id,
            li.customer_id,
            count(DISTINCT p.category_id) AS unique_categories_purchased
           FROM (public.line_items li
             JOIN public.products p ON ((((li.client_id)::text = (p.client_id)::text) AND (li.product_id = p.product_id))))
          GROUP BY li.client_id, li.customer_id
        ), review_agg AS (
         SELECT r.client_id,
            r.customer_id,
            count(*) AS total_reviews,
            round(avg(r.rating), 2) AS avg_rating,
            round((((count(
                CASE
                    WHEN ((r.sentiment)::text = 'positive'::text) THEN 1
                    ELSE NULL::integer
                END))::numeric * 100.0) / (NULLIF(count(*), 0))::numeric), 1) AS pct_positive_reviews,
            round((((count(
                CASE
                    WHEN ((r.sentiment)::text = 'negative'::text) THEN 1
                    ELSE NULL::integer
                END))::numeric * 100.0) / (NULLIF(count(*), 0))::numeric), 1) AS pct_negative_reviews,
            max(r.review_date) AS last_review_date,
            (EXTRACT(day FROM (cr_1.ref_date - max((r.review_date)::timestamp with time zone))))::integer AS days_since_last_review,
            count(
                CASE
                    WHEN (r.review_date >= ((cr_1.ref_date - '90 days'::interval))::date) THEN 1
                    ELSE NULL::integer
                END) AS reviews_last_90d,
            round(avg(r.sentiment_score), 3) AS avg_sentiment_score,
            round(avg(
                CASE
                    WHEN (r.review_date >= ((cr_1.ref_date - '90 days'::interval))::date) THEN r.sentiment_score
                    ELSE NULL::numeric
                END), 3) AS avg_sentiment_score_last_90d,
            count(
                CASE
                    WHEN (((r.sentiment)::text = 'negative'::text) AND (r.review_date >= ((cr_1.ref_date - '90 days'::interval))::date)) THEN 1
                    ELSE NULL::integer
                END) AS negative_reviews_last_90d,
            count(
                CASE
                    WHEN ((r.rating <= 2) AND (r.review_date >= ((cr_1.ref_date - '90 days'::interval))::date)) THEN 1
                    ELSE NULL::integer
                END) AS low_star_reviews_last_90d,
            (EXTRACT(day FROM (cr_1.ref_date - max(
                CASE
                    WHEN ((r.sentiment)::text = 'negative'::text) THEN (r.review_date)::timestamp with time zone
                    ELSE NULL::timestamp with time zone
                END))))::integer AS days_since_last_negative_review
           FROM (public.customer_reviews r
             JOIN client_ref cr_1 ON (((r.client_id)::text = (cr_1.client_id)::text)))
          GROUP BY r.client_id, r.customer_id, cr_1.ref_date
        ), ticket_agg AS (
         SELECT t.client_id,
            t.customer_id,
            count(*) AS total_tickets,
            count(
                CASE
                    WHEN (lower((t.status)::text) = 'open'::text) THEN 1
                    ELSE NULL::integer
                END) AS open_tickets,
            count(
                CASE
                    WHEN (lower((t.priority)::text) = 'critical'::text) THEN 1
                    ELSE NULL::integer
                END) AS critical_tickets,
            round(avg(t.resolution_time_hrs), 1) AS avg_resolution_time_hrs,
            round((((count(
                CASE
                    WHEN (lower((t.status)::text) = 'resolved'::text) THEN 1
                    ELSE NULL::integer
                END))::numeric * 100.0) / (NULLIF(count(*), 0))::numeric), 1) AS pct_tickets_resolved,
            count(
                CASE
                    WHEN (t.opened_date >= (cr_1.ref_date - '30 days'::interval)) THEN 1
                    ELSE NULL::integer
                END) AS tickets_last_30d,
            count(
                CASE
                    WHEN (t.opened_date >= (cr_1.ref_date - '90 days'::interval)) THEN 1
                    ELSE NULL::integer
                END) AS tickets_last_90d,
            count(
                CASE
                    WHEN (lower((t.priority)::text) = ANY (ARRAY['high'::text, 'critical'::text])) THEN 1
                    ELSE NULL::integer
                END) AS high_priority_tickets,
            count(
                CASE
                    WHEN (lower((t.status)::text) <> ALL (ARRAY['resolved'::text, 'closed'::text])) THEN 1
                    ELSE NULL::integer
                END) AS unresolved_tickets,
            (EXTRACT(day FROM (cr_1.ref_date - max(t.opened_date))))::integer AS days_since_last_ticket
           FROM (public.support_tickets t
             JOIN client_ref cr_1 ON (((t.client_id)::text = (cr_1.client_id)::text)))
          GROUP BY t.client_id, t.customer_id, cr_1.ref_date
        ), rfm_scored AS (
         SELECT order_agg.client_id,
            order_agg.customer_id,
            (6 - ntile(5) OVER (PARTITION BY order_agg.client_id ORDER BY order_agg.days_since_last_order)) AS rfm_recency_score,
            ntile(5) OVER (PARTITION BY order_agg.client_id ORDER BY order_agg.total_orders) AS rfm_frequency_score,
            ntile(5) OVER (PARTITION BY order_agg.client_id ORDER BY order_agg.total_spend_usd) AS rfm_monetary_score
           FROM order_agg
        ), last_purchase_per_product AS (
         SELECT li.client_id,
            li.customer_id,
            li.product_id,
            max(o.order_date) AS last_purchase_date
           FROM (public.line_items li
             JOIN public.orders o ON ((((li.client_id)::text = (o.client_id)::text) AND ((li.order_id)::text = (o.order_id)::text))))
          WHERE ((o.order_status)::text <> ALL (ARRAY['Cancelled'::text, 'Returned'::text]))
          GROUP BY li.client_id, li.customer_id, li.product_id
        ), subscription_agg AS (
         SELECT lp.client_id,
            lp.customer_id,
            count(DISTINCT lp.product_id) AS subscription_product_count,
            round(avg(sp.avg_refill_days), 1) AS avg_refill_cycle_days,
            (max(EXTRACT(day FROM (cr_1.ref_date - (lp.last_purchase_date + (((sp.avg_refill_days)::text || ' days'::text))::interval)))))::integer AS days_overdue_for_refill,
            sum(
                CASE
                    WHEN (EXTRACT(day FROM (cr_1.ref_date - lp.last_purchase_date)) > (sp.avg_refill_days * 1.5)) THEN 1
                    ELSE 0
                END) AS missed_refill_count
           FROM ((last_purchase_per_product lp
             JOIN public.vw_subscription_products sp ON ((((lp.client_id)::text = (sp.client_id)::text) AND (lp.product_id = sp.product_id) AND (sp.is_subscription_product = true))))
             JOIN client_ref cr_1 ON (((lp.client_id)::text = (cr_1.client_id)::text)))
          GROUP BY lp.client_id, lp.customer_id, cr_1.ref_date
        ), repeat_flag AS (
         SELECT oa_1.client_id,
            oa_1.customer_id,
                CASE
                    WHEN (oa_1.total_orders >= cr_1.min_repeat_orders) THEN 1
                    ELSE 0
                END AS is_repeat_customer
           FROM (order_agg oa_1
             JOIN client_ref cr_1 ON (((oa_1.client_id)::text = (cr_1.client_id)::text)))
        ), recent_gaps AS (
         SELECT ranked.client_id,
            ranked.customer_id,
            round(avg(ranked.gap_days), 1) AS recent_avg_gap_days
           FROM ( SELECT g.client_id,
                    g.customer_id,
                    g.gap_days,
                    row_number() OVER (PARTITION BY g.client_id, g.customer_id ORDER BY g.order_date DESC) AS rn,
                    cr_1.recent_order_gap_window
                   FROM (( SELECT orders.client_id,
                            orders.customer_id,
                            orders.order_date,
                            EXTRACT(day FROM (orders.order_date - lag(orders.order_date) OVER (PARTITION BY orders.client_id, orders.customer_id ORDER BY orders.order_date))) AS gap_days
                           FROM public.orders
                          WHERE ((orders.order_status)::text <> ALL (ARRAY['Cancelled'::text, 'Returned'::text]))) g
                     JOIN client_ref cr_1 ON (((g.client_id)::text = (cr_1.client_id)::text)))
                  WHERE (g.gap_days IS NOT NULL)) ranked
          WHERE (ranked.rn <= ranked.recent_order_gap_window)
          GROUP BY ranked.client_id, ranked.customer_id
        ), spend_percentiles AS (
         SELECT oa_1.client_id,
            oa_1.customer_id,
            oa_1.total_spend_usd,
            (percent_rank() OVER (PARTITION BY oa_1.client_id ORDER BY oa_1.total_spend_usd) * (100)::double precision) AS spend_pct_rank
           FROM order_agg oa_1
        ), tier_assignment AS (
         SELECT sp.client_id,
            sp.customer_id,
                CASE
                    WHEN ((cr_1.tier_method)::text = 'quartile'::text) THEN
                    CASE
                        WHEN (sp.spend_pct_rank >= (75)::double precision) THEN 'Platinum'::text
                        WHEN (sp.spend_pct_rank >= (50)::double precision) THEN 'Gold'::text
                        WHEN (sp.spend_pct_rank >= (25)::double precision) THEN 'Silver'::text
                        ELSE 'Bronze'::text
                    END
                    ELSE
                    CASE
                        WHEN (sp.total_spend_usd >= cr_1.custom_platinum_min) THEN 'Platinum'::text
                        WHEN (sp.total_spend_usd >= cr_1.custom_gold_min) THEN 'Gold'::text
                        WHEN (sp.total_spend_usd >= cr_1.custom_silver_min) THEN 'Silver'::text
                        ELSE 'Bronze'::text
                    END
                END AS customer_tier,
                CASE
                    WHEN (((cr_1.tier_method)::text = 'quartile'::text) AND (sp.spend_pct_rank >= (cr_1.high_value_percentile)::double precision)) THEN 1
                    WHEN (((cr_1.tier_method)::text <> 'quartile'::text) AND (sp.total_spend_usd >= cr_1.custom_platinum_min)) THEN 1
                    ELSE 0
                END AS is_high_value
           FROM (spend_percentiles sp
             JOIN client_ref cr_1 ON (((sp.client_id)::text = (cr_1.client_id)::text)))
        )
 SELECT c.client_id,
    c.customer_id,
    (EXTRACT(day FROM (cr.ref_date - (c.account_created_date)::timestamp with time zone)))::integer AS account_age_days,
    c.last_login_date,
    (EXTRACT(day FROM (cr.ref_date - GREATEST(COALESCE((c.last_login_date)::timestamp with time zone, '1899-12-31 18:00:00-06'::timestamp with time zone), COALESCE(oa.last_order_date, '1899-12-31 18:00:00-06'::timestamp with time zone)))))::integer AS days_since_last_login,
    oa.first_order_date,
    oa.last_order_date,
    COALESCE(oa.days_since_last_order, 9999) AS days_since_last_order,
    COALESCE(oa.total_orders, (0)::bigint) AS total_orders,
    COALESCE(oa.orders_last_30d, (0)::bigint) AS orders_last_30d,
    COALESCE(oa.orders_last_90d, (0)::bigint) AS orders_last_90d,
    COALESCE(oa.orders_last_180d, (0)::bigint) AS orders_last_180d,
    COALESCE(og.avg_days_between_orders, (0)::numeric) AS avg_days_between_orders,
    og.median_days_between_orders,
        CASE
            WHEN (og.median_days_between_orders IS NULL) THEN NULL::numeric
            ELSE round(abs((COALESCE(og.avg_days_between_orders, (0)::numeric) - og.median_days_between_orders)), 1)
        END AS order_gap_mean_median_diff,
    COALESCE(rg.recent_avg_gap_days, (0)::numeric) AS recent_avg_gap_days,
    COALESCE(oa.total_spend_usd, (0)::numeric) AS total_spend_usd,
    COALESCE(oa.avg_order_value_usd, (0)::numeric) AS avg_order_value_usd,
    COALESCE(oa.max_order_value_usd, (0)::numeric) AS max_order_value_usd,
    COALESCE(oa.spend_last_30d_usd, (0)::numeric) AS spend_last_30d_usd,
    COALESCE(oa.spend_last_90d_usd, (0)::numeric) AS spend_last_90d_usd,
    COALESCE(oa.spend_last_180d_usd, (0)::numeric) AS spend_last_180d_usd,
    COALESCE(oa.total_discount_usd, (0)::numeric) AS total_discount_usd,
    LEAST((100)::numeric, round(((COALESCE(oa.total_discount_usd, (0)::numeric) * 100.0) / NULLIF((COALESCE(oa.total_spend_usd, (0)::numeric) + COALESCE(oa.total_discount_usd, (0)::numeric)), (0)::numeric)), 2)) AS discount_rate_pct,
    COALESCE(oa.orders_with_discount, (0)::bigint) AS orders_with_discount,
    COALESCE(la.unique_products_purchased, (0)::bigint) AS unique_products_purchased,
    COALESCE(ca.unique_categories_purchased, (0)::bigint) AS unique_categories_purchased,
    COALESCE(la.avg_items_per_order, (0)::numeric) AS avg_items_per_order,
    COALESCE(la.return_rate_pct, (0)::numeric) AS return_rate_pct,
    COALESCE(ra.total_reviews, (0)::bigint) AS total_reviews,
    COALESCE(ra.avg_rating, (0)::numeric) AS avg_rating,
    COALESCE(ra.pct_positive_reviews, (0)::numeric) AS pct_positive_reviews,
    COALESCE(ra.pct_negative_reviews, (0)::numeric) AS pct_negative_reviews,
    ra.last_review_date,
    COALESCE(ra.days_since_last_review, 9999) AS days_since_last_review,
    COALESCE(ra.reviews_last_90d, (0)::bigint) AS reviews_last_90d,
    COALESCE(ra.avg_sentiment_score, (0)::numeric) AS avg_sentiment_score,
    COALESCE(ra.avg_sentiment_score_last_90d, (0)::numeric) AS avg_sentiment_score_last_90d,
    COALESCE(ra.negative_reviews_last_90d, (0)::bigint) AS negative_reviews_last_90d,
    COALESCE(ra.low_star_reviews_last_90d, (0)::bigint) AS low_star_reviews_last_90d,
    COALESCE(ra.days_since_last_negative_review, 9999) AS days_since_last_negative_review,
    COALESCE(ta.total_tickets, (0)::bigint) AS total_tickets,
    COALESCE(ta.open_tickets, (0)::bigint) AS open_tickets,
    COALESCE(ta.critical_tickets, (0)::bigint) AS critical_tickets,
    COALESCE(ta.avg_resolution_time_hrs, (0)::numeric) AS avg_resolution_time_hrs,
    COALESCE(ta.pct_tickets_resolved, (0)::numeric) AS pct_tickets_resolved,
    COALESCE(ta.tickets_last_30d, (0)::bigint) AS tickets_last_30d,
    COALESCE(ta.tickets_last_90d, (0)::bigint) AS tickets_last_90d,
    COALESCE(ta.high_priority_tickets, (0)::bigint) AS high_priority_tickets,
    COALESCE(ta.unresolved_tickets, (0)::bigint) AS unresolved_tickets,
    COALESCE(ta.days_since_last_ticket, 9999) AS days_since_last_ticket,
        CASE
            WHEN (COALESCE(oa.orders_last_90d, (0)::bigint) = 0) THEN NULL::numeric
            ELSE round((COALESCE(oa.spend_last_90d_usd, (0)::numeric) / (oa.orders_last_90d)::numeric), 2)
        END AS avg_order_value_last_90d_usd,
        CASE
            WHEN (COALESCE(oa.orders_last_90d, (0)::bigint) = 0) THEN NULL::numeric
            WHEN (COALESCE(oa.avg_order_value_usd, (0)::numeric) = (0)::numeric) THEN NULL::numeric
            ELSE round(((((oa.spend_last_90d_usd / (oa.orders_last_90d)::numeric) / oa.avg_order_value_usd) - (1)::numeric) * (100)::numeric), 2)
        END AS aov_trend_pct,
    COALESCE(la.avg_items_per_order_last_90d, (0)::numeric) AS avg_items_per_order_last_90d,
        CASE
            WHEN (la.avg_items_per_order_last_90d IS NULL) THEN NULL::numeric
            WHEN (COALESCE(la.avg_items_per_order, (0)::numeric) = (0)::numeric) THEN NULL::numeric
            ELSE round((((la.avg_items_per_order_last_90d / la.avg_items_per_order) - (1)::numeric) * (100)::numeric), 2)
        END AS basket_size_trend_pct,
    COALESCE(oa.orders_with_discount_last_90d, (0)::bigint) AS orders_with_discount_last_90d,
        CASE
            WHEN (COALESCE(oa.orders_last_90d, (0)::bigint) = 0) THEN NULL::numeric
            ELSE round((((oa.orders_with_discount_last_90d)::numeric * 100.0) / (oa.orders_last_90d)::numeric), 2)
        END AS pct_orders_discounted_last_90d,
        CASE
            WHEN ((COALESCE(oa.spend_last_90d_usd, (0)::numeric) + COALESCE(oa.discount_last_90d_usd, (0)::numeric)) = (0)::numeric) THEN NULL::numeric
            ELSE LEAST((100)::numeric, round(((oa.discount_last_90d_usd * 100.0) / (oa.spend_last_90d_usd + oa.discount_last_90d_usd)), 2))
        END AS discount_rate_last_90d_pct,
        CASE
            WHEN (COALESCE(og.avg_days_between_orders, (0)::numeric) = (0)::numeric) THEN NULL::numeric
            WHEN (COALESCE(rg.recent_avg_gap_days, (0)::numeric) = (0)::numeric) THEN NULL::numeric
            ELSE round((((rg.recent_avg_gap_days - og.avg_days_between_orders) / og.avg_days_between_orders) * (100)::numeric), 1)
        END AS order_gap_inflation_pct,
        CASE
            WHEN (COALESCE(oa.spend_last_90d_usd, (0)::numeric) = (0)::numeric) THEN NULL::numeric
            ELSE round(((oa.spend_last_30d_usd * 3.0) / oa.spend_last_90d_usd), 3)
        END AS spend_velocity_ratio,
    COALESCE(oa.total_spend_usd, (0)::numeric) AS ltv_usd,
    COALESCE(rf.rfm_recency_score, 1) AS rfm_recency_score,
    COALESCE(rf.rfm_frequency_score, 1) AS rfm_frequency_score,
    COALESCE(rf.rfm_monetary_score, 1) AS rfm_monetary_score,
    ((COALESCE(rf.rfm_recency_score, 1) + COALESCE(rf.rfm_frequency_score, 1)) + COALESCE(rf.rfm_monetary_score, 1)) AS rfm_total_score,
    COALESCE(rpf.is_repeat_customer, 0) AS is_repeat_customer,
    COALESCE(ta2.customer_tier, 'Bronze'::text) AS customer_tier,
    COALESCE(ta2.is_high_value, 0) AS is_high_value,
    COALESCE(sa.subscription_product_count, (0)::bigint) AS subscription_product_count,
    COALESCE(sa.avg_refill_cycle_days, (0)::numeric) AS avg_refill_cycle_days,
    COALESCE(sa.days_overdue_for_refill, 0) AS days_overdue_for_refill,
    COALESCE(sa.missed_refill_count, (0)::bigint) AS missed_refill_count,
        CASE
            WHEN ((EXTRACT(day FROM (cr.ref_date - (c.account_created_date)::timestamp with time zone)))::integer < COALESCE(((sa.avg_refill_cycle_days * (2)::numeric))::integer, cr.churn_window_days)) THEN 0
            WHEN (((oa.last_order_date IS NULL) OR ((EXTRACT(day FROM (cr.ref_date - oa.last_order_date)))::integer >= COALESCE(((sa.avg_refill_cycle_days * (2)::numeric))::integer, cr.churn_window_days))) AND ((EXTRACT(day FROM (cr.ref_date - GREATEST(COALESCE((c.last_login_date)::timestamp with time zone, '1899-12-31 18:00:00-06'::timestamp with time zone), COALESCE(oa.last_order_date, '1899-12-31 18:00:00-06'::timestamp with time zone)))))::integer > COALESCE(((sa.avg_refill_cycle_days * 1.5))::integer, cr.login_window_days))) THEN 1
            ELSE 0
        END AS churn_label,
    cr.ref_date AS computed_at
   FROM ((((((((((((public.customers c
     JOIN client_ref cr ON (((c.client_id)::text = (cr.client_id)::text)))
     LEFT JOIN order_agg oa ON ((((c.client_id)::text = (oa.client_id)::text) AND ((c.customer_id)::text = (oa.customer_id)::text))))
     LEFT JOIN rfm_scored rf ON ((((c.client_id)::text = (rf.client_id)::text) AND ((c.customer_id)::text = (rf.customer_id)::text))))
     LEFT JOIN order_gaps og ON ((((c.client_id)::text = (og.client_id)::text) AND ((c.customer_id)::text = (og.customer_id)::text))))
     LEFT JOIN recent_gaps rg ON ((((c.client_id)::text = (rg.client_id)::text) AND ((c.customer_id)::text = (rg.customer_id)::text))))
     LEFT JOIN line_agg la ON ((((c.client_id)::text = (la.client_id)::text) AND ((c.customer_id)::text = (la.customer_id)::text))))
     LEFT JOIN cat_agg ca ON ((((c.client_id)::text = (ca.client_id)::text) AND ((c.customer_id)::text = (ca.customer_id)::text))))
     LEFT JOIN review_agg ra ON ((((c.client_id)::text = (ra.client_id)::text) AND ((c.customer_id)::text = (ra.customer_id)::text))))
     LEFT JOIN ticket_agg ta ON ((((c.client_id)::text = (ta.client_id)::text) AND ((c.customer_id)::text = (ta.customer_id)::text))))
     LEFT JOIN repeat_flag rpf ON ((((c.client_id)::text = (rpf.client_id)::text) AND ((c.customer_id)::text = (rpf.customer_id)::text))))
     LEFT JOIN tier_assignment ta2 ON ((((c.client_id)::text = (ta2.client_id)::text) AND ((c.customer_id)::text = (ta2.customer_id)::text))))
     LEFT JOIN subscription_agg sa ON ((((c.client_id)::text = (sa.client_id)::text) AND ((c.customer_id)::text = (sa.customer_id)::text))))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.mv_customer_features OWNER TO chaitanya;

--
-- Name: outreach_messages; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.outreach_messages (
    message_id integer NOT NULL,
    client_id character varying(20) NOT NULL,
    customer_id character varying(30) NOT NULL,
    product_id integer,
    message_type character varying(50) NOT NULL,
    trigger_reason character varying(200),
    message_text text NOT NULL,
    channel character varying(30) NOT NULL,
    days_overdue integer,
    discount_offered numeric(5,2),
    sent_at timestamp with time zone DEFAULT now(),
    responded_at timestamp with time zone,
    responded boolean DEFAULT false,
    outcome character varying(50),
    revenue_recovered numeric(10,2)
);


ALTER TABLE public.outreach_messages OWNER TO chaitanya;

--
-- Name: TABLE outreach_messages; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.outreach_messages IS 'Personalised messages sent on churn/refill triggers';


--
-- Name: outreach_messages_message_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.outreach_messages_message_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.outreach_messages_message_id_seq OWNER TO chaitanya;

--
-- Name: outreach_messages_message_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.outreach_messages_message_id_seq OWNED BY public.outreach_messages.message_id;


--
-- Name: pipeline_outputs; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.pipeline_outputs (
    id integer NOT NULL,
    client_id character varying(20) DEFAULT 'CLT-001'::character varying NOT NULL,
    filename character varying(255) NOT NULL,
    title character varying(255),
    icon character varying(10) DEFAULT '📄'::character varying,
    description text,
    category character varying(50) DEFAULT 'other'::character varying,
    mime_type character varying(100) DEFAULT 'application/octet-stream'::character varying,
    file_size integer DEFAULT 0,
    file_content bytea NOT NULL,
    pipeline_run_at timestamp with time zone DEFAULT now()
);


ALTER TABLE public.pipeline_outputs OWNER TO chaitanya;

--
-- Name: pipeline_outputs_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.pipeline_outputs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.pipeline_outputs_id_seq OWNER TO chaitanya;

--
-- Name: pipeline_outputs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.pipeline_outputs_id_seq OWNED BY public.pipeline_outputs.id;


--
-- Name: price_alerts; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.price_alerts (
    id integer NOT NULL,
    product_name text NOT NULL,
    platform text NOT NULL,
    old_price numeric(10,2),
    new_price numeric(10,2) NOT NULL,
    change_amount numeric(10,2),
    change_percent numeric(6,2),
    direction text NOT NULL,
    url text,
    detected_at timestamp with time zone DEFAULT now() NOT NULL,
    acknowledged boolean DEFAULT false NOT NULL
);


ALTER TABLE public.price_alerts OWNER TO chaitanya;

--
-- Name: TABLE price_alerts; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.price_alerts IS 'Automated price change notifications';


--
-- Name: price_alerts_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.price_alerts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.price_alerts_id_seq OWNER TO chaitanya;

--
-- Name: price_alerts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.price_alerts_id_seq OWNED BY public.price_alerts.id;


--
-- Name: price_history; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.price_history (
    id integer NOT NULL,
    product_name text NOT NULL,
    platform text NOT NULL,
    price numeric(10,2) NOT NULL,
    currency text DEFAULT 'INR'::text NOT NULL,
    url text,
    scraped_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.price_history OWNER TO chaitanya;

--
-- Name: TABLE price_history; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.price_history IS 'Historical price snapshots for trend analysis';


--
-- Name: price_history_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.price_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.price_history_id_seq OWNER TO chaitanya;

--
-- Name: price_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.price_history_id_seq OWNED BY public.price_history.id;


--
-- Name: pricing_recommendations; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.pricing_recommendations (
    recommendation_id bigint NOT NULL,
    run_id uuid NOT NULL,
    client_id character varying(50) NOT NULL,
    product_name text NOT NULL,
    suggested_price numeric(12,2),
    pre_retention_price numeric(12,2),
    floor_price numeric(12,2),
    target_price numeric(12,2),
    our_cost numeric(12,2),
    raw_cogs numeric(12,2),
    competitor_min numeric(12,2),
    competitor_avg numeric(12,2),
    competitor_max numeric(12,2),
    competitor_median numeric(12,2),
    strategy character varying(30),
    confidence character varying(10),
    margin_percent numeric(6,2),
    market_trend character varying(10),
    flag character varying(50),
    reasoning text,
    platform_breakdown jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.pricing_recommendations OWNER TO chaitanya;

--
-- Name: TABLE pricing_recommendations; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.pricing_recommendations IS 'Strategist Agent — pricing engine output (one row per run per product)';


--
-- Name: pricing_recommendations_recommendation_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.pricing_recommendations_recommendation_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.pricing_recommendations_recommendation_id_seq OWNER TO chaitanya;

--
-- Name: pricing_recommendations_recommendation_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.pricing_recommendations_recommendation_id_seq OWNED BY public.pricing_recommendations.recommendation_id;


--
-- Name: product_features; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.product_features (
    id integer NOT NULL,
    product_name text NOT NULL,
    platform text NOT NULL,
    category text,
    product_feats jsonb,
    platform_feats jsonb,
    extracted_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.product_features OWNER TO chaitanya;

--
-- Name: TABLE product_features; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.product_features IS 'Extracted product attributes and platform features';


--
-- Name: product_features_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.product_features_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.product_features_id_seq OWNER TO chaitanya;

--
-- Name: product_features_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.product_features_id_seq OWNED BY public.product_features.id;


--
-- Name: product_prices; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.product_prices (
    price_id integer NOT NULL,
    product_id integer NOT NULL,
    qty_range_label character varying(50),
    qty_min integer NOT NULL,
    qty_max integer,
    unit_price_usd numeric(10,2) NOT NULL,
    cost_price_usd numeric(10,2),
    client_id character varying(20) NOT NULL
);


ALTER TABLE public.product_prices OWNER TO chaitanya;

--
-- Name: product_results; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.product_results (
    id integer NOT NULL,
    product_name text NOT NULL,
    scraped_at timestamp with time zone DEFAULT now() NOT NULL,
    product_url text,
    price double precision,
    platform text,
    product_details jsonb,
    title text
);


ALTER TABLE public.product_results OWNER TO chaitanya;

--
-- Name: TABLE product_results; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.product_results IS 'Raw scraped search results from tracked platforms';


--
-- Name: product_results_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.product_results_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.product_results_id_seq OWNER TO chaitanya;

--
-- Name: product_results_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.product_results_id_seq OWNED BY public.product_results.id;


--
-- Name: product_vendor_mapping; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.product_vendor_mapping (
    pv_id integer NOT NULL,
    product_id integer NOT NULL,
    brand_id integer,
    vendor_id integer,
    client_id character varying(20) NOT NULL
);


ALTER TABLE public.product_vendor_mapping OWNER TO chaitanya;

--
-- Name: retention_interventions; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.retention_interventions (
    intervention_id integer NOT NULL,
    client_id character varying(20) NOT NULL,
    customer_id character varying(30) NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    churn_score_id integer,
    churn_probability numeric(5,4),
    risk_tier character varying(10),
    offer_type character varying(100),
    discount_pct numeric(5,2),
    offer_message text,
    channel character varying(50),
    customer_ltv_usd numeric(12,2),
    max_allowed_discount numeric(5,2),
    guardrail_passed boolean DEFAULT true,
    escalated_to_human boolean DEFAULT false,
    offer_status character varying(20) DEFAULT 'pending'::character varying,
    outcome_recorded_at timestamp with time zone,
    revenue_recovered numeric(10,2),
    langfuse_trace_id character varying(100),
    agent_cost_usd numeric(8,6)
);


ALTER TABLE public.retention_interventions OWNER TO chaitanya;

--
-- Name: TABLE retention_interventions; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.retention_interventions IS 'Log of all retention offers sent by the AI agent';


--
-- Name: retention_interventions_intervention_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.retention_interventions_intervention_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.retention_interventions_intervention_id_seq OWNER TO chaitanya;

--
-- Name: retention_interventions_intervention_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.retention_interventions_intervention_id_seq OWNED BY public.retention_interventions.intervention_id;


--
-- Name: staging_brands; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_brands (
    brand_id integer CONSTRAINT brands_brand_id_not_null NOT NULL,
    brand_name character varying(100) CONSTRAINT brands_brand_name_not_null NOT NULL,
    brand_description text,
    vendor_id integer,
    active smallint DEFAULT 1,
    not_available smallint DEFAULT 0,
    category_hint character varying(100),
    client_id character varying(20) CONSTRAINT brands_client_id_not_null NOT NULL,
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL
);


ALTER TABLE public.staging_brands OWNER TO chaitanya;

--
-- Name: staging_brands_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_brands_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_brands_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_brands_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_brands_staging_row_id_seq OWNED BY public.staging_brands.staging_row_id;


--
-- Name: staging_categories; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_categories (
    category_id integer CONSTRAINT categories_category_id_not_null NOT NULL,
    category_name character varying(100) CONSTRAINT categories_category_name_not_null NOT NULL,
    client_id character varying(20) CONSTRAINT categories_client_id_not_null NOT NULL,
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL
);


ALTER TABLE public.staging_categories OWNER TO chaitanya;

--
-- Name: staging_categories_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_categories_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_categories_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_categories_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_categories_staging_row_id_seq OWNED BY public.staging_categories.staging_row_id;


--
-- Name: staging_customer_reviews; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_customer_reviews (
    client_id character varying(20) CONSTRAINT customer_reviews_client_id_not_null NOT NULL,
    review_id character varying(30) CONSTRAINT customer_reviews_review_id_not_null NOT NULL,
    customer_id character varying(30) CONSTRAINT customer_reviews_customer_id_not_null NOT NULL,
    product_id integer,
    order_id character varying(50),
    rating smallint,
    review_text text,
    review_date date,
    sentiment character varying(20),
    sentiment_score numeric(6,4),
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL,
    CONSTRAINT customer_reviews_rating_check CHECK (((rating >= 1) AND (rating <= 5)))
);


ALTER TABLE public.staging_customer_reviews OWNER TO chaitanya;

--
-- Name: staging_customer_reviews_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_customer_reviews_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_customer_reviews_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_customer_reviews_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_customer_reviews_staging_row_id_seq OWNED BY public.staging_customer_reviews.staging_row_id;


--
-- Name: staging_customers; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_customers (
    client_id character varying(20) CONSTRAINT customers_client_id_not_null NOT NULL,
    customer_id character varying(30) CONSTRAINT customers_customer_id_not_null NOT NULL,
    customer_email character varying(150),
    customer_name character varying(100),
    customer_phone character varying(30),
    account_created_date date,
    registration_channel character varying(100),
    country_code character varying(5) DEFAULT 'US'::character varying,
    state character varying(5),
    city character varying(100),
    zip_code character varying(20),
    shipping_address text,
    preferred_device character varying(50),
    email_opt_in boolean DEFAULT true,
    sms_opt_in boolean DEFAULT false,
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL,
    last_login_date date
);


ALTER TABLE public.staging_customers OWNER TO chaitanya;

--
-- Name: staging_customers_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_customers_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_customers_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_customers_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_customers_staging_row_id_seq OWNED BY public.staging_customers.staging_row_id;


--
-- Name: staging_line_items; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_line_items (
    client_id character varying(20) CONSTRAINT line_items_client_id_not_null NOT NULL,
    line_item_id character varying(30) CONSTRAINT line_items_line_item_id_not_null NOT NULL,
    order_id character varying(50) CONSTRAINT line_items_order_id_not_null NOT NULL,
    customer_id character varying(30) CONSTRAINT line_items_customer_id_not_null NOT NULL,
    product_id integer CONSTRAINT line_items_product_id_not_null NOT NULL,
    quantity integer DEFAULT 1 CONSTRAINT line_items_quantity_not_null NOT NULL,
    unit_price_usd numeric(10,2),
    final_line_total_usd numeric(10,2),
    item_discount_usd numeric(10,2) DEFAULT 0,
    item_status character varying(30),
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL
);


ALTER TABLE public.staging_line_items OWNER TO chaitanya;

--
-- Name: staging_line_items_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_line_items_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_line_items_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_line_items_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_line_items_staging_row_id_seq OWNED BY public.staging_line_items.staging_row_id;


--
-- Name: staging_orders; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_orders (
    client_id character varying(20) CONSTRAINT orders_client_id_not_null NOT NULL,
    order_id character varying(50) CONSTRAINT orders_order_id_not_null NOT NULL,
    customer_id character varying(30) CONSTRAINT orders_customer_id_not_null NOT NULL,
    order_date timestamp with time zone,
    order_status character varying(30),
    order_value_usd numeric(10,2),
    discount_usd numeric(10,2) DEFAULT 0,
    coupon_code character varying(50),
    payment_method character varying(50),
    order_item_count integer,
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL
);


ALTER TABLE public.staging_orders OWNER TO chaitanya;

--
-- Name: staging_orders_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_orders_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_orders_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_orders_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_orders_staging_row_id_seq OWNED BY public.staging_orders.staging_row_id;


--
-- Name: staging_product_prices; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_product_prices (
    price_id integer CONSTRAINT product_prices_price_id_not_null NOT NULL,
    product_id integer CONSTRAINT product_prices_product_id_not_null NOT NULL,
    qty_range_label character varying(50),
    qty_min integer CONSTRAINT product_prices_qty_min_not_null NOT NULL,
    qty_max integer,
    unit_price_usd numeric(10,2) CONSTRAINT product_prices_unit_price_usd_not_null NOT NULL,
    cost_price_usd numeric(10,2),
    client_id character varying(20) CONSTRAINT product_prices_client_id_not_null NOT NULL,
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL
);


ALTER TABLE public.staging_product_prices OWNER TO chaitanya;

--
-- Name: staging_product_prices_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_product_prices_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_product_prices_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_product_prices_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_product_prices_staging_row_id_seq OWNED BY public.staging_product_prices.staging_row_id;


--
-- Name: staging_product_vendor_mapping; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_product_vendor_mapping (
    pv_id integer CONSTRAINT product_vendor_mapping_pv_id_not_null NOT NULL,
    product_id integer CONSTRAINT product_vendor_mapping_product_id_not_null NOT NULL,
    brand_id integer,
    vendor_id integer,
    client_id character varying(20) CONSTRAINT product_vendor_mapping_client_id_not_null NOT NULL,
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL
);


ALTER TABLE public.staging_product_vendor_mapping OWNER TO chaitanya;

--
-- Name: staging_product_vendor_mapping_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_product_vendor_mapping_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_product_vendor_mapping_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_product_vendor_mapping_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_product_vendor_mapping_staging_row_id_seq OWNED BY public.staging_product_vendor_mapping.staging_row_id;


--
-- Name: staging_products; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_products (
    product_id integer CONSTRAINT products_product_id_not_null NOT NULL,
    sku character varying(50) CONSTRAINT products_sku_not_null NOT NULL,
    product_name character varying(200) CONSTRAINT products_product_name_not_null NOT NULL,
    category_id integer,
    sub_category_id integer,
    sub_sub_category_id integer,
    brand_id integer,
    product_price_id integer,
    rating numeric(3,1),
    active smallint DEFAULT 1,
    not_available smallint DEFAULT 0,
    client_id character varying(20) CONSTRAINT products_client_id_not_null NOT NULL,
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL
);


ALTER TABLE public.staging_products OWNER TO chaitanya;

--
-- Name: staging_products_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_products_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_products_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_products_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_products_staging_row_id_seq OWNED BY public.staging_products.staging_row_id;


--
-- Name: staging_sub_categories; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_sub_categories (
    sub_category_id integer CONSTRAINT sub_categories_sub_category_id_not_null NOT NULL,
    sub_category_name character varying(100) CONSTRAINT sub_categories_sub_category_name_not_null NOT NULL,
    category_id integer CONSTRAINT sub_categories_category_id_not_null NOT NULL,
    client_id character varying(20) CONSTRAINT sub_categories_client_id_not_null NOT NULL,
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL
);


ALTER TABLE public.staging_sub_categories OWNER TO chaitanya;

--
-- Name: staging_sub_categories_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_sub_categories_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_sub_categories_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_sub_categories_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_sub_categories_staging_row_id_seq OWNED BY public.staging_sub_categories.staging_row_id;


--
-- Name: staging_sub_sub_categories; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_sub_sub_categories (
    sub_sub_category_id integer CONSTRAINT sub_sub_categories_sub_sub_category_id_not_null NOT NULL,
    sub_sub_category_name character varying(150) CONSTRAINT sub_sub_categories_sub_sub_category_name_not_null NOT NULL,
    sub_category_id integer CONSTRAINT sub_sub_categories_sub_category_id_not_null NOT NULL,
    category_id integer CONSTRAINT sub_sub_categories_category_id_not_null NOT NULL,
    client_id character varying(20) CONSTRAINT sub_sub_categories_client_id_not_null NOT NULL,
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL
);


ALTER TABLE public.staging_sub_sub_categories OWNER TO chaitanya;

--
-- Name: staging_sub_sub_categories_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_sub_sub_categories_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_sub_sub_categories_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_sub_sub_categories_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_sub_sub_categories_staging_row_id_seq OWNED BY public.staging_sub_sub_categories.staging_row_id;


--
-- Name: staging_support_tickets; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_support_tickets (
    client_id character varying(20) CONSTRAINT support_tickets_client_id_not_null NOT NULL,
    ticket_id character varying(30) CONSTRAINT support_tickets_ticket_id_not_null NOT NULL,
    customer_id character varying(30) CONSTRAINT support_tickets_customer_id_not_null NOT NULL,
    ticket_type character varying(100),
    priority character varying(20),
    status character varying(30),
    channel character varying(50),
    opened_date timestamp with time zone,
    resolved_date timestamp with time zone,
    resolution_time_hrs numeric(8,2),
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL
);


ALTER TABLE public.staging_support_tickets OWNER TO chaitanya;

--
-- Name: staging_support_tickets_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_support_tickets_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_support_tickets_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_support_tickets_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_support_tickets_staging_row_id_seq OWNED BY public.staging_support_tickets.staging_row_id;


--
-- Name: staging_vendors; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.staging_vendors (
    vendor_id integer CONSTRAINT vendors_vendor_id_not_null NOT NULL,
    vendor_name character varying(150) CONSTRAINT vendors_vendor_name_not_null NOT NULL,
    vendor_description text,
    vendor_contact_no character varying(30),
    vendor_address text,
    vendor_email character varying(150),
    client_id character varying(20) CONSTRAINT vendors_client_id_not_null NOT NULL,
    batch_id uuid NOT NULL,
    staging_row_id bigint NOT NULL
);


ALTER TABLE public.staging_vendors OWNER TO chaitanya;

--
-- Name: staging_vendors_staging_row_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.staging_vendors_staging_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_vendors_staging_row_id_seq OWNER TO chaitanya;

--
-- Name: staging_vendors_staging_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.staging_vendors_staging_row_id_seq OWNED BY public.staging_vendors.staging_row_id;


--
-- Name: sub_categories; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.sub_categories (
    sub_category_id integer NOT NULL,
    sub_category_name character varying(100) NOT NULL,
    category_id integer NOT NULL,
    client_id character varying(20) NOT NULL
);


ALTER TABLE public.sub_categories OWNER TO chaitanya;

--
-- Name: sub_sub_categories; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.sub_sub_categories (
    sub_sub_category_id integer NOT NULL,
    sub_sub_category_name character varying(150) NOT NULL,
    sub_category_id integer NOT NULL,
    category_id integer NOT NULL,
    client_id character varying(20) NOT NULL
);


ALTER TABLE public.sub_sub_categories OWNER TO chaitanya;

--
-- Name: upload_batches; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.upload_batches (
    batch_id uuid DEFAULT gen_random_uuid() NOT NULL,
    client_id character varying(20) NOT NULL,
    status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    committed_at timestamp with time zone,
    discarded_at timestamp with time zone,
    error_message text,
    CONSTRAINT ck_batch_status CHECK (((status)::text = ANY ((ARRAY['pending'::character varying, 'committed'::character varying, 'discarded'::character varying])::text[])))
);


ALTER TABLE public.upload_batches OWNER TO chaitanya;

--
-- Name: users; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.users (
    user_id character varying(30) NOT NULL,
    email character varying(150) NOT NULL,
    password_hash character varying(255) NOT NULL,
    name character varying(100) NOT NULL,
    role character varying(20) DEFAULT 'client_user'::character varying NOT NULL,
    client_access text[] DEFAULT '{}'::text[] NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_login timestamp with time zone,
    CONSTRAINT users_role_chk CHECK (((role)::text = ANY ((ARRAY['super_admin'::character varying, 'client_user'::character varying])::text[])))
);


ALTER TABLE public.users OWNER TO chaitanya;

--
-- Name: value_propositions; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.value_propositions (
    vp_id integer NOT NULL,
    tier_name character varying(50) NOT NULL,
    risk_level character varying(30) NOT NULL,
    action_type character varying(100),
    message_template text,
    discount_pct numeric(5,2) DEFAULT 0,
    channel character varying(50),
    priority integer DEFAULT 5
);


ALTER TABLE public.value_propositions OWNER TO chaitanya;

--
-- Name: value_propositions_vp_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.value_propositions_vp_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.value_propositions_vp_id_seq OWNER TO chaitanya;

--
-- Name: value_propositions_vp_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.value_propositions_vp_id_seq OWNED BY public.value_propositions.vp_id;


--
-- Name: value_tiers; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.value_tiers (
    tier_id character varying(10) NOT NULL,
    tier_name character varying(50) NOT NULL,
    threshold_type character varying(20),
    threshold_value numeric(10,2),
    description text,
    benefits text
);


ALTER TABLE public.value_tiers OWNER TO chaitanya;

--
-- Name: vendors; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.vendors (
    vendor_id integer NOT NULL,
    vendor_name character varying(150) NOT NULL,
    vendor_description text,
    vendor_contact_no character varying(30),
    vendor_address text,
    vendor_email character varying(150),
    client_id character varying(20) NOT NULL
);


ALTER TABLE public.vendors OWNER TO chaitanya;

--
-- Name: vw_customer_360; Type: VIEW; Schema: public; Owner: chaitanya
--

CREATE VIEW public.vw_customer_360 AS
 SELECT c.client_id,
    c.customer_id,
    c.customer_name,
    c.customer_email,
    c.customer_phone,
    c.account_created_date,
    c.registration_channel,
    c.state,
    c.city,
    c.preferred_device,
    c.email_opt_in,
    c.sms_opt_in,
    r.days_since_last_order,
    r.last_order_date,
    r.total_orders,
    r.orders_last_90d,
    r.avg_order_value_usd,
    r.total_spend_usd,
    r.ltv_usd,
    r.rfm_total_score,
    r.rfm_segment,
    r.customer_tier,
    r.return_rate_pct,
    r.account_age_days,
    cs.churn_probability,
    cs.risk_tier,
    cs.driver_1,
    cs.driver_2,
    cs.driver_3,
    cs.scored_at AS last_scored_at
   FROM ((public.customers c
     LEFT JOIN public.customer_rfm_features r ON ((((c.client_id)::text = (r.client_id)::text) AND ((c.customer_id)::text = (r.customer_id)::text))))
     LEFT JOIN LATERAL ( SELECT s.score_id,
            s.client_id,
            s.customer_id,
            s.scored_at,
            s.churn_probability,
            s.risk_tier,
            s.churn_label_simulated,
            s.driver_1,
            s.driver_2,
            s.driver_3,
            s.model_version,
            s.batch_run_id
           FROM public.churn_scores s
          WHERE (((s.client_id)::text = (c.client_id)::text) AND ((s.customer_id)::text = (c.customer_id)::text))
          ORDER BY s.scored_at DESC
         LIMIT 1) cs ON (true));


ALTER VIEW public.vw_customer_360 OWNER TO chaitanya;

--
-- Name: vw_at_risk_customers; Type: VIEW; Schema: public; Owner: chaitanya
--

CREATE VIEW public.vw_at_risk_customers AS
 SELECT client_id,
    customer_id,
    customer_name,
    customer_email,
    customer_phone,
    account_created_date,
    registration_channel,
    state,
    city,
    preferred_device,
    email_opt_in,
    sms_opt_in,
    days_since_last_order,
    last_order_date,
    total_orders,
    orders_last_90d,
    avg_order_value_usd,
    total_spend_usd,
    ltv_usd,
    rfm_total_score,
    rfm_segment,
    customer_tier,
    return_rate_pct,
    account_age_days,
    churn_probability,
    risk_tier,
    driver_1,
    driver_2,
    driver_3,
    last_scored_at
   FROM public.vw_customer_360
  WHERE ((risk_tier)::text = ANY ((ARRAY['HIGH'::character varying, 'MEDIUM'::character varying])::text[]))
  ORDER BY churn_probability DESC;


ALTER VIEW public.vw_at_risk_customers OWNER TO chaitanya;

--
-- Name: vw_customer_order_summary; Type: VIEW; Schema: public; Owner: chaitanya
--

CREATE VIEW public.vw_customer_order_summary AS
 SELECT client_id,
    customer_id,
    count(order_id) AS total_orders,
    sum(order_value_usd) AS total_spend_usd,
    avg(order_value_usd) AS avg_order_value_usd,
    min(order_date) AS first_order_date,
    max(order_date) AS last_order_date,
    EXTRACT(day FROM (now() - max(order_date))) AS days_since_last_order,
    sum(
        CASE
            WHEN (order_date >= (now() - '30 days'::interval)) THEN 1
            ELSE 0
        END) AS orders_last_30d,
    sum(
        CASE
            WHEN (order_date >= (now() - '90 days'::interval)) THEN 1
            ELSE 0
        END) AS orders_last_90d,
    sum(
        CASE
            WHEN (order_date >= (now() - '90 days'::interval)) THEN order_value_usd
            ELSE (0)::numeric
        END) AS spend_last_90d_usd,
    sum(discount_usd) AS total_discounts_usd,
    count(
        CASE
            WHEN (discount_usd > (0)::numeric) THEN 1
            ELSE NULL::integer
        END) AS orders_with_discount
   FROM public.orders o
  WHERE ((order_status)::text <> 'Cancelled'::text)
  GROUP BY client_id, customer_id;


ALTER VIEW public.vw_customer_order_summary OWNER TO chaitanya;

--
-- Name: websites; Type: TABLE; Schema: public; Owner: chaitanya
--

CREATE TABLE public.websites (
    id integer NOT NULL,
    name text NOT NULL,
    base_url text DEFAULT ''::text NOT NULL,
    search_url text DEFAULT ''::text NOT NULL,
    active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    encoding text DEFAULT 'plus'::text NOT NULL
);


ALTER TABLE public.websites OWNER TO chaitanya;

--
-- Name: TABLE websites; Type: COMMENT; Schema: public; Owner: chaitanya
--

COMMENT ON TABLE public.websites IS 'E-commerce platforms tracked by Scout Agent';


--
-- Name: websites_id_seq; Type: SEQUENCE; Schema: public; Owner: chaitanya
--

CREATE SEQUENCE public.websites_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.websites_id_seq OWNER TO chaitanya;

--
-- Name: websites_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: chaitanya
--

ALTER SEQUENCE public.websites_id_seq OWNED BY public.websites.id;


--
-- Name: audit_log id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.audit_log ALTER COLUMN id SET DEFAULT nextval('public.audit_log_id_seq'::regclass);


--
-- Name: chat_messages id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.chat_messages ALTER COLUMN id SET DEFAULT nextval('public.chat_messages_id_seq'::regclass);


--
-- Name: churn_scores score_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.churn_scores ALTER COLUMN score_id SET DEFAULT nextval('public.churn_scores_score_id_seq'::regclass);


--
-- Name: client_config config_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.client_config ALTER COLUMN config_id SET DEFAULT nextval('public.client_config_config_id_seq'::regclass);


--
-- Name: customer_price_context context_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.customer_price_context ALTER COLUMN context_id SET DEFAULT nextval('public.customer_price_context_context_id_seq'::regclass);


--
-- Name: customer_purchase_cycles cycle_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.customer_purchase_cycles ALTER COLUMN cycle_id SET DEFAULT nextval('public.customer_purchase_cycles_cycle_id_seq'::regclass);


--
-- Name: entity_listings id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.entity_listings ALTER COLUMN id SET DEFAULT nextval('public.entity_listings_id_seq'::regclass);


--
-- Name: llm_cost_log id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.llm_cost_log ALTER COLUMN id SET DEFAULT nextval('public.llm_cost_log_id_seq'::regclass);


--
-- Name: outreach_messages message_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.outreach_messages ALTER COLUMN message_id SET DEFAULT nextval('public.outreach_messages_message_id_seq'::regclass);


--
-- Name: pipeline_outputs id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.pipeline_outputs ALTER COLUMN id SET DEFAULT nextval('public.pipeline_outputs_id_seq'::regclass);


--
-- Name: price_alerts id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.price_alerts ALTER COLUMN id SET DEFAULT nextval('public.price_alerts_id_seq'::regclass);


--
-- Name: price_history id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.price_history ALTER COLUMN id SET DEFAULT nextval('public.price_history_id_seq'::regclass);


--
-- Name: pricing_recommendations recommendation_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.pricing_recommendations ALTER COLUMN recommendation_id SET DEFAULT nextval('public.pricing_recommendations_recommendation_id_seq'::regclass);


--
-- Name: product_features id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.product_features ALTER COLUMN id SET DEFAULT nextval('public.product_features_id_seq'::regclass);


--
-- Name: product_results id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.product_results ALTER COLUMN id SET DEFAULT nextval('public.product_results_id_seq'::regclass);


--
-- Name: retention_interventions intervention_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.retention_interventions ALTER COLUMN intervention_id SET DEFAULT nextval('public.retention_interventions_intervention_id_seq'::regclass);


--
-- Name: staging_brands staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_brands ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_brands_staging_row_id_seq'::regclass);


--
-- Name: staging_categories staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_categories ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_categories_staging_row_id_seq'::regclass);


--
-- Name: staging_customer_reviews staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_customer_reviews ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_customer_reviews_staging_row_id_seq'::regclass);


--
-- Name: staging_customers staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_customers ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_customers_staging_row_id_seq'::regclass);


--
-- Name: staging_line_items staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_line_items ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_line_items_staging_row_id_seq'::regclass);


--
-- Name: staging_orders staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_orders ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_orders_staging_row_id_seq'::regclass);


--
-- Name: staging_product_prices staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_product_prices ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_product_prices_staging_row_id_seq'::regclass);


--
-- Name: staging_product_vendor_mapping staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_product_vendor_mapping ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_product_vendor_mapping_staging_row_id_seq'::regclass);


--
-- Name: staging_products staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_products ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_products_staging_row_id_seq'::regclass);


--
-- Name: staging_sub_categories staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_sub_categories ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_sub_categories_staging_row_id_seq'::regclass);


--
-- Name: staging_sub_sub_categories staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_sub_sub_categories ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_sub_sub_categories_staging_row_id_seq'::regclass);


--
-- Name: staging_support_tickets staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_support_tickets ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_support_tickets_staging_row_id_seq'::regclass);


--
-- Name: staging_vendors staging_row_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_vendors ALTER COLUMN staging_row_id SET DEFAULT nextval('public.staging_vendors_staging_row_id_seq'::regclass);


--
-- Name: value_propositions vp_id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.value_propositions ALTER COLUMN vp_id SET DEFAULT nextval('public.value_propositions_vp_id_seq'::regclass);


--
-- Name: websites id; Type: DEFAULT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.websites ALTER COLUMN id SET DEFAULT nextval('public.websites_id_seq'::regclass);


--
-- Name: active_tokens active_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.active_tokens
    ADD CONSTRAINT active_tokens_pkey PRIMARY KEY (token);


--
-- Name: audit_log audit_log_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.audit_log
    ADD CONSTRAINT audit_log_pkey PRIMARY KEY (id);


--
-- Name: brands brands_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.brands
    ADD CONSTRAINT brands_pkey PRIMARY KEY (client_id, brand_id);


--
-- Name: business_segments business_segments_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.business_segments
    ADD CONSTRAINT business_segments_pkey PRIMARY KEY (segment_id);


--
-- Name: categories categories_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.categories
    ADD CONSTRAINT categories_pkey PRIMARY KEY (client_id, category_id);


--
-- Name: chat_messages chat_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.chat_messages
    ADD CONSTRAINT chat_messages_pkey PRIMARY KEY (id);


--
-- Name: churn_scores churn_scores_client_customer_unique; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.churn_scores
    ADD CONSTRAINT churn_scores_client_customer_unique UNIQUE (client_id, customer_id);


--
-- Name: churn_scores churn_scores_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.churn_scores
    ADD CONSTRAINT churn_scores_pkey PRIMARY KEY (score_id);


--
-- Name: client_config client_config_client_id_key; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.client_config
    ADD CONSTRAINT client_config_client_id_key UNIQUE (client_id);


--
-- Name: client_config client_config_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.client_config
    ADD CONSTRAINT client_config_pkey PRIMARY KEY (config_id);


--
-- Name: customer_price_context customer_price_context_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.customer_price_context
    ADD CONSTRAINT customer_price_context_pkey PRIMARY KEY (context_id);


--
-- Name: customer_purchase_cycles customer_purchase_cycles_client_id_customer_id_product_id_key; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.customer_purchase_cycles
    ADD CONSTRAINT customer_purchase_cycles_client_id_customer_id_product_id_key UNIQUE (client_id, customer_id, product_id);


--
-- Name: customer_purchase_cycles customer_purchase_cycles_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.customer_purchase_cycles
    ADD CONSTRAINT customer_purchase_cycles_pkey PRIMARY KEY (cycle_id);


--
-- Name: customer_reviews customer_reviews_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.customer_reviews
    ADD CONSTRAINT customer_reviews_pkey PRIMARY KEY (client_id, review_id);


--
-- Name: customer_rfm_features customer_rfm_features_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.customer_rfm_features
    ADD CONSTRAINT customer_rfm_features_pkey PRIMARY KEY (client_id, customer_id);


--
-- Name: customers customers_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.customers
    ADD CONSTRAINT customers_pkey PRIMARY KEY (client_id, customer_id);


--
-- Name: entities entities_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_pkey PRIMARY KEY (id);


--
-- Name: entity_listings entity_listings_entity_id_platform_key; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.entity_listings
    ADD CONSTRAINT entity_listings_entity_id_platform_key UNIQUE (entity_id, platform);


--
-- Name: entity_listings entity_listings_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.entity_listings
    ADD CONSTRAINT entity_listings_pkey PRIMARY KEY (id);


--
-- Name: line_items line_items_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.line_items
    ADD CONSTRAINT line_items_pkey PRIMARY KEY (client_id, line_item_id);


--
-- Name: llm_cost_log llm_cost_log_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.llm_cost_log
    ADD CONSTRAINT llm_cost_log_pkey PRIMARY KEY (id);


--
-- Name: message_templates message_templates_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.message_templates
    ADD CONSTRAINT message_templates_pkey PRIMARY KEY (client_id, id);


--
-- Name: orders orders_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_pkey PRIMARY KEY (client_id, order_id);


--
-- Name: outreach_messages outreach_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.outreach_messages
    ADD CONSTRAINT outreach_messages_pkey PRIMARY KEY (message_id);


--
-- Name: pipeline_outputs pipeline_outputs_client_id_filename_key; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.pipeline_outputs
    ADD CONSTRAINT pipeline_outputs_client_id_filename_key UNIQUE (client_id, filename);


--
-- Name: pipeline_outputs pipeline_outputs_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.pipeline_outputs
    ADD CONSTRAINT pipeline_outputs_pkey PRIMARY KEY (id);


--
-- Name: price_alerts price_alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.price_alerts
    ADD CONSTRAINT price_alerts_pkey PRIMARY KEY (id);


--
-- Name: price_history price_history_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.price_history
    ADD CONSTRAINT price_history_pkey PRIMARY KEY (id);


--
-- Name: pricing_recommendations pricing_recommendations_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.pricing_recommendations
    ADD CONSTRAINT pricing_recommendations_pkey PRIMARY KEY (recommendation_id);


--
-- Name: product_features product_features_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.product_features
    ADD CONSTRAINT product_features_pkey PRIMARY KEY (id);


--
-- Name: product_features product_features_product_name_platform_key; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.product_features
    ADD CONSTRAINT product_features_product_name_platform_key UNIQUE (product_name, platform);


--
-- Name: product_prices product_prices_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.product_prices
    ADD CONSTRAINT product_prices_pkey PRIMARY KEY (client_id, price_id);


--
-- Name: product_results product_results_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.product_results
    ADD CONSTRAINT product_results_pkey PRIMARY KEY (id);


--
-- Name: product_results product_results_product_name_platform_key; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.product_results
    ADD CONSTRAINT product_results_product_name_platform_key UNIQUE (product_name, platform);


--
-- Name: product_vendor_mapping product_vendor_mapping_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.product_vendor_mapping
    ADD CONSTRAINT product_vendor_mapping_pkey PRIMARY KEY (client_id, pv_id);


--
-- Name: products products_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_pkey PRIMARY KEY (client_id, product_id);


--
-- Name: retention_interventions retention_interventions_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.retention_interventions
    ADD CONSTRAINT retention_interventions_pkey PRIMARY KEY (intervention_id);


--
-- Name: staging_brands staging_brands_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_brands
    ADD CONSTRAINT staging_brands_pkey PRIMARY KEY (staging_row_id);


--
-- Name: staging_categories staging_categories_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_categories
    ADD CONSTRAINT staging_categories_pkey PRIMARY KEY (staging_row_id);


--
-- Name: staging_customer_reviews staging_customer_reviews_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_customer_reviews
    ADD CONSTRAINT staging_customer_reviews_pkey PRIMARY KEY (staging_row_id);


--
-- Name: staging_customers staging_customers_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_customers
    ADD CONSTRAINT staging_customers_pkey PRIMARY KEY (staging_row_id);


--
-- Name: staging_line_items staging_line_items_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_line_items
    ADD CONSTRAINT staging_line_items_pkey PRIMARY KEY (staging_row_id);


--
-- Name: staging_orders staging_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_orders
    ADD CONSTRAINT staging_orders_pkey PRIMARY KEY (staging_row_id);


--
-- Name: staging_product_prices staging_product_prices_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_product_prices
    ADD CONSTRAINT staging_product_prices_pkey PRIMARY KEY (staging_row_id);


--
-- Name: staging_product_vendor_mapping staging_product_vendor_mapping_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_product_vendor_mapping
    ADD CONSTRAINT staging_product_vendor_mapping_pkey PRIMARY KEY (staging_row_id);


--
-- Name: staging_products staging_products_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_products
    ADD CONSTRAINT staging_products_pkey PRIMARY KEY (staging_row_id);


--
-- Name: staging_sub_categories staging_sub_categories_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_sub_categories
    ADD CONSTRAINT staging_sub_categories_pkey PRIMARY KEY (staging_row_id);


--
-- Name: staging_sub_sub_categories staging_sub_sub_categories_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_sub_sub_categories
    ADD CONSTRAINT staging_sub_sub_categories_pkey PRIMARY KEY (staging_row_id);


--
-- Name: staging_support_tickets staging_support_tickets_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_support_tickets
    ADD CONSTRAINT staging_support_tickets_pkey PRIMARY KEY (staging_row_id);


--
-- Name: staging_vendors staging_vendors_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.staging_vendors
    ADD CONSTRAINT staging_vendors_pkey PRIMARY KEY (staging_row_id);


--
-- Name: sub_categories sub_categories_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.sub_categories
    ADD CONSTRAINT sub_categories_pkey PRIMARY KEY (client_id, sub_category_id);


--
-- Name: sub_sub_categories sub_sub_categories_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.sub_sub_categories
    ADD CONSTRAINT sub_sub_categories_pkey PRIMARY KEY (client_id, sub_sub_category_id);


--
-- Name: support_tickets support_tickets_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.support_tickets
    ADD CONSTRAINT support_tickets_pkey PRIMARY KEY (client_id, ticket_id);


--
-- Name: upload_batches upload_batches_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.upload_batches
    ADD CONSTRAINT upload_batches_pkey PRIMARY KEY (batch_id);


--
-- Name: customer_price_context uq_customer_price_context; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.customer_price_context
    ADD CONSTRAINT uq_customer_price_context UNIQUE (customer_id, product_name);


--
-- Name: users users_email_key; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_email_key UNIQUE (email);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (user_id);


--
-- Name: value_propositions value_propositions_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.value_propositions
    ADD CONSTRAINT value_propositions_pkey PRIMARY KEY (vp_id);


--
-- Name: value_tiers value_tiers_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.value_tiers
    ADD CONSTRAINT value_tiers_pkey PRIMARY KEY (tier_id);


--
-- Name: vendors vendors_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.vendors
    ADD CONSTRAINT vendors_pkey PRIMARY KEY (client_id, vendor_id);


--
-- Name: websites websites_name_key; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.websites
    ADD CONSTRAINT websites_name_key UNIQUE (name);


--
-- Name: websites websites_pkey; Type: CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.websites
    ADD CONSTRAINT websites_pkey PRIMARY KEY (id);


--
-- Name: idx_audit_action; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_audit_action ON public.audit_log USING btree (action_type);


--
-- Name: idx_audit_client; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_audit_client ON public.audit_log USING btree (client_id);


--
-- Name: idx_audit_outcome; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_audit_outcome ON public.audit_log USING btree (outcome);


--
-- Name: idx_audit_ts; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_audit_ts ON public.audit_log USING btree (ts DESC);


--
-- Name: idx_audit_user; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_audit_user ON public.audit_log USING btree (user_id);


--
-- Name: idx_brands_client; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_brands_client ON public.brands USING btree (client_id);


--
-- Name: idx_categories_client; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_categories_client ON public.categories USING btree (client_id);


--
-- Name: idx_chat_client; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_chat_client ON public.chat_messages USING btree (client_id, created_at DESC);


--
-- Name: idx_chat_conv; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_chat_conv ON public.chat_messages USING btree (conversation_id, created_at);


--
-- Name: idx_churn_scores_customer; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_churn_scores_customer ON public.churn_scores USING btree (client_id, customer_id);


--
-- Name: idx_churn_scores_scored; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_churn_scores_scored ON public.churn_scores USING btree (scored_at DESC);


--
-- Name: idx_churn_scores_tier; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_churn_scores_tier ON public.churn_scores USING btree (risk_tier);


--
-- Name: idx_client_config_is_active; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_client_config_is_active ON public.client_config USING btree (is_active);


--
-- Name: idx_cpc_client_customers; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_cpc_client_customers ON public.customer_price_context USING btree (client_id, customer_id);


--
-- Name: idx_customers_client; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_customers_client ON public.customers USING btree (client_id);


--
-- Name: idx_customers_email; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_customers_email ON public.customers USING btree (customer_email);


--
-- Name: idx_cycles_customer; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_cycles_customer ON public.customer_purchase_cycles USING btree (client_id, customer_id);


--
-- Name: idx_cycles_expected; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_cycles_expected ON public.customer_purchase_cycles USING btree (expected_next_date);


--
-- Name: idx_cycles_overdue; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_cycles_overdue ON public.customer_purchase_cycles USING btree (days_overdue DESC);


--
-- Name: idx_entities_query; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_entities_query ON public.entities USING btree (query);


--
-- Name: idx_entity_listings_entity; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_entity_listings_entity ON public.entity_listings USING btree (entity_id);


--
-- Name: idx_entity_listings_entity_id; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_entity_listings_entity_id ON public.entity_listings USING btree (entity_id);


--
-- Name: idx_interventions_customer; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_interventions_customer ON public.retention_interventions USING btree (client_id, customer_id);


--
-- Name: idx_interventions_status; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_interventions_status ON public.retention_interventions USING btree (offer_status);


--
-- Name: idx_line_items_customer; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_line_items_customer ON public.line_items USING btree (client_id, customer_id);


--
-- Name: idx_line_items_order; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_line_items_order ON public.line_items USING btree (client_id, order_id);


--
-- Name: idx_line_items_product; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_line_items_product ON public.line_items USING btree (product_id);


--
-- Name: idx_llm_cost_log_client_created; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_llm_cost_log_client_created ON public.llm_cost_log USING btree (client_id, created_at DESC);


--
-- Name: idx_mv_cf_churn; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_mv_cf_churn ON public.mv_customer_features USING btree (churn_label, rfm_total_score DESC);


--
-- Name: idx_mv_cf_overdue; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_mv_cf_overdue ON public.mv_customer_features USING btree (days_overdue_for_refill DESC);


--
-- Name: idx_mv_cf_pk; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE UNIQUE INDEX idx_mv_cf_pk ON public.mv_customer_features USING btree (client_id, customer_id);


--
-- Name: idx_mv_cf_recency; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_mv_cf_recency ON public.mv_customer_features USING btree (days_since_last_order DESC);


--
-- Name: idx_mv_cf_tier; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_mv_cf_tier ON public.mv_customer_features USING btree (customer_tier, is_high_value);


--
-- Name: idx_orders_customer; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_orders_customer ON public.orders USING btree (client_id, customer_id);


--
-- Name: idx_orders_date; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_orders_date ON public.orders USING btree (order_date);


--
-- Name: idx_orders_status; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_orders_status ON public.orders USING btree (order_status);


--
-- Name: idx_outreach_customer; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_outreach_customer ON public.outreach_messages USING btree (client_id, customer_id);


--
-- Name: idx_outreach_outcome; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_outreach_outcome ON public.outreach_messages USING btree (outcome);


--
-- Name: idx_outreach_type; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_outreach_type ON public.outreach_messages USING btree (message_type);


--
-- Name: idx_price_alerts_detected; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_price_alerts_detected ON public.price_alerts USING btree (detected_at DESC);


--
-- Name: idx_price_alerts_product; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_price_alerts_product ON public.price_alerts USING btree (product_name, platform);


--
-- Name: idx_price_alerts_product_platform; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_price_alerts_product_platform ON public.price_alerts USING btree (product_name, platform, detected_at DESC);


--
-- Name: idx_price_history_product; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_price_history_product ON public.price_history USING btree (product_name, platform);


--
-- Name: idx_price_history_product_platform; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_price_history_product_platform ON public.price_history USING btree (product_name, platform, scraped_at DESC);


--
-- Name: idx_price_history_scraped; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_price_history_scraped ON public.price_history USING btree (scraped_at DESC);


--
-- Name: idx_pricing_rec_client_run; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_pricing_rec_client_run ON public.pricing_recommendations USING btree (client_id, run_id);


--
-- Name: idx_pricing_rec_product; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_pricing_rec_product ON public.pricing_recommendations USING btree (client_id, product_name);


--
-- Name: idx_product_features_product_name; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_product_features_product_name ON public.product_features USING btree (product_name);


--
-- Name: idx_product_prices_client; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_product_prices_client ON public.product_prices USING btree (client_id);


--
-- Name: idx_product_results_product; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_product_results_product ON public.product_results USING btree (product_name);


--
-- Name: idx_products_client; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_products_client ON public.products USING btree (client_id);


--
-- Name: idx_pvm_client; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_pvm_client ON public.product_vendor_mapping USING btree (client_id);


--
-- Name: idx_reviews_customer; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_reviews_customer ON public.customer_reviews USING btree (client_id, customer_id);


--
-- Name: idx_reviews_product; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_reviews_product ON public.customer_reviews USING btree (product_id);


--
-- Name: idx_reviews_rating; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_reviews_rating ON public.customer_reviews USING btree (rating);


--
-- Name: idx_reviews_sentiment; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_reviews_sentiment ON public.customer_reviews USING btree (sentiment);


--
-- Name: idx_sub_categories_client; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_sub_categories_client ON public.sub_categories USING btree (client_id);


--
-- Name: idx_sub_sub_categories_client; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_sub_sub_categories_client ON public.sub_sub_categories USING btree (client_id);


--
-- Name: idx_tickets_customer; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_tickets_customer ON public.support_tickets USING btree (client_id, customer_id);


--
-- Name: idx_tickets_priority; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_tickets_priority ON public.support_tickets USING btree (priority);


--
-- Name: idx_tickets_status; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_tickets_status ON public.support_tickets USING btree (status);


--
-- Name: idx_tickets_type; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_tickets_type ON public.support_tickets USING btree (ticket_type);


--
-- Name: idx_users_email; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_users_email ON public.users USING btree (email);


--
-- Name: idx_vendors_client; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_vendors_client ON public.vendors USING btree (client_id);


--
-- Name: idx_vp_tier_risk; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX idx_vp_tier_risk ON public.value_propositions USING btree (tier_name, risk_level);


--
-- Name: ix_batch_client_status; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_batch_client_status ON public.upload_batches USING btree (client_id, status);


--
-- Name: ix_stg_brands_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_brands_batch ON public.staging_brands USING btree (batch_id);


--
-- Name: ix_stg_categories_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_categories_batch ON public.staging_categories USING btree (batch_id);


--
-- Name: ix_stg_customer_reviews_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_customer_reviews_batch ON public.staging_customer_reviews USING btree (batch_id);


--
-- Name: ix_stg_customers_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_customers_batch ON public.staging_customers USING btree (batch_id);


--
-- Name: ix_stg_line_items_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_line_items_batch ON public.staging_line_items USING btree (batch_id);


--
-- Name: ix_stg_orders_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_orders_batch ON public.staging_orders USING btree (batch_id);


--
-- Name: ix_stg_product_prices_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_product_prices_batch ON public.staging_product_prices USING btree (batch_id);


--
-- Name: ix_stg_products_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_products_batch ON public.staging_products USING btree (batch_id);


--
-- Name: ix_stg_pvm_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_pvm_batch ON public.staging_product_vendor_mapping USING btree (batch_id);


--
-- Name: ix_stg_sub_categories_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_sub_categories_batch ON public.staging_sub_categories USING btree (batch_id);


--
-- Name: ix_stg_sub_sub_categories_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_sub_sub_categories_batch ON public.staging_sub_sub_categories USING btree (batch_id);


--
-- Name: ix_stg_support_tickets_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_support_tickets_batch ON public.staging_support_tickets USING btree (batch_id);


--
-- Name: ix_stg_vendors_batch; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE INDEX ix_stg_vendors_batch ON public.staging_vendors USING btree (batch_id);


--
-- Name: uq_one_pending_batch_per_client; Type: INDEX; Schema: public; Owner: chaitanya
--

CREATE UNIQUE INDEX uq_one_pending_batch_per_client ON public.upload_batches USING btree (client_id) WHERE ((status)::text = 'pending'::text);


--
-- Name: brands brands_vendor_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.brands
    ADD CONSTRAINT brands_vendor_fk FOREIGN KEY (client_id, vendor_id) REFERENCES public.vendors(client_id, vendor_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: churn_scores churn_scores_client_id_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.churn_scores
    ADD CONSTRAINT churn_scores_client_id_customer_id_fkey FOREIGN KEY (client_id, customer_id) REFERENCES public.customers(client_id, customer_id);


--
-- Name: customer_purchase_cycles customer_purchase_cycles_client_id_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.customer_purchase_cycles
    ADD CONSTRAINT customer_purchase_cycles_client_id_customer_id_fkey FOREIGN KEY (client_id, customer_id) REFERENCES public.customers(client_id, customer_id);


--
-- Name: customer_reviews customer_reviews_client_id_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.customer_reviews
    ADD CONSTRAINT customer_reviews_client_id_customer_id_fkey FOREIGN KEY (client_id, customer_id) REFERENCES public.customers(client_id, customer_id);


--
-- Name: customer_reviews customer_reviews_product_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.customer_reviews
    ADD CONSTRAINT customer_reviews_product_fk FOREIGN KEY (client_id, product_id) REFERENCES public.products(client_id, product_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: entity_listings entity_listings_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.entity_listings
    ADD CONSTRAINT entity_listings_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id) ON DELETE CASCADE;


--
-- Name: products fk_products_price; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT fk_products_price FOREIGN KEY (client_id, product_price_id) REFERENCES public.product_prices(client_id, price_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: line_items line_items_client_id_order_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.line_items
    ADD CONSTRAINT line_items_client_id_order_id_fkey FOREIGN KEY (client_id, order_id) REFERENCES public.orders(client_id, order_id);


--
-- Name: line_items line_items_product_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.line_items
    ADD CONSTRAINT line_items_product_fk FOREIGN KEY (client_id, product_id) REFERENCES public.products(client_id, product_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: orders orders_client_id_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_client_id_customer_id_fkey FOREIGN KEY (client_id, customer_id) REFERENCES public.customers(client_id, customer_id);


--
-- Name: outreach_messages outreach_messages_client_id_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.outreach_messages
    ADD CONSTRAINT outreach_messages_client_id_customer_id_fkey FOREIGN KEY (client_id, customer_id) REFERENCES public.customers(client_id, customer_id);


--
-- Name: product_prices product_prices_product_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.product_prices
    ADD CONSTRAINT product_prices_product_fk FOREIGN KEY (client_id, product_id) REFERENCES public.products(client_id, product_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: products products_brand_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_brand_fk FOREIGN KEY (client_id, brand_id) REFERENCES public.brands(client_id, brand_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: products products_category_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_category_fk FOREIGN KEY (client_id, category_id) REFERENCES public.categories(client_id, category_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: products products_sub_category_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_sub_category_fk FOREIGN KEY (client_id, sub_category_id) REFERENCES public.sub_categories(client_id, sub_category_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: products products_sub_sub_category_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.products
    ADD CONSTRAINT products_sub_sub_category_fk FOREIGN KEY (client_id, sub_sub_category_id) REFERENCES public.sub_sub_categories(client_id, sub_sub_category_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: product_vendor_mapping pvm_brand_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.product_vendor_mapping
    ADD CONSTRAINT pvm_brand_fk FOREIGN KEY (client_id, brand_id) REFERENCES public.brands(client_id, brand_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: product_vendor_mapping pvm_product_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.product_vendor_mapping
    ADD CONSTRAINT pvm_product_fk FOREIGN KEY (client_id, product_id) REFERENCES public.products(client_id, product_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: product_vendor_mapping pvm_vendor_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.product_vendor_mapping
    ADD CONSTRAINT pvm_vendor_fk FOREIGN KEY (client_id, vendor_id) REFERENCES public.vendors(client_id, vendor_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: retention_interventions retention_interventions_churn_score_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.retention_interventions
    ADD CONSTRAINT retention_interventions_churn_score_id_fkey FOREIGN KEY (churn_score_id) REFERENCES public.churn_scores(score_id);


--
-- Name: sub_categories sub_categories_category_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.sub_categories
    ADD CONSTRAINT sub_categories_category_fk FOREIGN KEY (client_id, category_id) REFERENCES public.categories(client_id, category_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: sub_sub_categories sub_sub_categories_category_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.sub_sub_categories
    ADD CONSTRAINT sub_sub_categories_category_fk FOREIGN KEY (client_id, category_id) REFERENCES public.categories(client_id, category_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: sub_sub_categories sub_sub_categories_sub_category_fk; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.sub_sub_categories
    ADD CONSTRAINT sub_sub_categories_sub_category_fk FOREIGN KEY (client_id, sub_category_id) REFERENCES public.sub_categories(client_id, sub_category_id) DEFERRABLE INITIALLY DEFERRED;


--
-- Name: support_tickets support_tickets_client_id_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: chaitanya
--

ALTER TABLE ONLY public.support_tickets
    ADD CONSTRAINT support_tickets_client_id_customer_id_fkey FOREIGN KEY (client_id, customer_id) REFERENCES public.customers(client_id, customer_id);


--
-- Name: mv_customer_features; Type: MATERIALIZED VIEW DATA; Schema: public; Owner: chaitanya
--

REFRESH MATERIALIZED VIEW public.mv_customer_features;


--
-- PostgreSQL database dump complete
--

\unrestrict V4swM1x6XKacKsRGCqvd9xiqUMOKsBWzSflGTIojMxbceZtqhkT8gAskVc8CILv

