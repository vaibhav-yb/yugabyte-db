--
-- YSQL database dump
--

-- Dumped from database version 11.2-YB-2.15.1.0-b0
-- Dumped by ysql_dump version 11.2-YB-2.15.1.0-b0

SET yb_binary_restore = true;
SET yb_non_ddl_txn_for_sys_tables_allowed = true;
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: hint_plan; Type: SCHEMA; Schema: -; Owner: yugabyte_test
--

CREATE SCHEMA hint_plan;


ALTER SCHEMA hint_plan OWNER TO yugabyte_test;

--
-- Name: pg_hint_plan; Type: EXTENSION; Schema: -; Owner:
--

-- For binary upgrade, create an empty extension and insert objects into it
DROP EXTENSION IF EXISTS pg_hint_plan;
SELECT pg_catalog.binary_upgrade_create_empty_extension('pg_hint_plan', 'hint_plan', false, '1.3.7', '{16549,16547}', '{"",""}', ARRAY[]::pg_catalog.text[]);


SET default_tablespace = '';

--
-- Name: grp1; Type: TABLEGROUP; Schema: -; Owner: tablegroup_test_user
--

CREATE TABLEGROUP grp1;


ALTER TABLEGROUP grp1 OWNER TO tablegroup_test_user;

--
-- Name: grp2; Type: TABLEGROUP; Schema: -; Owner: tablegroup_test_user
--

CREATE TABLEGROUP grp2;


ALTER TABLEGROUP grp2 OWNER TO tablegroup_test_user;

SET default_tablespace = tsp1;

--
-- Name: grp_with_spc; Type: TABLEGROUP; Schema: -; Owner: tablegroup_test_user; Tablespace: tsp1
--

CREATE TABLEGROUP grp_with_spc;


ALTER TABLEGROUP grp_with_spc OWNER TO tablegroup_test_user;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: hints; Type: TABLE; Schema: hint_plan; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16551'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16550'::pg_catalog.oid);

CREATE TABLE hint_plan.hints (
    id integer NOT NULL,
    norm_query_string text NOT NULL,
    application_name text NOT NULL,
    hints text NOT NULL,
    CONSTRAINT hints_pkey PRIMARY KEY((id) HASH)
)
SPLIT INTO 3 TABLETS;

-- For binary upgrade, handle extension membership the hard way
ALTER EXTENSION pg_hint_plan ADD TABLE hint_plan.hints;


ALTER TABLE hint_plan.hints OWNER TO yugabyte_test;

--
-- Name: hints_id_seq; Type: SEQUENCE; Schema: hint_plan; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16548'::pg_catalog.oid);

CREATE SEQUENCE hint_plan.hints_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- For binary upgrade, handle extension membership the hard way
ALTER EXTENSION pg_hint_plan ADD SEQUENCE hint_plan.hints_id_seq;


ALTER TABLE hint_plan.hints_id_seq OWNER TO yugabyte_test;

--
-- Name: hints_id_seq; Type: SEQUENCE OWNED BY; Schema: hint_plan; Owner: yugabyte_test
--

ALTER SEQUENCE hint_plan.hints_id_seq OWNED BY hint_plan.hints.id;


--
-- Name: chat_user; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16475'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16474'::pg_catalog.oid);

CREATE TABLE public.chat_user (
    "chatID" text NOT NULL,
    CONSTRAINT chat_user_pkey PRIMARY KEY(("chatID") HASH)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.chat_user OWNER TO yugabyte_test;

--
-- Name: p1; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16558'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16557'::pg_catalog.oid);

CREATE TABLE public.p1 (
    k integer NOT NULL,
    v text,
    CONSTRAINT p1_pkey PRIMARY KEY((k) HASH)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.p1 OWNER TO yugabyte_test;

--
-- Name: p2; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16565'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16564'::pg_catalog.oid);

CREATE TABLE public.p2 (
    k integer NOT NULL,
    v text,
    CONSTRAINT p2_pkey PRIMARY KEY((k) HASH)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.p2 OWNER TO yugabyte_test;

--
-- Name: rls_private; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16467'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16466'::pg_catalog.oid);

CREATE TABLE public.rls_private (
    k integer NOT NULL,
    v text,
    CONSTRAINT rls_private_pkey PRIMARY KEY((k) HASH)
)
SPLIT INTO 3 TABLETS;

ALTER TABLE ONLY public.rls_private FORCE ROW LEVEL SECURITY;


ALTER TABLE public.rls_private OWNER TO yugabyte_test;

--
-- Name: rls_public; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16462'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16461'::pg_catalog.oid);

CREATE TABLE public.rls_public (
    k integer NOT NULL,
    v text,
    CONSTRAINT rls_public_pkey PRIMARY KEY((k) HASH)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.rls_public OWNER TO yugabyte_test;

--
-- Name: tbl1; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16390'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16389'::pg_catalog.oid);

CREATE TABLE public.tbl1 (
    a integer NOT NULL,
    b integer
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tbl1 OWNER TO yugabyte_test;

--
-- Name: tbl10; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16440'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16439'::pg_catalog.oid);

CREATE TABLE public.tbl10 (
    a integer NOT NULL,
    b integer NOT NULL,
    c integer NOT NULL,
    d integer,
    CONSTRAINT tbl10_pkey PRIMARY KEY((a, c) HASH, b ASC)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tbl10 OWNER TO yugabyte_test;

--
-- Name: tbl11; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16445'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16444'::pg_catalog.oid);

CREATE TABLE public.tbl11 (
    a integer NOT NULL,
    b integer NOT NULL,
    c integer,
    CONSTRAINT tbl11_pkey PRIMARY KEY(a DESC, b ASC)
);


ALTER TABLE public.tbl11 OWNER TO yugabyte_test;

--
-- Name: tbl12; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16450'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16449'::pg_catalog.oid);

CREATE TABLE public.tbl12 (
    a integer NOT NULL,
    b integer,
    c integer NOT NULL,
    d integer NOT NULL,
    CONSTRAINT tbl12_pkey PRIMARY KEY(a ASC, d DESC, c DESC)
);


ALTER TABLE public.tbl12 OWNER TO yugabyte_test;

--
-- Name: tbl13; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16455'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16454'::pg_catalog.oid);

CREATE TABLE public.tbl13 (
    a integer,
    b integer NOT NULL,
    c integer NOT NULL,
    d integer,
    CONSTRAINT tbl13_pkey PRIMARY KEY((b, c) HASH)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tbl13 OWNER TO yugabyte_test;

--
-- Name: tbl1_a_seq; Type: SEQUENCE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16387'::pg_catalog.oid);

CREATE SEQUENCE public.tbl1_a_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tbl1_a_seq OWNER TO yugabyte_test;

--
-- Name: tbl1_a_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: yugabyte_test
--

ALTER SEQUENCE public.tbl1_a_seq OWNED BY public.tbl1.a;


--
-- Name: tbl2; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16396'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16395'::pg_catalog.oid);

CREATE TABLE public.tbl2 (
    a integer NOT NULL
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tbl2 OWNER TO yugabyte_test;

--
-- Name: tbl2_a_seq; Type: SEQUENCE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16393'::pg_catalog.oid);

CREATE SEQUENCE public.tbl2_a_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tbl2_a_seq OWNER TO yugabyte_test;

--
-- Name: tbl2_a_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: yugabyte_test
--

ALTER SEQUENCE public.tbl2_a_seq OWNED BY public.tbl2.a;


--
-- Name: tbl3; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16400'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16399'::pg_catalog.oid);

CREATE TABLE public.tbl3 (
    a integer NOT NULL,
    b integer,
    CONSTRAINT tbl3_pkey PRIMARY KEY(a ASC)
);


ALTER TABLE public.tbl3 OWNER TO yugabyte_test;

--
-- Name: tbl4; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16405'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16404'::pg_catalog.oid);

CREATE TABLE public.tbl4 (
    a integer NOT NULL,
    b integer NOT NULL,
    CONSTRAINT tbl4_pkey PRIMARY KEY((a) HASH, b ASC)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tbl4 OWNER TO yugabyte_test;

--
-- Name: tbl5; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16410'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16409'::pg_catalog.oid);

CREATE TABLE public.tbl5 (
    a integer NOT NULL,
    b integer,
    c integer,
    CONSTRAINT tbl5_pkey PRIMARY KEY((a) HASH)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tbl5 OWNER TO yugabyte_test;

--
-- Name: tbl6; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16415'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16414'::pg_catalog.oid);

CREATE TABLE public.tbl6 (
    a integer NOT NULL,
    CONSTRAINT tbl6_pkey PRIMARY KEY((a) HASH)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tbl6 OWNER TO yugabyte_test;

--
-- Name: tbl7; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16420'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16419'::pg_catalog.oid);

CREATE TABLE public.tbl7 (
    a integer,
    b integer NOT NULL,
    c integer NOT NULL,
    d integer,
    CONSTRAINT tbl7_pkey PRIMARY KEY((b) HASH, c ASC)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tbl7 OWNER TO yugabyte_test;

--
-- Name: tbl8; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16425'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16424'::pg_catalog.oid);

CREATE TABLE public.tbl8 (
    a integer NOT NULL,
    b integer,
    c integer,
    d integer NOT NULL,
    CONSTRAINT tbl8_pkey PRIMARY KEY((a) HASH, d ASC)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tbl8 OWNER TO yugabyte_test;

--
-- Name: tbl9; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16435'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16434'::pg_catalog.oid);

CREATE TABLE public.tbl9 (
    a integer NOT NULL,
    b integer NOT NULL,
    c integer,
    CONSTRAINT tbl9_pkey PRIMARY KEY((a, b) HASH)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tbl9 OWNER TO yugabyte_test;

--
-- Name: tgroup_after_options; Type: TABLE; Schema: public; Owner: tablegroup_test_user
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16508'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16507'::pg_catalog.oid);

CREATE TABLE public.tgroup_after_options (
    a integer
)
WITH (parallel_workers='2', colocation_id='20002')
TABLEGROUP grp1;


ALTER TABLE public.tgroup_after_options OWNER TO tablegroup_test_user;

--
-- Name: tgroup_empty_options; Type: TABLE; Schema: public; Owner: tablegroup_test_user
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16514'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16513'::pg_catalog.oid);

CREATE TABLE public.tgroup_empty_options (
    a integer
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tgroup_empty_options OWNER TO tablegroup_test_user;

--
-- Name: tgroup_in_between_options; Type: TABLE; Schema: public; Owner: tablegroup_test_user
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16511'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16510'::pg_catalog.oid);

CREATE TABLE public.tgroup_in_between_options (
    a integer
)
WITH (parallel_workers='2', autovacuum_enabled='true', colocation_id='20003')
TABLEGROUP grp1;


ALTER TABLE public.tgroup_in_between_options OWNER TO tablegroup_test_user;

--
-- Name: tgroup_no_options_and_tgroup; Type: TABLE; Schema: public; Owner: tablegroup_test_user
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16490'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16489'::pg_catalog.oid);

CREATE TABLE public.tgroup_no_options_and_tgroup (
    a integer
)
WITH (colocation_id='20001')
TABLEGROUP grp1;


ALTER TABLE public.tgroup_no_options_and_tgroup OWNER TO tablegroup_test_user;

--
-- Name: tgroup_one_option; Type: TABLE; Schema: public; Owner: tablegroup_test_user
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16493'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16492'::pg_catalog.oid);

CREATE TABLE public.tgroup_one_option (
    a integer
)
WITH (autovacuum_enabled='true')
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tgroup_one_option OWNER TO tablegroup_test_user;

--
-- Name: tgroup_one_option_and_tgroup; Type: TABLE; Schema: public; Owner: tablegroup_test_user
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16496'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16495'::pg_catalog.oid);

CREATE TABLE public.tgroup_one_option_and_tgroup (
    a integer
)
WITH (autovacuum_enabled='true', colocation_id='20001')
TABLEGROUP grp2;


ALTER TABLE public.tgroup_one_option_and_tgroup OWNER TO tablegroup_test_user;

--
-- Name: tgroup_options; Type: TABLE; Schema: public; Owner: tablegroup_test_user
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16499'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16498'::pg_catalog.oid);

CREATE TABLE public.tgroup_options (
    a integer
)
WITH (autovacuum_enabled='true', parallel_workers='2')
SPLIT INTO 3 TABLETS;


ALTER TABLE public.tgroup_options OWNER TO tablegroup_test_user;

--
-- Name: tgroup_options_and_tgroup; Type: TABLE; Schema: public; Owner: tablegroup_test_user
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16502'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16501'::pg_catalog.oid);

CREATE TABLE public.tgroup_options_and_tgroup (
    a integer
)
WITH (autovacuum_enabled='true', parallel_workers='2', colocation_id='20002')
TABLEGROUP grp2;


ALTER TABLE public.tgroup_options_and_tgroup OWNER TO tablegroup_test_user;

--
-- Name: tgroup_options_tgroup_and_custom_colocation_id; Type: TABLE; Schema: public; Owner: tablegroup_test_user
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16505'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16504'::pg_catalog.oid);

CREATE TABLE public.tgroup_options_tgroup_and_custom_colocation_id (
    a integer
)
WITH (autovacuum_enabled='true', parallel_workers='2', colocation_id='100500')
TABLEGROUP grp2;


ALTER TABLE public.tgroup_options_tgroup_and_custom_colocation_id OWNER TO tablegroup_test_user;

--
-- Name: tgroup_with_spc; Type: TABLE; Schema: public; Owner: tablegroup_test_user
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16517'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16516'::pg_catalog.oid);

CREATE TABLE public.tgroup_with_spc (
    a integer
)
WITH (colocation_id='20001')
TABLEGROUP grp_with_spc;


ALTER TABLE public.tgroup_with_spc OWNER TO tablegroup_test_user;

--
-- Name: th1; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16520'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16519'::pg_catalog.oid);

CREATE TABLE public.th1 (
    a integer,
    b text,
    c double precision
)
SPLIT INTO 2 TABLETS;


ALTER TABLE public.th1 OWNER TO yugabyte_test;

--
-- Name: th2; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16523'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16522'::pg_catalog.oid);

CREATE TABLE public.th2 (
    a integer NOT NULL,
    b text NOT NULL,
    c double precision,
    CONSTRAINT th2_pkey PRIMARY KEY((a) HASH, b ASC)
)
SPLIT INTO 3 TABLETS;


ALTER TABLE public.th2 OWNER TO yugabyte_test;

--
-- Name: th3; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16528'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16527'::pg_catalog.oid);

CREATE TABLE public.th3 (
    a integer NOT NULL,
    b text NOT NULL,
    c double precision,
    CONSTRAINT th3_pkey PRIMARY KEY((a, b) HASH)
)
SPLIT INTO 4 TABLETS;


ALTER TABLE public.th3 OWNER TO yugabyte_test;

--
-- Name: tr1; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16533'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16532'::pg_catalog.oid);

CREATE TABLE public.tr1 (
    a integer NOT NULL,
    b text,
    c double precision,
    CONSTRAINT tr1_pkey PRIMARY KEY(a ASC)
)
SPLIT AT VALUES ((1), (100));


ALTER TABLE public.tr1 OWNER TO yugabyte_test;

--
-- Name: tr2; Type: TABLE; Schema: public; Owner: yugabyte_test
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16538'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16537'::pg_catalog.oid);

CREATE TABLE public.tr2 (
    a integer NOT NULL,
    b text NOT NULL,
    c double precision NOT NULL,
    CONSTRAINT tr2_pkey PRIMARY KEY(a DESC, b ASC, c DESC)
)
SPLIT AT VALUES ((100, 'a', 2.5), (50, 'n', MINVALUE), (1, 'z', -5.12000000000000011));


ALTER TABLE public.tr2 OWNER TO yugabyte_test;

--
-- Name: uaccount; Type: TABLE; Schema: public; Owner: regress_rls_alice
--


-- For binary upgrade, must preserve pg_type oid
SELECT pg_catalog.binary_upgrade_set_next_pg_type_oid('16481'::pg_catalog.oid);


-- For binary upgrade, must preserve pg_type array oid
SELECT pg_catalog.binary_upgrade_set_next_array_pg_type_oid('16480'::pg_catalog.oid);

CREATE TABLE public.uaccount (
    pguser name NOT NULL,
    seclv integer,
    CONSTRAINT uaccount_pkey PRIMARY KEY(pguser ASC)
);


ALTER TABLE public.uaccount OWNER TO regress_rls_alice;

--
-- Name: hints id; Type: DEFAULT; Schema: hint_plan; Owner: yugabyte_test
--

ALTER TABLE ONLY hint_plan.hints ALTER COLUMN id SET DEFAULT nextval('hint_plan.hints_id_seq'::regclass);


--
-- Name: tbl1 a; Type: DEFAULT; Schema: public; Owner: yugabyte_test
--

ALTER TABLE ONLY public.tbl1 ALTER COLUMN a SET DEFAULT nextval('public.tbl1_a_seq'::regclass);


--
-- Name: tbl2 a; Type: DEFAULT; Schema: public; Owner: yugabyte_test
--

ALTER TABLE ONLY public.tbl2 ALTER COLUMN a SET DEFAULT nextval('public.tbl2_a_seq'::regclass);


--
-- Data for Name: hints; Type: TABLE DATA; Schema: hint_plan; Owner: yugabyte_test
--

COPY hint_plan.hints (id, norm_query_string, application_name, hints) FROM stdin;
\.


--
-- Data for Name: chat_user; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.chat_user ("chatID") FROM stdin;
\.


--
-- Data for Name: p1; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.p1 (k, v) FROM stdin;
\.


--
-- Data for Name: p2; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.p2 (k, v) FROM stdin;
\.


--
-- Data for Name: rls_private; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.rls_private (k, v) FROM stdin;
\.


--
-- Data for Name: rls_public; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.rls_public (k, v) FROM stdin;
\.


--
-- Data for Name: tbl1; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl1 (a, b) FROM stdin;
1	100
\.


--
-- Data for Name: tbl10; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl10 (a, b, c, d) FROM stdin;
\.


--
-- Data for Name: tbl11; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl11 (a, b, c) FROM stdin;
\.


--
-- Data for Name: tbl12; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl12 (a, b, c, d) FROM stdin;
\.


--
-- Data for Name: tbl13; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl13 (a, b, c, d) FROM stdin;
\.


--
-- Data for Name: tbl2; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl2 (a) FROM stdin;
\.


--
-- Data for Name: tbl3; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl3 (a, b) FROM stdin;
\.


--
-- Data for Name: tbl4; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl4 (a, b) FROM stdin;
\.


--
-- Data for Name: tbl5; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl5 (a, b, c) FROM stdin;
4	7	16
\.


--
-- Data for Name: tbl6; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl6 (a) FROM stdin;
\.


--
-- Data for Name: tbl7; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl7 (a, b, c, d) FROM stdin;
\.


--
-- Data for Name: tbl8; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl8 (a, b, c, d) FROM stdin;
\.


--
-- Data for Name: tbl9; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tbl9 (a, b, c) FROM stdin;
\.


--
-- Data for Name: tgroup_after_options; Type: TABLE DATA; Schema: public; Owner: tablegroup_test_user
--

COPY public.tgroup_after_options (a) FROM stdin;
\.


--
-- Data for Name: tgroup_empty_options; Type: TABLE DATA; Schema: public; Owner: tablegroup_test_user
--

COPY public.tgroup_empty_options (a) FROM stdin;
\.


--
-- Data for Name: tgroup_in_between_options; Type: TABLE DATA; Schema: public; Owner: tablegroup_test_user
--

COPY public.tgroup_in_between_options (a) FROM stdin;
\.


--
-- Data for Name: tgroup_no_options_and_tgroup; Type: TABLE DATA; Schema: public; Owner: tablegroup_test_user
--

COPY public.tgroup_no_options_and_tgroup (a) FROM stdin;
\.


--
-- Data for Name: tgroup_one_option; Type: TABLE DATA; Schema: public; Owner: tablegroup_test_user
--

COPY public.tgroup_one_option (a) FROM stdin;
\.


--
-- Data for Name: tgroup_one_option_and_tgroup; Type: TABLE DATA; Schema: public; Owner: tablegroup_test_user
--

COPY public.tgroup_one_option_and_tgroup (a) FROM stdin;
\.


--
-- Data for Name: tgroup_options; Type: TABLE DATA; Schema: public; Owner: tablegroup_test_user
--

COPY public.tgroup_options (a) FROM stdin;
\.


--
-- Data for Name: tgroup_options_and_tgroup; Type: TABLE DATA; Schema: public; Owner: tablegroup_test_user
--

COPY public.tgroup_options_and_tgroup (a) FROM stdin;
\.


--
-- Data for Name: tgroup_options_tgroup_and_custom_colocation_id; Type: TABLE DATA; Schema: public; Owner: tablegroup_test_user
--

COPY public.tgroup_options_tgroup_and_custom_colocation_id (a) FROM stdin;
\.


--
-- Data for Name: tgroup_with_spc; Type: TABLE DATA; Schema: public; Owner: tablegroup_test_user
--

COPY public.tgroup_with_spc (a) FROM stdin;
\.


--
-- Data for Name: th1; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.th1 (a, b, c) FROM stdin;
\.


--
-- Data for Name: th2; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.th2 (a, b, c) FROM stdin;
\.


--
-- Data for Name: th3; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.th3 (a, b, c) FROM stdin;
\.


--
-- Data for Name: tr1; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tr1 (a, b, c) FROM stdin;
\.


--
-- Data for Name: tr2; Type: TABLE DATA; Schema: public; Owner: yugabyte_test
--

COPY public.tr2 (a, b, c) FROM stdin;
\.


--
-- Data for Name: uaccount; Type: TABLE DATA; Schema: public; Owner: regress_rls_alice
--

COPY public.uaccount (pguser, seclv) FROM stdin;
\.


--
-- Name: hints_id_seq; Type: SEQUENCE SET; Schema: hint_plan; Owner: yugabyte_test
--

SELECT pg_catalog.setval('hint_plan.hints_id_seq', 1, false);


--
-- Name: tbl1_a_seq; Type: SEQUENCE SET; Schema: public; Owner: yugabyte_test
--

SELECT pg_catalog.setval('public.tbl1_a_seq', 1, true);


--
-- Name: tbl2_a_seq; Type: SEQUENCE SET; Schema: public; Owner: yugabyte_test
--

SELECT pg_catalog.setval('public.tbl2_a_seq', 1, false);


--
-- Name: p1 c1; Type: CONSTRAINT; Schema: public; Owner: yugabyte_test
--

CREATE UNIQUE INDEX NONCONCURRENTLY c1 ON public.p1 USING lsm (v ASC) SPLIT AT VALUES (('foo'), ('qux'));

ALTER TABLE ONLY public.p1
    ADD CONSTRAINT c1 UNIQUE USING INDEX c1;


--
-- Name: p2 c2; Type: CONSTRAINT; Schema: public; Owner: yugabyte_test
--

CREATE UNIQUE INDEX NONCONCURRENTLY c2 ON public.p2 USING lsm (v HASH) SPLIT INTO 10 TABLETS;

ALTER TABLE ONLY public.p2
    ADD CONSTRAINT c2 UNIQUE USING INDEX c2;


--
-- Name: hints_norm_and_app; Type: INDEX; Schema: hint_plan; Owner: yugabyte_test
--

CREATE UNIQUE INDEX hints_norm_and_app ON hint_plan.hints USING lsm (norm_query_string HASH, application_name ASC) SPLIT INTO 3 TABLETS;


--
-- Name: tbl8_idx; Type: INDEX; Schema: public; Owner: yugabyte_test
--

CREATE INDEX tbl8_idx ON public.tbl8 USING lsm ((b, c) HASH) SPLIT INTO 3 TABLETS;


--
-- Name: tbl8_idx2; Type: INDEX; Schema: public; Owner: yugabyte_test
--

CREATE INDEX tbl8_idx2 ON public.tbl8 USING lsm (a HASH, b ASC) SPLIT INTO 3 TABLETS;


--
-- Name: tbl8_idx3; Type: INDEX; Schema: public; Owner: yugabyte_test
--

CREATE INDEX tbl8_idx3 ON public.tbl8 USING lsm (b ASC);


--
-- Name: tbl8_idx4; Type: INDEX; Schema: public; Owner: yugabyte_test
--

CREATE INDEX tbl8_idx4 ON public.tbl8 USING lsm (b DESC);


--
-- Name: tbl8_idx5; Type: INDEX; Schema: public; Owner: yugabyte_test
--

CREATE INDEX tbl8_idx5 ON public.tbl8 USING lsm (c HASH) SPLIT INTO 3 TABLETS;


--
-- Name: th2_c_b_idx; Type: INDEX; Schema: public; Owner: yugabyte_test
--

CREATE INDEX th2_c_b_idx ON public.th2 USING lsm (c HASH, b DESC) SPLIT INTO 4 TABLETS;


--
-- Name: th3_c_b_idx; Type: INDEX; Schema: public; Owner: yugabyte_test
--

CREATE INDEX th3_c_b_idx ON public.th3 USING lsm ((c, b) HASH) SPLIT INTO 3 TABLETS;


--
-- Name: tr2_c_b_a_idx; Type: INDEX; Schema: public; Owner: yugabyte_test
--

CREATE INDEX tr2_c_b_a_idx ON public.tr2 USING lsm (c ASC, b DESC, a ASC) SPLIT AT VALUES ((-5.12000000000000011, 'z', 1), (-0.75, 'l', MINVALUE), (2.5, 'a', 100));


--
-- Name: tr2_c_idx; Type: INDEX; Schema: public; Owner: yugabyte_test
--

CREATE INDEX tr2_c_idx ON public.tr2 USING lsm (c DESC) SPLIT AT VALUES ((100.5), (1.5));


--
-- Name: uaccount account_policies; Type: POLICY; Schema: public; Owner: regress_rls_alice
--

CREATE POLICY account_policies ON public.uaccount USING ((pguser = CURRENT_USER));


--
-- Name: rls_public p1; Type: POLICY; Schema: public; Owner: yugabyte_test
--

CREATE POLICY p1 ON public.rls_public USING (((k % 2) = 0));


--
-- Name: rls_private p2; Type: POLICY; Schema: public; Owner: yugabyte_test
--

CREATE POLICY p2 ON public.rls_private FOR INSERT WITH CHECK (((k % 2) = 1));


--
-- Name: rls_private p3; Type: POLICY; Schema: public; Owner: yugabyte_test
--

CREATE POLICY p3 ON public.rls_private FOR UPDATE USING (((k % 2) = 1));


--
-- Name: rls_private; Type: ROW SECURITY; Schema: public; Owner: yugabyte_test
--

ALTER TABLE public.rls_private ENABLE ROW LEVEL SECURITY;

--
-- Name: rls_public; Type: ROW SECURITY; Schema: public; Owner: yugabyte_test
--

ALTER TABLE public.rls_public ENABLE ROW LEVEL SECURITY;

--
-- Name: uaccount; Type: ROW SECURITY; Schema: public; Owner: regress_rls_alice
--

ALTER TABLE public.uaccount ENABLE ROW LEVEL SECURITY;

--
-- Name: pg_hint_plan; Type: EXTENSION; Schema: -; Owner:
--

-- YB: ensure extconfig field for extension: pg_hint_plan in pg_extension catalog is correct
UPDATE pg_extension SET extconfig = ARRAY['hint_plan.hints'::regclass::oid,'hint_plan.hints_id_seq'::regclass::oid]::oid[] WHERE extname = 'pg_hint_plan';


--
-- Name: SCHEMA hint_plan; Type: ACL; Schema: -; Owner: yugabyte_test
--

SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);
GRANT USAGE ON SCHEMA hint_plan TO PUBLIC;
SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);


--
-- Name: FUNCTION pg_stat_statements_reset(); Type: ACL; Schema: pg_catalog; Owner: postgres
--

SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);
REVOKE ALL ON FUNCTION pg_catalog.pg_stat_statements_reset() FROM PUBLIC;
SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);


--
-- Name: TABLE hints; Type: ACL; Schema: hint_plan; Owner: yugabyte_test
--

SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);
GRANT SELECT ON TABLE hint_plan.hints TO PUBLIC;
SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);


--
-- Name: TABLE pg_stat_statements; Type: ACL; Schema: pg_catalog; Owner: postgres
--

SELECT pg_catalog.binary_upgrade_set_record_init_privs(true);
GRANT SELECT ON TABLE pg_catalog.pg_stat_statements TO PUBLIC;
SELECT pg_catalog.binary_upgrade_set_record_init_privs(false);


--
-- Name: TABLE rls_private; Type: ACL; Schema: public; Owner: yugabyte_test
--

GRANT SELECT ON TABLE public.rls_private TO rls_user;


--
-- Name: TABLE rls_public; Type: ACL; Schema: public; Owner: yugabyte_test
--

GRANT ALL ON TABLE public.rls_public TO PUBLIC;


--
-- YSQL database dump complete
--

