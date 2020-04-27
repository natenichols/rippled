\set ON_ERROR_STOP on
SET client_min_messages = WARNING;
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
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: -
--

-- CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;

--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: -
--

-- COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: account_transactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.account_transactions (
    account           bytea  NOT NULL,
    ledger_seq        bigint NOT NULL,
    transaction_index bigint NOT NULL,
    trans_id          bytea  NOT NULL
);


--
-- Name: ledgers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ledgers (
    ledger_seq        bigint PRIMARY KEY,
    ledger_hash       bytea  UNIQUE NOT NULL,
    prev_hash         bytea  UNIQUE NOT NULL,
    total_coins       bigint NOT NULL,
    closing_time      bigint NOT NULL,
    prev_closing_time bigint NOT NULL,
    close_time_res    bigint NOT NULL,
    close_flags       bigint NOT NULL,
    account_set_hash  bytea  NOT NULL,
    trans_set_hash    bytea  NOT NULL
);


--
-- Name: account_transactions account_transactions_pkey;
-- Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.account_transactions
    ADD CONSTRAINT account_transactions_pkey PRIMARY KEY (
    account, ledger_seq, transaction_index);

CREATE INDEX account_transactions_trans_id ON public.account_transactions
    USING btree (trans_id);


--
-- Name: ledgers ledgers_hash_unique; Type: CONSTRAINT;
-- Schema: public; Owner: -
--

--ALTER TABLE ONLY public.ledgers
--    ADD CONSTRAINT ledgers_hash_unique UNIQUE (ledger_hash);

--ALTER TABLE ONLY public.ledgers
--    ADD CONSTRAINT ledgers_parent_hash_unique UNIQUE (parent_hash);

--
-- Name: ledgers ledgers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

--ALTER TABLE ONLY public.ledgers
--    ADD CONSTRAINT ledgers_pkey PRIMARY KEY (ledger_seq);

CREATE RULE ledgers_update_protect AS ON UPDATE TO
    public.ledgers DO INSTEAD NOTHING;


--
-- Name: transactions transactions_pkey; Type: CONSTRAINT;
-- Schema: public; Owner: -
--

--ALTER TABLE ONLY public.transactions
--    ADD CONSTRAINT transactions_pkey PRIMARY KEY (
--    ledger_seq, transaction_index);

--CREATE RULE transactions_update_protect AS ON UPDATE TO
--    public.transactions DO INSTEAD NOTHING;

--
-- Name: fki_account_transactions_fkey; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX fki_account_transactions_fkey ON public.account_transactions
    USING btree (ledger_seq);


--
-- Name: transactions_hash_idx; Type: INDEX; Schema: public; Owner: -
--

--CREATE INDEX transactions_hash_idx ON public.transactions
--    USING btree (((tx ->> 'hash'::text)));


--
-- Name: account_transactions account_transactions_fkey;
-- Type: FK CONSTRAINT; Schema: public; Owner: -
--

--ALTER TABLE ONLY public.account_transactions
--    ADD CONSTRAINT account_transactions_fkey FOREIGN KEY (
--    ledger_seq, transaction_index) REFERENCES public.transactions(
--    ledger_seq, transaction_index) ON DELETE CASCADE;

--CREATE RULE account_transactions_update_protect AS ON UPDATE TO
--    public.account_transactions DO INSTEAD NOTHING;

--
-- Name: account_transactions account_transactions_fkey; Type: FK CONSTRAINT;
-- Schema: public; Owner: -
--

ALTER TABLE ONLY public.account_transactions
    ADD CONSTRAINT account_transactions_fkey FOREIGN KEY (ledger_seq)
    REFERENCES public.ledgers(ledger_seq) ON DELETE CASCADE;

