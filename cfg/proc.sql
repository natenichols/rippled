-- Deploying this file idempotentently configures custom functions and
-- related objects.

\set ON_ERROR_STOP on
SET client_min_messages = WARNING;

-- rippled error codes. To modify, update the error_label_enum and
-- the array in the anonymous function just below. PLpg/SQL doesn't have a
-- tidy way to programmatically insert new enumerated type values.
DROP TYPE IF EXISTS error_label_enum cascade;
CREATE TYPE error_label_enum AS ENUM (
    'rpcINVALID_PARAMS',
    'rpcNOT_IMPL',
    'rpcTXN_NOT_FOUND',
    'rpcINTERNAL',
    'rpcLGR_IDXS_INVALID',
    'rpcACT_MALFORMED',
    'rpcLGR_NOT_FOUND'
);

-- Attributes of rippled errors.
DROP TABLE IF EXISTS errors;
CREATE TABLE IF NOT EXISTS errors (
    label   error_label_enum,
    code    int,
    error   text,
    message text
);

-- Anonymous function to populate the errors table. Update the array
-- in addition to the enum definition to make changes.
DO $$
DECLARE
    _error_list errors[] := ARRAY[
        ('rpcINVALID_PARAMS', 31, 'invalidParams', 'Invalid parameters.'),
        ('rpcNOT_IMPL', 74, 'notImpl', 'Not implemented.'),
        ('rpcTXN_NOT_FOUND', 29, 'txnNotFound', 'Transaction not found.'),
        ('rpcINTERNAL', 73, 'internal', 'Internal error.'),
        ('rpcLGR_IDXS_INVALID', 57, 'lgrIdxsInvalid',
		'Ledger indexes invalid.'),
        ('rpcACT_MALFORMED', 35, 'actMalformed', 'Account malformed.'),
        ('rpcLGR_NOT_FOUND', 21, 'lgrNotFound', 'Ledger not found.')
    ];
    _error errors;
BEGIN
    FOREACH _error IN ARRAY _error_list LOOP
        INSERT INTO errors VALUES (
            _error.label,
            _error.code,
            _error.error,
            _error.message
        );
    END LOOP;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS errors_label_idx on errors (label);
VACUUM ANALYZE errors;

-- Return json error object based on error type.
CREATE OR REPLACE FUNCTION return_error (
    _in_error error_label_enum,
    _in_error_text text = NULL
) RETURNS jsonb AS $$
DECLARE
    _record record;
BEGIN
    EXECUTE 'SELECT * FROM errors WHERE label = $1'
        INTO _record USING _in_error;
    RETURN jsonb_build_object('result',
        jsonb_build_object('status', 'error',
            'error_message', _record.message,
            'error', (SELECT CASE WHEN _in_error_text IS NOT NULL THEN
                _in_error_text ELSE _record.error END),
            'error_code', _record.code
        )
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tx (
    _in_trans_id bytea
) RETURNS jsonb AS $$
DECLARE
    _min_seq           bigint := (SELECT ledger_seq
	                            FROM ledgers
				   WHERE ledger_seq = min_ledger()
			             FOR UPDATE);
    _max_seq           bigint := max_ledger();
    _ledger_seq        bigint;
BEGIN
    IF _min_seq IS NULL THEN
        RETURN jsonb_build_object('error', 'empty database');
    END IF;
    IF length(_in_trans_id) != 32 THEN
        RETURN jsonb_build_object('error', '_in_trans_id size: '
            || to_char(length(_in_trans_id), '999'));
    END IF;

    EXECUTE 'SELECT ledger_seq
               FROM account_transactions
	      WHERE trans_id = $1
                AND ledger_seq BETWEEN $2 AND $3
	      LIMIT 1
    ' INTO _ledger_seq USING _in_trans_id, _min_seq, _max_seq;
    IF _ledger_seq IS NULL THEN
        RETURN jsonb_build_object('min_seq', _min_seq, 'max_seq', _max_seq);
    END IF;
    RETURN jsonb_build_object('ledger_seq', _ledger_seq);
END;
$$ LANGUAGE plpgsql;

-- Equivalent to rippled method tx()
CREATE OR REPLACE FUNCTION tx_old (
    _in_transaction text
) RETURNS jsonb AS $$
DECLARE
    _tx   jsonb;
    _meta jsonb;
BEGIN
    IF _in_transaction IS NULL THEN
        RETURN return_error('rpcINVALID_PARAMS');
    ELSIF is_hex_txid(_in_transaction) IS FALSE THEN
        RETURN return_error('rpcNOT_IMPL');
    END IF;

    EXECUTE 'SELECT tx, meta
               FROM transactions
              WHERE tx ->> ''hash'' = $1
    ' INTO _tx, _meta USING _in_transaction;
    IF _tx IS NULL THEN RETURN return_error('rpcTXN_NOT_FOUND'); END IF;

    RETURN jsonb_build_object('result', _tx ||
        jsonb_build_object('meta', _meta, 'status', 'success',
            'validated', TRUE));
END;
$$ LANGUAGE plpgsql;

-- Return the earliest ledger sequence intended for range operations
-- that protect the bottom of the range from deletion. Return NULL if empty.
CREATE OR REPLACE FUNCTION min_ledger () RETURNS bigint AS $$
DECLARE
    _min_seq bigint := (SELECT ledger_seq from min_seq);
BEGIN
    IF _min_seq IS NULL THEN
        RETURN (SELECT ledger_seq FROM ledgers ORDER BY ledger_seq ASC LIMIT 1);
    ELSE
	RETURN _min_seq;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Return the latest ledger sequence in the database, or NULL if empty.
CREATE OR REPLACE FUNCTION max_ledger () RETURNS bigint AS $$
BEGIN
    RETURN (SELECT ledger_seq FROM ledgers ORDER BY ledger_seq DESC LIMIT 1);
END;
$$ LANGUAGE plpgsql;

-- Check if transactionid is 256 bits represented in hex.
CREATE OR REPLACE FUNCTION is_hex_txid (
    _in_hash text
) RETURNS bool AS $$
BEGIN
    RETURN _in_hash ~* '^[0-9a-f]{64}$';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION play (
) RETURNS RECORD AS $$
DECLARE
    _ret RECORD;
    _arr bytea[];
BEGIN
    _arr := ARRAY[E'\\x0d0a'];
    _arr := array_prepend(_arr, ARRAY[E'\\x0a0d']);
    SELECT 'heh', 73, _arr into _ret;
    RETURN _ret;
END;
$$ LANGUAGE plpgsql;

/*
 * account_tx helper. From the rippled reporting process, only the
 * parameters without defaults are required. For the parameters with
 * defaults, validation should be done by rippled, such as:
 * _in_account_id should be a valid xrp base58 address.
 * _in_forward either true or false according to the published api
 * _in_limit should be validated and not simply passed through from
 *     client.
 *
 * For _in_ledger_index_min and _in_ledger_index_max, if passed in the
 * request, verify that their type is int and pass through as is.
 * For _ledger_hash, verify and convert from hex length 32 bytes and
 * prepend with \x ("\\x" C++).
 *
 * For _in_ledger_index, if the input type is integer, then pass through
 * as is. If the type is string and contents = validated, then do not
 * set _in_ledger_index. Instead set _in_invalidated to TRUE.
 *
 * There is no need for rippled to do any type of lookup on max/min
 * ledger range, lookup of hash, or the like. This functions does those
 * things, including error responses if bad input. Only the above must
 * be done to set the correct search range.
 *
 * If a marker is present in the request, verify the members "ledger"
 * and "seq" are integers and they correspond to _in_marker_seq
 * _in_marker_index.
 * To reiterate:
 * JSON input field "ledger" corresponds to _in_marker_seq
 * JSON input field "seq" corresponds to _in_marker_index
 */

CREATE OR REPLACE FUNCTION account_tx (
    _in_account_id bytea,
    _in_forward bool,
    _in_limit bigint,
    _in_ledger_index_min bigint = NULL,
    _in_ledger_index_max bigint = NULL,
    _in_ledger_hash      bytea  = NULL,
    _in_ledger_index     bigint = NULL,
    _in_validated bool   = NULL,
    _in_marker_seq       bigint = NULL,
    _in_marker_index     bigint = NULL
) RETURNS jsonb AS $$
DECLARE
    _min          bigint;
    _max          bigint;
    _sort_order   text       := (SELECT CASE WHEN _in_forward IS TRUE THEN
	                         'ASC' ELSE 'DESC' END);
    _marker       bool;
    _between_min  bigint;
    _between_max  bigint;
    _sql          text;
    _cursor       refcursor;
    _result       jsonb;
    _record       record;
    _tally        bigint     := 0;
    _ret_marker   jsonb;
    _transactions jsonb[]    := '{}';
BEGIN
    IF _in_ledger_index_min IS NOT NULL OR
            _in_ledger_index_max IS NOT NULL THEN
        _min := (SELECT CASE WHEN _in_ledger_index_min IS NULL
            THEN min_ledger() ELSE greatest(
            _in_ledger_index_min, min_ledger()) END);
        _max := (SELECT CASE WHEN _in_ledger_index_max IS NULL OR
            _in_ledger_index_max = -1 THEN max_ledger() ELSE
            least(_in_ledger_index_max, max_ledger()) END);

        IF _max < _min THEN
            RETURN jsonb_build_object('error', 'max is less than min ledger');
        END IF;

    ELSIF _in_ledger_hash IS NOT NULL OR _in_ledger_index IS NOT NULL
            OR _in_validated IS TRUE THEN
        IF _in_ledger_hash IS NOT NULL THEN
            IF length(_in_ledger_hash) != 32 THEN
                RETURN jsonb_build_object('error', '_in_ledger_hash size: '
                    || to_char(length(_in_ledger_hash), '999'));
            END IF;
            EXECUTE 'SELECT ledger_seq
                       FROM ledgers
                      WHERE ledger_hash = $1'
                INTO _min USING _in_ledger_hash::bytea;
        ELSE
            IF _in_ledger_index IS NOT NULL AND _in_validated IS TRUE THEN
                RETURN jsonb_build_object('error',
		    '_in_ledger_index cannot be set and _in_validated true');
            END IF;
            IF _in_validated IS TRUE THEN
                _in_ledger_index := max_ledger();
            END IF;
            _min := (SELECT ledger_seq
                       FROM ledgers
                      WHERE ledger_seq = _in_ledger_index);
        END IF;
        IF _min IS NULL THEN
            RETURN jsonb_build_object('error', 'ledger not found');
        END IF;
        _max := _min;
    ELSE
        _min := min_ledger();
        _max := max_ledger();
    END IF;

    IF _in_marker_seq IS NOT NULL OR _in_marker_index IS NOT NULL THEN
        _marker := TRUE;
        IF _in_marker_seq IS NULL OR _in_marker_index IS NULL THEN
            -- The rippled implementation returns no transaction results
            -- if either of these values are missing.
            _between_min := 0;
            _between_max := 0;
        ELSE
            IF _in_forward IS TRUE THEN
                _between_min := _in_marker_seq;
                _between_max := _max;
            ELSE
                _between_min := _min;
                _between_max := _in_marker_seq;
            END IF;
        END IF;
    ELSE
	_marker := FALSE;
        _between_min := _min;
        _between_max := _max;
    END IF;
    IF _between_max < _between_min THEN
        RETURN jsonb_build_object('error', 'ledger search range is '
	    || to_char(_between_min, '999') || '-'
	    || to_char(_between_max, '999'));
    END IF;

    _sql := format('
	SELECT ledger_seq, transaction_index, trans_id
	  FROM account_transactions
	 WHERE account = $1
	   AND ledger_seq BETWEEN $2 AND $3
	 ORDER BY ledger_seq %s,
	       transaction_index %s
	', _sort_order, _sort_order);

    OPEN _cursor FOR EXECUTE _sql USING _in_account_id, _between_min,
            _between_max;
    LOOP
        FETCH _cursor INTO _record;
        IF _record IS NULL THEN EXIT; END IF;
        IF _marker IS TRUE THEN
            IF _in_marker_seq = _record.ledger_seq THEN
                IF _in_forward IS TRUE THEN
                    IF _in_marker_index > _record.transaction_index THEN
                        CONTINUE;
                    END IF;
                ELSE
                    IF _in_marker_index < _record.transaction_index THEN
                        CONTINUE;
                    END IF;
                END IF;
            END IF;
            _marker := FALSE;
        END IF;

        _tally := _tally + 1;
        IF _tally > _in_limit THEN
            _ret_marker := jsonb_build_object(
                'ledger', _record.ledger_seq,
                'seq', _record.transaction_index);
            EXIT;
        END IF;

        -- Is the transaction index in the tx object?
        _transactions := _transactions || jsonb_build_object(
            'ledger_seq', _record.ledger_seq,
            'transaction_index', _record.transaction_index,
            'trans_id', _record.trans_id);

    END LOOP;
    CLOSE _cursor;

    _result := jsonb_build_object('ledger_index_min', _min,
        'ledger_index_max', _max,
        'transactions', _transactions);
    IF _ret_marker IS NOT NULL THEN
        _result := _result || jsonb_build_object('marker', _ret_marker);
    END IF;
    RETURN _result;

END;
$$ LANGUAGE plpgsql;

-- OLD Equivalent to rippled account_tx() method.
CREATE OR REPLACE FUNCTION account_tx_old (
    _in_account          text  = NULL,
    _in_ledger_index_min bigint   = NULL,
    _in_ledger_index_max bigint   = NULL,
    _in_ledger_hash      text  = NULL,
    _in_ledger_index     bigint   = NULL,
    _in_validated        bool  = FALSE,
    _in_forward          bool  = FALSE,
    _in_limit            int   = NULL,
    _in_marker_seq       bigint   = NULL,
    _in_marker_index     bigint   = NULL
) RETURNS jsonb AS $$
DECLARE
    _limit        int;
    _min          bigint;
    _max          bigint;
    _between_min  bigint;
    _between_max  bigint;
    _marker       bool    := FALSE;
    _record       record;
    _sort_order   text    := (SELECT CASE WHEN _in_forward IS TRUE THEN 'ASC'
                              ELSE 'DESC' END);
    _sql          text;
    _transactions jsonb[] := '{}';
    _tally        int     := 0;
    _ret_marker   jsonb;
    _cursor       refcursor;
    _result       jsonb;
BEGIN
    IF _in_limit < 0 THEN RETURN return_error('rpcINTERNAL'); END IF;
    IF NOT EXISTS (SELECT 1 FROM ledgers) THEN
        RETURN return_error('rpcLGR_IDXS_INVALID');
    END IF;
    IF _in_account IS NULL THEN
        RETURN return_error('rpcINVALID_PARAMS');
    END IF;
    IF _in_account ~ '^r[1-9A-HJ-NP-Za-km-z]{24,34}$' IS FALSE THEN
        RETURN return_error('rpcACT_MALFORMED');
    END IF;
    IF _in_forward IS NULL THEN _in_forward := FALSE; END IF;

    IF _in_ledger_index_min IS NOT NULL OR
            _in_ledger_index_max IS NOT NULL THEN
        _min := (SELECT CASE WHEN _in_ledger_index_min IS NULL
            THEN min_ledger() ELSE greatest(
            _in_ledger_index_min, min_ledger()) END);
        _max := (SELECT CASE WHEN _in_ledger_index_max IS NULL OR
            _in_ledger_index_max = -1 THEN max_ledger() ELSE
            least(_in_ledger_index_max, max_ledger()) END);

        IF _max < _min THEN RETURN return_error('rpcLGR_IDXS_INVALID'); END IF;
    ELSIF _in_ledger_hash IS NOT NULL OR _in_ledger_index IS NOT NULL
            OR _in_validated IS TRUE THEN
        IF _in_ledger_hash IS NOT NULL THEN
            IF is_hex_txid(_in_ledger_hash) IS FALSE THEN
                RETURN return_error('rpcINVALID_PARAMS', 'ledgerHashMalformed');
            END IF;
            EXECUTE 'SELECT ledger_seq
                       FROM ledgers
                      WHERE ledger_hash = $1'
                INTO _min USING ('\x' || _in_ledger_hash)::bytea;
        ELSE
            IF _in_ledger_index IS NOT NULL AND _in_validated IS TRUE THEN
                RETURN return_error('rpcINVALID_PARAMS');
            END IF;
            IF _in_validated IS TRUE THEN
                _in_ledger_index := max_ledger();
            END IF;
            _min := (SELECT ledger_seq
                       FROM ledgers
                      WHERE ledger_seq = _in_ledger_index);
        END IF;
        IF _min IS NULL THEN
            RETURN return_error('rpcLGR_NOT_FOUND', 'ledgerNotFound');
        END IF;
        _max := _min;
    ELSE
        _min := min_ledger();
        _max := max_ledger();
    END IF;

    _limit := (SELECT CASE WHEN _in_limit IS NULL OR _in_limit > 200 OR
        _in_limit = 0 THEN 200 ELSE _in_limit END);

    IF _in_marker_seq IS NOT NULL OR _in_marker_index IS NOT NULL THEN
        _marker := TRUE;
        IF _in_marker_seq IS NULL OR _in_marker_index IS NULL THEN
            -- The rippled implementation returns no transaction results
            -- if either of these values are missing.
            _between_min := 0;
            _between_max := 0;
        ELSE
            IF _in_forward IS TRUE THEN
                _between_min := _in_marker_seq;
                _between_max := _max;
            ELSE
                _between_min := _min;
                _between_max := _in_marker_seq;
            END IF;       
        END IF;
    ELSE
        _between_min := _min;
        _between_max := _max;
    END IF;

    IF _between_max < _between_min THEN
        RETURN return_error('rpcLGR_IDXS_INVALID');
    END IF;

    _sql := format('
        SELECT transactions.ledger_seq, transactions.transaction_index, tx, meta
          FROM transactions
               INNER JOIN account_transactions
                       ON transactions.ledger_seq =
                          account_transactions.ledger_seq
                          AND transactions.transaction_index =
                               account_transactions.transaction_index
         WHERE account_transactions.account = $1
           AND account_transactions.ledger_seq BETWEEN $2 AND $3
         ORDER BY account_transactions.ledger_seq %s,
               account_transactions.transaction_index %s
        ', _sort_order, _sort_order);

    OPEN _cursor FOR EXECUTE _sql USING _in_account, _between_min,
            _between_max;
    LOOP
        FETCH _cursor INTO _record;
        IF _record IS NULL THEN EXIT; END IF;
        IF _marker IS TRUE THEN
            IF _in_forward IS TRUE THEN
                IF _in_marker_index >
                        _record.transaction_index AND
                        _in_marker_seq = _record.ledger_seq THEN
                    CONTINUE;
                END IF;
            ELSE
                IF _in_marker_index <
                        _record.transaction_index AND
                        _in_marker_seq = _record.ledger_seq THEN
                    CONTINUE;
                END IF;
            END IF;
            _marker := FALSE;
        END IF;

        _tally := _tally + 1;
        IF _tally > _limit THEN
            _ret_marker := jsonb_build_object(
                'ledger', _record.ledger_seq,
                'seq', _record.transaction_index);
            EXIT;
        END IF;

        _transactions := _transactions || jsonb_build_object(
            'tx', _record.tx, 'meta', _record.meta, 'validated', TRUE);
    END LOOP;
    CLOSE _cursor;

    _result := jsonb_build_object('account', _in_account,
        'ledger_index_min', _min, 'ledger_index_max', _max,
        'transactions', _transactions);
    IF _in_limit IS NOT NULL THEN
        _result := _result || jsonb_build_object('limit', _in_limit);
    END IF;
    IF _ret_marker IS NOT NULL THEN
        _result := _result || jsonb_build_object('marker', _ret_marker);
    END IF;

    RETURN jsonb_build_object('result', _result ||
        jsonb_build_object('status', 'success'));

END;
$$ LANGUAGE plpgsql;

-- Trigger prior to insert on ledgers table. Validates length of hash fields.
-- Verifies ancestry based on ledger_hash & prev_hash as follows:
-- 1) If ledgers is empty, allows insert.
-- 2) For each new row, check for previous and later ledgers by a single
--    sequence. For each that exist, confirm ancestry based on hashes.
-- 3) Disallow inserts with no prior or next ledger by sequence if any
--    ledgers currently exist. This disallows gaps to be introduced by
--    way of inserting.
CREATE OR REPLACE FUNCTION insert_ancestry() RETURNS TRIGGER AS $$
DECLARE
    _parent bytea;
    _child  bytea;
BEGIN
    IF length(NEW.ledger_hash) != 32 OR length(NEW.prev_hash) != 32 THEN
        RAISE 'ledger_hash and parent_has must each be 32 bytes: %', NEW;
    END IF;

    IF (SELECT ledger_hash
          FROM ledgers
         ORDER BY ledger_seq DESC
         LIMIT 1) = NEW.prev_hash THEN RETURN NEW; END IF;

    IF NOT EXISTS (SELECT 1 FROM LEDGERS) THEN RETURN NEW; END IF;

    _parent := (SELECT ledger_hash
                  FROM ledgers
                 WHERE ledger_seq = NEW.ledger_seq - 1);
    _child  := (SELECT prev_hash
                  FROM ledgers
                 WHERE ledger_seq = NEW.ledger_seq + 1);
    IF _parent IS NULL AND _child IS NULL THEN
        RAISE 'Ledger Ancestry error: orphan.';
    END IF;
    IF _parent != NEW.prev_hash THEN
        RAISE 'Ledger Ancestry error: bad parent.';
    END IF;
    IF _child != NEW.ledger_hash THEN
        RAISE 'Ledger Ancestry error: bad child.';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ancestry_trigger ON ledgers;
CREATE TRIGGER ancestry_trigger BEFORE INSERT ON ledgers
    FOR EACH ROW EXECUTE PROCEDURE insert_ancestry();

-- Function to delete old data. All data belonging to ledgers prior to the
-- _in_seq parameter will be deleted.
CREATE OR REPLACE FUNCTION truncate_ledgers (
    _in_seq bigint
) RETURNS void AS $$
BEGIN
    DELETE FROM LEDGERS WHERE ledger_seq < _in_seq;
END;
$$ LANGUAGE plpgsql;

-- Track the minimum sequence that should be used for ranged queries
-- with protection against deletion during the query. This should
-- be updated before calling online_delete() to not block deleting that
-- range.
CREATE TABLE IF NOT EXISTS min_seq (
    ledger_seq bigint NOT NULL
);

-- Trigger to replace existing upon insert to min_seq table.
-- Ensures only 1 row in that table.
CREATE OR REPLACE FUNCTION delete_min_seq() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM min_seq) THEN
        DELETE FROM min_seq;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS min_seq_trigger ON min_seq;
CREATE TRIGGER min_seq_trigger BEFORE INSERT ON min_seq
    FOR EACH ROW EXECUTE PROCEDURE delete_min_seq();

-- Set the minimum sequence for use in ranged queries with protection
-- against deletion greater than or equal to the input parameter. This
-- should be called prior to online_delete() with the same parameter
-- value so that online_delete() is not blocked by range queries
-- that are protected against concurrent deletion of the ledger at
-- the bottom of the range. This function needs to be called from a 
-- separate transaction from that which executes online_delete().
CREATE OR REPLACE FUNCTION prepare_delete (
    _in_last_rotated bigint
) RETURNS void AS $$
BEGIN
    INSERT INTO min_seq VALUES (_in_last_rotated + 1);
END;
$$ LANGUAGE plpgsql;

-- Function to delete old data. All data belonging to ledgers prior to and 
-- equal to the _in_seq parameter will be deleted. This should be
-- called with the input parameter equivalent to the value of lastRotated
-- in rippled's online_delete routine.
CREATE OR REPLACE FUNCTION online_delete (
    _in_seq bigint
) RETURNS void AS $$
BEGIN
    DELETE FROM LEDGERS WHERE ledger_seq <= _in_seq;
END;
$$ LANGUAGE plpgsql;


-- Verify correct ancestry of ledgers in database:
-- Database to persist last-confirmed latest ledger with proper ancestry.
CREATE TABLE IF NOT EXISTS ancestry_verified (
    ledger_seq bigint NOT NULL
);

-- Trigger to replace existing upon insert to ancestry_verified table.
-- Ensures only 1 row in that table.
CREATE OR REPLACE FUNCTION delete_verified() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM ancestry_verified) THEN
        DELETE FROM ancestry_verified;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ancestry_verified_trigger ON ancestry_verified;
CREATE TRIGGER ancestry_verified_trigger BEFORE INSERT ON ancestry_verified
    FOR EACH ROW EXECUTE PROCEDURE delete_verified();

-- Function to verify ancestry of ledgers based on ledger_hash and prev_hash.
-- Raises exception upon failure and prints current and parent rows.
-- _in_full: If TRUE, verify entire table. Else verify starting from
--           value in ancestry_verfied table. If no value, then start
--           from lowest ledger.
-- _in_persist: If TRUE, persist the latest ledger with correct ancestry.
--              If an exception was raised because of failure, persist
--              the latest ledger prior to that which failed.
-- _in_min: If set and _in_full is not true, the starting ledger from which
--          to verify.
-- _in_max: If set and _in_full is not true, the latest ledger to verify.
CREATE OR REPLACE FUNCTION check_ancestry (
    _in_full    bool = TRUE,
    _in_persist bool = FALSE,
    _in_min      bigint = NULL,
    _in_max      bigint = NULL
) RETURNS void AS $$
DECLARE
    _min                 bigint;
    _max                 bigint;
    _last_verified       bigint;
    _parent          ledgers;
    _current         ledgers;
    _cursor        refcursor;
BEGIN
    IF _in_full IS TRUE AND
            (_in_min IS NOT NULL) OR (_in_max IS NOT NULL) THEN
        RAISE 'Cannot specify manual range and do full check.';
    END IF;

    IF _in_min IS NOT NULL THEN
        _min := _in_min;
    ELSIF _in_full IS NOT TRUE THEN
        _last_verified := (SELECT ledger_seq FROM ancestry_verified);
        IF _last_verified IS NULL THEN
            _min := min_ledger();
        ELSE
            _min := _last_verified + 1;
        END IF;
    ELSE
        _min := min_ledger();
    END IF;
    EXECUTE 'SELECT * FROM ledgers WHERE ledger_seq = $1'
        INTO _parent USING _min - 1;
    IF _last_verified IS NOT NULL AND _parent IS NULL THEN
        RAISE 'Verified ledger % doesn''t exist.', _last_verified;
    END IF;

    IF _in_max IS NOT NULL THEN
        _max := _in_max;
    ELSE
        _max := max_ledger();
    END IF;

    OPEN _cursor FOR EXECUTE 'SELECT *
                                FROM ledgers
                               WHERE ledger_seq BETWEEN $1 AND $2
                               ORDER BY ledger_seq ASC'
                               USING _min, _max;
    LOOP
        FETCH _cursor INTO _current;
        IF _current IS NULL THEN EXIT; END IF;
        IF _parent IS NOT NULL THEN
            IF _current.prev_hash != _parent.ledger_hash THEN
                CLOSE _cursor;
                RAISE 'Ledger ancestry failure current, parent:% %',
                    _current, _parent;
            END IF;
        END IF;
        _parent := _current;
    END LOOP;
    CLOSE _cursor;

    IF _in_persist IS TRUE AND _parent IS NOT NULL THEN
        INSERT INTO ancestry_verified VALUES (_parent.ledger_seq);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Return number of whole seconds since the latest ledger was inserted, based
-- on ledger close time, not wall-clock of the insert.
-- Note that ledgers.closing_time is number of seconds since the XRP
-- epoch, which is 01/01/2000 00:00:00. This in turn is 946684800 seconds
-- after the UNIX epoch.
CREATE OR REPLACE FUNCTION lag () RETURNS bigint AS $$
BEGIN
    RETURN (EXTRACT(EPOCH FROM (now())) -
        (946684800 + (SELECT closing_time
                        FROM ledgers
                       ORDER BY ledger_seq DESC
                       LIMIT 1)))::bigint;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION complete_ledgers () RETURNS text AS $$
DECLARE
    _min bigint = min_ledger();
BEGIN
    IF _min IS NULL THEN RETURN 'empty'; END IF;
    RETURN _min || '-' || max_ledger();
END;
$$ LANGUAGE plpgsql;

