-- Partition maintenance callable by the least-privilege service role. Creating, detaching and dropping
-- partitions needs table ownership, so the function runs as its owner (the migration role) and does only
-- that. It returns the DEFAULT partitions that hold rows: rows there mean a missing day partition or a bad
-- device clock (design.md: alarm when DEFAULT rows > 0).
CREATE SCHEMA partman_maint;

CREATE FUNCTION partman_maint.run() RETURNS SETOF text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET timezone = 'UTC'                       -- day boundaries in UTC, as in the create_parent calls
AS $$
DECLARE
  default_table text;
  has_rows boolean;
BEGIN
  PERFORM partman.run_maintenance();
  FOR default_table IN
    SELECT format('%I.%I', n.nspname, c.relname)
    FROM partman.part_config p
    JOIN pg_inherits i ON i.inhparent = p.parent_table::regclass
    JOIN pg_class c ON c.oid = i.inhrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'
    ORDER BY 1
  LOOP
    EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)', default_table) INTO has_rows;
    IF has_rows THEN
      RETURN NEXT default_table;
    END IF;
  END LOOP;
END;
$$;

REVOKE ALL ON FUNCTION partman_maint.run() FROM PUBLIC;
