EXTENSION = pgbouncer_fdw
EXTVERSION = $(shell grep default_version $(EXTENSION).control | \
               sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
               
DATA = $(filter-out $(wildcard sql/*--*.sql),$(wildcard sql/*.sql))
#DOCS = $(wildcard doc/*.md)
PG_CONFIG = pg_config

all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: $(sort $(wildcard sql/tables/*.sql)) $(sort $(wildcard sql/functions/*.sql)) $(sort $(wildcard sql/views/*.sql))
	cat $^ > $@

DATA = $(wildcard updates/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
