MODULE_big = planhint1c
DOCS = README.planhint1c
REGRESS = planhint1c
OBJS=planhint1c.o

ifdef USE_PGXS
PGXS = $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/planhint1c
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

#top_srcdir = ../..
include $(top_srcdir)/contrib/contrib-global.mk
endif
