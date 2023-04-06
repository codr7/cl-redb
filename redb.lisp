(defpackage redb
  (:use cffi cl)
  (:export *cx* *db* *mig-db*
	   add-enum
	   begin boolean-col
	   col cols commit create cx-val
	   db def define-db delete-rec down drop
	   enum-col exec exists?
	   field find-def find-field
	   find-rec from-sql
	   id-col integer-col
	   join-fkey join-table
	   load-rec
	   modified? mig
	   name new-boolean-col new-cx new-fkey new-id-col new-integer-col new-json-col new-key
	   new-mig new-query new-rec new-seq  new-string-col new-table new-text-col new-tstamp-col
	   next next-val now
	   params pkey prepare push-mig
	   rec rec-exists? rec= recv del-enum rollback run-mig run-mig-up run-mig-down
	   select set-key set-rec send send-cmd send-dml send-prepared send-val sql string-col store-field
	   store-rec stored? stored-val
	   table table-create table-drop table-exists? to-sql tstamp tstamp-col tstamp= tx-val
	   up
	   with-cx with-db with-query with-result with-tx

	   ConnStatusType ExecStatusType
	   PGconn PGresult
	   PQclear PQcmdTuples PQconnectdb PQerrorMessage PQgetResult PQgetvalue PQfinish PQfname PQnfields
	   PQntuples PQresultErrorMessage PQresultStatus PQsendQueryParams PQsendQueryPrepared PQsendPrepare
	   PQstatus))
