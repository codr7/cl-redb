(in-package redb)

(define-foreign-library libpq (t (:default "/usr/lib/x86_64-linux-gnu/libpq")))

(use-foreign-library libpq)

(defctype PGconn :pointer)

(defcenum ConnStatusType
  :CONNECTION_OK
  :CONNECTION_BAD)

;; PGconn *PQconnectdb(const char *conninfo)
(defcfun "PQconnectdb" PGconn (conninfo :string))

;; void PQfinish(PGconn *conn)
(defcfun "PQfinish" :void (conn PGconn))

;;ConnStatusType PQstatus(const PGconn *conn)
(defcfun "PQstatus" ConnStatusType (conn PGconn))

;;char *PQerrorMessage(const PGconn *conn);
(defcfun "PQerrorMessage" :string (conn PGConn))

(defctype PGresult :pointer)

(defcenum ExecStatusType
  :PGRES_EMPTY_QUERY
  :PGRES_COMMAND_OK
  :PGRES_TUPLES_OK
  :PGRES_COPY_IN
  :PGRES_COPY_OUT
  :PGRES_BAD_RESPONSE :PGRES_NONFATAL_ERROR
  :PGRES_FATAL_ERROR
  :PGRES_COPY_BOTH
  :PGRES_SINGLE_TUPLE
  :PGRES_PIPELINE_SYNC
  :PGRES_PIPELINE_ABORTED)

;;int PQsendQueryParams(PGconn *conn,
;;                      const char *command,
;;                      int nParams,
;;                      const Oid *paramTypes,
;;                      const char * const *paramValues,
;;                      const int *paramLengths,
;;                      const int *paramFormats,
;;                      int resultFormat);
(defcfun "PQsendQueryParams" :int
  (conn PGconn)
  (command :string)
  (nParams :int)
  (paramTypes :pointer)
  (paramValues :pointer)
  (paramLengths :pointer)
  (paramFormats :pointer)
  (resultFormat :int))

;;PGresult *PQgetResult(PGconn *conn);
(defcfun "PQgetResult" PGresult (conn PGconn))
    
;; void PQclear(PGresult *res);
(defcfun "PQclear" :void (res PGresult))

;;ExecStatusType PQresultStatus(const PGresult *res);
(defcfun "PQresultStatus" ExecStatusType (res PGresult))

;;char *PQerrorMessage(const PGconn *conn);
(defcfun "PQresultErrorMessage" :string (res PGresult))

;;int PQntuples(const PGresult *res);
(defcfun "PQntuples" :int (res PGresult))

;;int PQnfields(const PGresult *res);
(defcfun "PQnfields" :int (res PGresult))

;; char *PQfname(const PGresult *res, int column_number)
(defcfun "PQfname" :string (res PGresult) (column_number :int))

;; char *PQgetvalue(const PGresult *res, int row_number, int column_number)
(defcfun "PQgetvalue" :string (res PGresult) (row_number :int) (column_number :int))

(defmacro do-result ((var res) &body body)
  `(let ((,var ,res))
     (unwind-protect
	  (progn ,@body)
       (PQclear ,var))))
