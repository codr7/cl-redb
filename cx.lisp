(in-package redb)

(defvar *cx*)

(defun connect (db user password &key (host "localhost"))
  (let ((pg (PQconnectdb (format nil "postgresql://~a:~a@~a/~a" user password host db))))
    (unless (eq (PQstatus pg) :CONNECTION_OK)
      (error (PQerrorMessage pg)))
    pg))

(defstruct cx
  (pg nil :type t)
  (stored-vals (make-hash-table) :type hash-table))

(defun new-cx (pg)
  (make-cx :pg pg))

(defmacro with-cx ((&rest args) &body body)
  `(let ((*cx* (new-cx (connect ,@args))))
     (when (not (eq (PQstatus (cx-pg *cx*)) :CONNECTION_OK))
       (error (PQerrorMessage (cx-pg *cx*))))

     (unwind-protect
	  (progn ,@body)
       (PQfinish (cx-pg *cx*)))))

(defun cx-val (fld &key (cx *cx*))
  (gethash fld (cx-stored-vals cx)))

(defun (setf cx-val) (val fld &key (cx *cx*))
  (sethash fld (cx-stored-vals cx) val))

(defmethod send (sql params)
  (let ((nparams (len params)))
    (with-foreign-object (cparams :pointer nparams)
      (let ((i 0))
	(dolist (p params)
	  (setf (mem-aref cparams :pointer i) (foreign-string-alloc p))
	  (incf i)))

      (unwind-protect
	   (when (zerop (PQsendQueryParams (cx-pg *cx*)
					 sql
					 nparams
					 (null-pointer) cparams (null-pointer) (null-pointer)
					 0))
	     (error (PQerrorMessage (cx-pg *cx*))))
	(dotimes (i (len params))
	  (foreign-string-free (mem-aref cparams :pointer i))))))
  
  nil)

(defmethod send-prepare (sql)
  (let ((name (gensym)))
    (when (zerop (PQsendPrepare (cx-pg *cx*)
				(sql-name name)
				sql
				0
				(null-pointer)))
      (error (PQerrorMessage (cx-pg *cx*))))
    (multiple-value-bind (result status) (recv)
      (assert (eq status :PGRES_COMMAND_OK))
      (PQclear result)
      (assert (null (recv))))
    name))

(defmethod send-prepared (name params)
  (let ((nparams (len params)))
    (with-foreign-object (cparams :pointer nparams)
      (let ((i 0))
	(dolist (p params)
	  (setf (mem-aref cparams :pointer i) (foreign-string-alloc p))
	  (incf i)))

      (unwind-protect
	   (when (zerop (PQsendQueryPrepared (cx-pg *cx*)
					     (sql-name name)
					     nparams
					     cparams (null-pointer) (null-pointer)
					     0))
	     (error (PQerrorMessage (cx-pg *cx*))))
	(dotimes (i (len params))
	  (foreign-string-free (mem-aref cparams :pointer i))))))
  
  nil)

(defmethod recv ()
  (let ((result (PQgetResult (cx-pg *cx*))))
    (if (null-pointer-p result)
	(values nil nil)
	(let* ((status (PQresultStatus result)))
	  (unless (or (eq status :PGRES_COMMAND_OK) (eq status :PGRES_TUPLES_OK))
	    (error "~a~%~a" status (PQresultErrorMessage result)))
	  (values result status)))))

(defmethod send-cmd (sql params)
  (send sql params)
  
  (multiple-value-bind (result status) (recv)
    (assert (eq status :PGRES_COMMAND_OK))
    (PQclear result)
    (assert (null (recv)))))

(defmethod send-dml (sql params)
  (send sql params)
  
  (multiple-value-bind (result status) (recv)
    (assert (null (recv)))
    (assert (eq status :PGRES_COMMAND_OK))
    (let ((ntuples (parse-integer (PQcmdTuples result))))
      (PQclear result)
      ntuples)))

(defun send-val (sql params)
  (send sql params)

  (multiple-value-bind (result status) (recv)
    (assert (eq status :PGRES_TUPLES_OK))    
    (assert (null (recv)))
    (assert (= (PQntuples result) 1))
    (assert (= (PQnfields result) 1))
    (let ((v (PQgetvalue result 0 0)))
      (PQclear result)
      v)))
