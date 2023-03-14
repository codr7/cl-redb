(in-package redb)

(defvar *cx*)

(defstruct cx
  (pg nil :type t)
  (stored-values (make-hash-table) :type hash-table))

(defun new-cx (pg)
  (make-cx :pg pg))

(defun connect (db user password &key (host "localhost"))
  (let ((pg (PQconnectdb (format nil "postgresql://~a:~a@~a/~a" user password host db))))
    (unless (eq (PQstatus pg) :CONNECTION_OK)
      (error (PQerrorMessage pg)))
    pg))

(defmacro with-cx ((&rest args) &body body)
  `(let ((*cx* (new-cx (connect ,@args))))
     (unwind-protect
	  (progn ,@body)
       (PQfinish (cx-pg *cx*)))))

(defmethod send (sql params &key (cx *cx*))
  (let ((nparams (length params)))
    (with-foreign-object (cparams :pointer nparams)
      (let ((i 0))
	(dolist (p params)
	  (setf (mem-aref cparams :pointer i) (foreign-string-alloc p))
	  (incf i)))
      
      (unless (= (PQsendQueryParams (cx-pg cx)
				    sql
				    nparams
				    (null-pointer) cparams (null-pointer) (null-pointer)
				    0)
		 1)
	(error (PQerrorMessage (cx-pg cx))))
      
      (dotimes (i (length params))
	(foreign-string-free (mem-aref cparams :pointer i)))))
  nil)

(defmethod recv (&key (cx *cx*))
  (let ((result (PQgetResult (cx-pg cx))))
    (if (null-pointer-p result)
	(values nil nil)
	(let* ((status (PQresultStatus result)))
	  (unless (or (eq status :PGRES_COMMAND_OK) (eq status :PGRES_TUPLES_OK))
	    (error "~a~%~a" status (PQresultErrorMessage result)))
	  (values result status)))))

(defmethod send-command (sql params &key (cx *cx*))
  (send sql params :cx cx)
  
  (multiple-value-bind (result status) (recv :cx cx)
    (assert (eq status :PGRES_COMMAND_OK))
    (PQclear result)
    (assert (null (recv :cx cx)))))

(defun test-cx ()
  (with-cx ("test" "test" "test")
    (when (not (eq (PQstatus (cx-pg *cx*)) :CONNECTION_OK))
      (error (PQerrorMessage (cx-pg *cx*))))

    (send "SELECT * FROM pg_tables" '())
    
    (multiple-value-bind (result status) (recv)
      (assert (eq status :PGRES_TUPLES_OK))
      (PQclear result)
      (assert (null (recv))))))

