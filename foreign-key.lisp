(in-package redb)

(defclass foreign-key (key)
  ((foreign-table :initarg :foreign-table :initform (error "missing foreigntable") :reader foreign-table)
   (col-map :initform (make-hash-table) :reader col-map)))

(defun new-foreign-key (tbl name foreign-tbl)
  (let* ((key (make-instance 'foreign-key :table tbl :name name :foreign-table foreign-tbl)))
    (with-slots (col-map) key
      (do-cols (fc (primary-key foreign-tbl))
	(let* ((c (col-clone fc (sym name '- (name fc)))))
	  (add-col key c)
	  (setf (gethash c col-map) fc))))
    key))

(defmethod create ((key foreign-key) &key (cx *cx*))
  (with-slots (table) key
    (let* ((sql (with-output-to-string (out)
		  (format out "ALTER TABLE ~a ADD CONSTRAINT ~a FOREIGN KEY ("
			  (sql-name table) (sql-name key))
		  
		  (let* ((i 0))
		    (dohash (c fc (col-map key))
		      (unless (zerop i)
			(format out ", "))
		      (format out "~a" (sql-name c))
		      (incf i)))
		  
		  (format out ") REFERENCES ~a (" (sql-name (foreign-table key)))

		  (let* ((i 0))
		    (dohash (c fc (col-map key))
		      (unless (zerop i)
			(format out ", "))
		      (format out "~a" (sql-name fc))
		      (incf i)))
		  
		  (format out ")"))))
      (send sql nil :cx cx)))
  (multiple-value-bind (r s) (recv :cx cx)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  nil)
