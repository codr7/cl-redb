(in-package redb)

(defclass foreign-key (key)
  ((foreign-table :initarg :foreign-table :initform (error "missing foreigntable") :reader foreign-table)
   (col-map :initform (make-hash-table) :reader col-map)))

(defun new-foreign-key (name foreign-table)
  (let* ((key (make-instance 'foreign-key :name name :foreign-table foreign-table)))
    (with-slots (col-map) key
      (do-cols (fc (primary-key foreign-table))
	(let* ((c (col-clone fc (sym name '- (name fc)))))
	  (add-col key c)
	  (setf (gethash c col-map) fc))))
    key))

(defmethod key-create ((key foreign-key) table)
  (let* ((sql (with-output-to-string (out)
		(format out "ALTER TABLE ~a ADD CONSTRAINT ~a FOREIGN KEY ("
			(to-sql table) (to-sql key))
		
		(let* ((i 0))
		  (dohash (c fc (col-map key))
		    (unless (zerop i)
		      (format out ", "))
		    (format out "~a" (to-sql c))
		    (incf i)))
		
		(format out ") REFERENCES ~a (" (to-sql (foreign-table key)))

		(let* ((i 0))
		  (dohash (c fc (col-map key))
		    (unless (zerop i)
		      (format out ", "))
		    (format out "~a" (to-sql fc))
		    (incf i)))
		
		(format out ")"))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  (assert (null (get-result)))
  nil)
