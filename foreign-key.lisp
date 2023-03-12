(in-package redb)

(defclass foreign-key (key)
  ((table :initarg :table :initform (error "missing table") :reader table)
   (col-map :initform (make-hash-table) :reader col-map)))

(defun new-foreign-key (name table)
  (let* ((key (make-instance 'foreign-key :name name :table table)))
    (with-slots (col-indices col-map cols) key
      (do-cols (fc (primary-key table))
	(let* ((c (col-clone fc (syms! name '- (name fc)))))
	  (setf (gethash (name c) col-indices) (length cols))
	  (vector-push-extend c cols)
	  (setf (gethash c col-map) fc))))
    key))

(defmethod key-create ((self foreign-key) table)
  (let* ((sql (with-output-to-string (out)
		(format out "ALTER TABLE ~a ADD CONSTRAINT ~a FOREIGN KEY ("
			(to-sql table) (to-sql self))
		
		(let* ((i 0))
		  (dohash (c fc (col-map self))
		    (unless (zerop i)
		      (format out ", "))
		    (format out "~a" (to-sql c))
		    (incf i)))
		
		(format out ") REFERENCES ~a (" (to-sql (table self)))

		(let* ((i 0))
		  (dohash (c fc (col-map self))
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
