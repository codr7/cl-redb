(in-package redb)

(defclass foreign-key (key)
  ((foreign-table :initarg :foreign-table :initform (error "missing foreigntable") :reader foreign-table)
   (col-map :initform (make-hash-table) :reader col-map)
   (null? :initarg :null? :initform nil :reader null?)))

(defmethod print-object ((key foreign-key) out)
  (format out "(foreign-key ~a)" (str! (name key))))

(defun new-foreign-key (tbl name foreign-tbl &key null?)
  (let ((key (make-instance 'foreign-key :table tbl :name name :foreign-table foreign-tbl :null? null?)))
    (with-slots (col-map) key
      (dolist (fc (cols (primary-key foreign-tbl)))
	(let ((c (col-clone fc tbl (sym name '- (name fc)))))
	  (add-col key c)
	  (setf (gethash c col-map) fc))))
    key))

(defmethod create ((key foreign-key) &key (cx *cx*))
  (with-slots (table) key
    (let ((sql (with-output-to-string (out)
		 (format out "ALTER TABLE ~a ADD CONSTRAINT ~a FOREIGN KEY ("
			 (sql-name table) (sql-name key))
		 
		 (let ((i 0))
		   (dohash (c fc (col-map key))
		     (unless (zerop i)
		       (format out ", "))
		     (format out "~a" (sql-name c))
		     (incf i)))
		 
		 (format out ") REFERENCES ~a (" (sql-name (foreign-table key)))

		 (let ((i 0))
		   (dohash (c fc (col-map key))
		     (unless (zerop i)
		       (format out ", "))
		     (format out "~a" (sql-name fc))
		     (incf i)))
		 
		 (format out ")"))))
      (send-command sql nil :cx cx)))
  nil)
