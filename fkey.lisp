(in-package redb)

(defclass fkey (key)
  ((foreign-table :initarg :foreign-table :initform (error "missing foreigntable") :reader foreign-table)
   (foreign-cols :initform nil :reader foreign-cols)
   (null? :initarg :null? :initform nil :reader null?)))

(defmethod print-object ((key fkey) out)
  (format out "(fkey ~a)" (str! (name key))))

(defun new-fkey (tbl name foreign-tbl &key null?)
  (let ((key (make-instance 'fkey :table tbl :name name :foreign-table foreign-tbl :null? null?)))
    (with-slots (foreign-cols) key
      (dolist (fc (cols (pkey foreign-tbl)))
	(let ((c (col-clone fc tbl (kw name '- (name fc)))))
	  (add-col key c)
	  (push (cons c fc) foreign-cols))))
    key))

(defmethod create ((key fkey))
  (with-slots (table) key
    (let ((sql (with-output-to-string (out)
		 (format out "ALTER TABLE ~a ADD CONSTRAINT ~a FOREIGN KEY ("
			 (sql-name table) (sql-name key))
		 
		 (let ((i 0))
		   (dolist (c (cols key))
		     (unless (zerop i)
		       (format out ", "))
		     (format out "~a" (sql-name c))
		     (incf i)))
		 
		 (format out ") REFERENCES ~a (" (sql-name (foreign-table key)))

		 (let ((i 0))
		   (dolist (c (foreign-cols key))
		     (unless (zerop i)
		       (format out ", "))
		     (format out "~a" (sql-name (rest c)))
		     (incf i)))
		 
		 (format out ") ON UPDATE CASCADE ON DELETE RESTRICT"))))
      (send-dml sql nil)))
  nil)
