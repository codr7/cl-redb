(in-package redb)

(defclass key (table-def)
  ((cols :initform nil :reader cols)))

(defmethod add-col ((key key) (col col))
  (with-slots (cols) key
    (push col cols)))

(defmethod print-object ((key key) out)
  (format out "(key ~a)" (str! (name key))))

(defun new-key (tbl name &rest cols)
  (let ((key (make-instance 'key :table tbl :name name)))
    (dolist (c cols)
      (add-col key c))
    key))

(defmethod exists? ((key key))
    (boolean-from-sql (send-val "SELECT EXISTS (
                                   SELECT constraint_name 
                                   FROM information_schema.constraint_column_usage 
                                   WHERE table_name = $1  and constraint_name = $2
                                 )"
				`(,(sql-name (table key)) ,(sql-name key)))))
(defmethod create ((key key))
  (unless (exists? key)
    (with-slots (table) key
      (let ((sql (with-output-to-string (out)
		   (format out "ALTER TABLE ~a ADD CONSTRAINT ~a ~a ("
			   (sql-name table)
			   (sql-name key)
			   (if (eq key (pkey table)) "PRIMARY KEY" "UNIQUE"))
		   
		   (let ((i 0))
		     (dolist (c (cols key))
		       (unless (zerop i)
			 (format out ", "))
		       (format out "~a" (sql-name c))
		       (incf i)))
		   
		   (format out ")"))))
	(send-dml sql nil)
	t))))

(defmethod drop ((key key))
  (when (exists? key)
    (with-slots (table) key
      (let ((sql (with-output-to-string (out)
		   (format out "ALTER TABLE ~a DROP CONSTRAINT ~a" (sql-name table) (sql-name key)))))
	(send-dml sql nil)
	t))))
