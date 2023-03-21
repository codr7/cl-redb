(in-package redb)

(defclass index (table-def)
  ((cols :initform nil :initarg :cols :reader cols)
   (unique? :initarg :unique? :reader unique?)))

(defmethod add-col ((idx index) (col col))
  (with-slots (cols) idx
    (push col cols)))

(defmethod print-object ((idx index) out)
  (format out "(index ~a)" (str! (name idx))))

(defun new-index (tbl name unique? &rest cols)
  (make-instance 'index :table tbl :name name :unique? unique? :cols cols))

(defmethod exists? ((idx index))
    (boolean-from-sql (send-val "SELECT EXISTS (
                                   SELECT FROM
                                     pg_class t,
                                     pg_class i,
                                     pg_index ix
                                   WHERE
                                     t.oid = ix.indrelid
                                     and i.oid = ix.indexrelid
                                     and t.relname = $1
                                     and i.relname = $2
                                 )"
				`(,(sql-name (table idx)) ,(sql-name idx)))))

(defmethod create ((idx index))
  (unless (exists? idx)
    (with-slots (table) idx
      (let ((sql (with-output-to-string (out)
		   (write-string "CREATE" out)
		   
		   (when (unique? idx)
		     (write-string " UNIQUE" out))
		   
		   (format out " INDEX ~a ON ~a (" (sql-name idx) (sql-name table))
		   
		   (let ((i 0))
		     (dolist (c (cols idx))
		       (unless (zerop i)
			 (format out ", "))
		       (format out "~a" (sql-name c))
		       (incf i)))
		   
		   (format out ")"))))
	(send-dml sql nil)
	t))))

(defmethod drop ((idx index))
  (when (exists? idx)
    (let ((sql (with-output-to-string (out)
		 (format out "DROP INDEX ~a" (sql-name idx)))))
      (send-dml sql nil)
      t)))
