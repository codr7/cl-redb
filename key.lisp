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

(defmethod create ((key key) &key (cx *cx*))
  (with-slots (table) key
    (let ((sql (with-output-to-string (out)
		 (format out "ALTER TABLE ~a ADD CONSTRAINT ~a ~a ("
			 (sql-name table)
			 (sql-name key)
			 (if (eq key (primary-key table)) "PRIMARY KEY" "UNIQUE"))
		 
		 (let ((i 0))
		   (dolist (c (cols key))
		     (unless (zerop i)
		       (format out ", "))
		     (format out "~a" (sql-name c))
		     (incf i)))
		 
		 (format out ")"))))
      (send-command sql nil :cx cx)))
  nil)

(defmethod drop ((key key) &key (cx *cx*))
  (with-slots (table) key
    (let ((sql (format nil "ALTER TABLE ~a DROP CONSTRAINT IF EXISTS ~a"
		       (sql-name table) (sql-name key))))
      (send-command sql nil :cx cx)))
  nil)
