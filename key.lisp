(in-package redb)

(defclass key (table-def rel)
  ())

(defmethod print-object ((key key) out)
  (format out "(key ~a)" (str! (name key))))

(defun new-key (tbl name &rest cols)
  (let* ((key (make-instance 'key :table tbl :name name)))
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
		 
		 (let* ((i 0))
		   (do-cols (c key)
		     (unless (zerop i)
		       (format out ", "))
		     (format out "~a" (sql-name c))
		     (incf i)))
		 
		 (format out ")"))))
      (send sql nil :cx cx)))
  (multiple-value-bind (result status) (recv :cx cx)
    (assert (eq status :PGRES_COMMAND_OK))
    (PQclear result)
    (assert (null (recv :cx cx))))
  nil)

(defmethod drop ((key key) &key (cx *cx*))
  (with-slots (table) key
    (let ((sql (format nil "ALTER TABLE ~a DROP CONSTRAINT IF EXISTS ~a"
		       (sql-name table) (sql-name key))))
      (send sql nil :cx cx)))
  (multiple-value-bind (result status) (recv :cx cx)
    (assert (eq status :PGRES_COMMAND_OK))
    (PQclear result)
    (assert (null (recv :cx cx))))
  nil)
