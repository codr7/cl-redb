(in-package redb)

(defclass seq (def)
  ((start :initarg :start :reader start)))

(defmethod print-object ((seq seq) out)
  (format out "(seq ~a)" (str! (name seq))))

(defun new-seq (name &key (start 1))
  (make-instance 'seq :name name :start start))

(defmethod create ((seq seq))
  (unless (exists? seq)
    (let ((sql (with-output-to-string (out)
		 (format out "CREATE SEQUENCE ~a START ~a" (sql-name seq) (start seq)))))
      (send-command sql nil))
    t))

(defmethod drop ((seq seq))
  (when (exists? seq)
    (let ((sql (format nil "DROP SEQUENCE ~a" (sql-name seq))))
      (send-command sql nil)))
  t)

(defmethod exists? ((seq seq))
  (send "SELECT EXISTS (
          SELECT FROM pg_class
          WHERE relkind = 'S'
          AND relname = $1
        )"
	(list (sql-name (name seq))))
  (multiple-value-bind (result status) (recv)
    (assert (eq status :PGRES_TUPLES_OK))    
    (assert (null (recv)))
    (assert (= (PQntuples result) 1))
    (assert (= (PQnfields result) 1))
    (let ((exists? (boolean-from-sql (PQgetvalue result 0 0))))
      (PQclear result)
      exists?)))
