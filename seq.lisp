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
      (send-cmd sql nil))
    t))

(defmethod drop ((seq seq))
  (when (exists? seq)
    (let ((sql (format nil "DROP SEQUENCE ~a" (sql-name seq))))
      (send-cmd sql nil)))
  t)

(defmethod exists? ((seq seq))
  (boolean-from-sql (send-val "SELECT EXISTS (
                                 SELECT FROM pg_class
                                 WHERE relkind = 'S'
                                 AND relname = $1
                               )"
			      (list (sql-name (name seq))))))

(defun next-val (seq)
  (integer-from-sql (send-val (format nil "SELECT NEXTVAL('~a')" (sql-name seq)) nil)))
