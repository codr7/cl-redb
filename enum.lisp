(in-package redb)

(defclass enum (def)
  ((alts :initarg :alts :reader alts)))

(defmethod print-object ((enum enum) out)
  (format out "(enum ~a)" (str! (name enum))))

(defun new-enum (name &rest alts)
  (make-instance 'enum :name name
		       :alts (make-array (length alts) :element-type 'keyword
						       :initial-contents alts)))

(defmethod create ((enum enum))
  (unless (exists? enum)
    (let ((sql (with-output-to-string (out)
		 (format out "CREATE TYPE ~a AS ENUM (" (sql-name enum))
		 (with-slots (alts) enum
		   (dotimes (i (length alts))
		     (unless (zerop i)
		       (format out ", "))
		     (format out "'~a'" (sql-name (aref alts i))))
		   (format out ")")))))
      (send-command sql nil))
    t))

(defmethod drop ((enum enum))
  (when (exists? enum)
    (let ((sql (format nil "DROP TYPE ~a" (sql-name enum))))
      (send-command sql nil)))
  t)

(defmethod exists? ((enum enum))
  (send "SELECT EXISTS (
                 SELECT FROM pg_type
                 WHERE typname  = $1
               )"
	(list (sql-name (name enum))))
  (multiple-value-bind (result status) (recv)
    (assert (eq status :PGRES_TUPLES_OK))    
    (assert (null (recv)))
    (assert (= (PQntuples result) 1))
    (assert (= (PQnfields result) 1))
    (let ((exists? (boolean-from-sql (PQgetvalue result 0 0))))
      (PQclear result)
      exists?)))
