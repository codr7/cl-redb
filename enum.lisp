(in-package redb)

(defclass enum (def)
  ((alts :initarg :alts :reader alts)))

(defun new-enum (name &rest alts)
  (make-instance 'enum :name name :alts (make-array (length alts) :element-type 'keyword :initial-contents alts)))

(defmethod create ((self enum) &key (cx *cx*))
  (let* ((sql (with-output-to-string (out)
		(format out "CREATE TYPE ~a AS ENUM (" (sql-name self))
		(with-slots (alts) self
		  (dotimes (i (length alts))
		    (unless (zerop i)
		      (format out ", "))
		    (format out "'~a'" (sql-name (aref alts i))))
		  (format out ")")))))
    (send sql nil :cx cx))
  (multiple-value-bind (r s) (recv :cx cx)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r)))

(defmethod drop ((self enum) &key (cx *cx*))
  (let* ((sql (format nil "DROP TYPE IF EXISTS ~a" (sql-name self))))
    (send sql nil :cx cx))
  (multiple-value-bind (r s) (recv :cx cx)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  nil)

(defmethod exists? ((self enum) &key (cx *cx*))
  (send "SELECT EXISTS (
                 SELECT FROM pg_type
                 WHERE typname  = $1
               )"
	(list (sql-name (name self)))
	:cx cx)
  (let* ((r (recv :cx cx)))
    (assert (= (PQntuples r) 1))
    (assert (= (PQnfields r) 1))
    (let* ((result (boolean-from-sql (PQgetvalue r 0 0))))
      (PQclear r)
      result)))
