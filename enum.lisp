(in-package redb)

(defclass enum (def)
  ((alts :initarg :alts :reader alts)))

(defun new-enum (name &rest alts)
  (make-instance 'enum :name name :alts (make-array (length alts) :element-type 'keyword :initial-contents alts)))

(defun enum-create (self)
  (let* ((sql (with-output-to-string (out)
		(format out "CREATE TYPE ~a AS ENUM (" (sql-name self))
		(with-slots (alts) self
		  (dotimes (i (length alts))
		    (unless (zerop i)
		      (format out ", "))
		    (format out "'~a'" (sql-name (aref alts i))))
		  (format out ")")))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))
    (PQclear r))
  (assert (null (get-result))))

(defmethod create ((self enum))
  (enum-create self))

(defun enum-drop (self)
  (let* ((sql (format nil "DROP TYPE IF EXISTS ~a" (sql-name self))))
    (send-query sql '()))
  (multiple-value-bind (r s) (get-result)
    (assert (eq s :PGRES_COMMAND_OK))    
    (PQclear r))
  (assert (null (get-result)))
  nil)

(defmethod drop ((self enum))
  (enum-drop self))

(defun enum-exists? (self)
  (send-query "SELECT EXISTS (
                 SELECT FROM pg_type
                 WHERE typname  = $1
               )"
	      (list (sql-name (name self))))
  (let* ((r (get-result)))
    (assert (= (PQntuples r) 1))
    (assert (= (PQnfields r) 1))
    (let* ((result (boolean-from-sql (PQgetvalue r 0 0))))
      (PQclear r)
      (assert (null (get-result)))
      result)))

(defmethod exists? ((self enum))
  (enum-exists? self))
