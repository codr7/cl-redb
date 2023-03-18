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
      (send-dml sql nil))
    t))

(defmethod drop ((enum enum))
  (when (exists? enum)
    (let ((sql (format nil "DROP TYPE ~a" (sql-name enum))))
      (send-dml sql nil)))
  t)

(defmethod exists? ((enum enum))
  (boolean-from-sql (send-val "SELECT EXISTS (
                                 SELECT FROM pg_type
                                 WHERE typname  = $1
                               )"
			      (list (sql-name (name enum))))))
