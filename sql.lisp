(in-package redb)

(defmethod sql-name ((self string))
  (let* ((out (copy-seq self)))
    (dotimes (i (length out))
      (let* ((c (char out i)))
	(when (char= c #\-)
	  (setf (char out i) #\_))))
    out))

(defmethod sql-name ((self symbol))
  (sql-name (string-downcase (symbol-name self))))
